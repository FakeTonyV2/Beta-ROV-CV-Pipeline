"""Opt-in tests that are evidence only when a real UVC camera is attached."""

from __future__ import annotations

import os
import platform
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path
from uuid import uuid4

import psutil
import pytest

from purdue_rov_cv.camera.backend import CaptureBackendError, SurfaceRtpStream, V4L2CaptureBackend
from purdue_rov_cv.camera.service import CameraService
from purdue_rov_cv.camera.v4l2 import V4L2DeviceProbe
from purdue_rov_cv.config import load_config
from purdue_rov_cv.config.models import CameraAdapter, CameraFormat
from purdue_rov_cv.frame_buffer import shared_memory_name
from purdue_rov_cv.module_runner.frame_source import SharedMemoryFrameSource
from purdue_rov_cv.video.gstreamer import GStreamerRtpReceiver

pytestmark = pytest.mark.hardware


def _free_udp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as candidate:
        candidate.bind(("127.0.0.1", 0))
        return int(candidate.getsockname()[1])


def _hardware_camera():
    configured = os.environ.get("PURDUE_ROV_CV_HARDWARE_CONFIG")
    camera_id = os.environ.get("PURDUE_ROV_CV_HARDWARE_CAMERA")
    if not configured or not camera_id:
        pytest.skip(
            "UNVERIFIED — HARDWARE UNAVAILABLE: set PURDUE_ROV_CV_HARDWARE_CONFIG and PURDUE_ROV_CV_HARDWARE_CAMERA"
        )
    config = load_config(Path(configured), environ={})
    if camera_id not in config.cameras:
        pytest.fail(f"hardware camera {camera_id!r} is absent from {configured}")
    camera = config.cameras[camera_id]
    if camera.adapter is not CameraAdapter.V4L2:
        pytest.fail("Phase 10 hardware evidence requires adapter=gstreamer_v4l2")
    return camera_id, camera


def test_real_stable_identity_enumeration_and_mode_open() -> None:
    camera_id, camera = _hardware_camera()
    if shutil.which("v4l2-ctl") is None or shutil.which("gst-launch-1.0") is None:
        pytest.fail("v4l2-ctl and gst-launch-1.0 are required")
    report = V4L2DeviceProbe().validate(camera_id, camera, open_mode=True)
    assert report.mode_opened, report.open_detail
    versions = {
        "os": platform.platform(),
        "kernel": platform.release(),
        "python": sys.version.split()[0],
        "gstreamer": subprocess.run(
            ["gst-launch-1.0", "--version"], capture_output=True, text=True, check=False
        ).stdout.splitlines()[0],
        "v4l2_ctl": subprocess.run(
            ["v4l2-ctl", "--version"], capture_output=True, text=True, check=False
        ).stdout.splitlines()[0],
        "configured_path": str(report.device.configured_path),
        "resolved_path": str(report.device.resolved_path),
        "resolution_tier": report.device.resolution_tier.value,
        "identity": report.device.stable_identity,
        "model": report.device.properties.get("ID_MODEL", "unknown"),
        "vid_pid": (
            f"{report.device.properties.get('ID_VENDOR_ID', 'unknown')}:"
            f"{report.device.properties.get('ID_MODEL_ID', 'unknown')}"
        ),
        "advertised_modes": [mode.describe() for mode in report.modes],
        "tested_mode": report.configured_mode.describe(),
    }
    print(versions)


def _next_module_frame(service: CameraService, source: SharedMemoryFrameSource, after: int = -1):
    deadline = time.monotonic() + 8.0
    while time.monotonic() < deadline:
        service.step()
        if not source.attached:
            source.attach()
        frame = source.read(0.05) if source.attached else None
        if frame is not None and frame.frame_number > after:
            return frame
    pytest.fail(f"real camera did not deliver a module-visible frame after {after}")


def test_real_camera_service_shared_memory_rebuild_identity_and_running_shutdown() -> None:
    camera_id, camera = _hardware_camera()
    process = psutil.Process()
    initial_rss = process.memory_info().rss
    initial_fds = process.num_fds()
    session = uuid4()
    service = CameraService(
        camera_id,
        camera,
        lambda: V4L2CaptureBackend(camera_id, camera),
        session_uuid=session,
    )
    source = SharedMemoryFrameSource(
        shared_memory_name(camera_id),
        camera_id=camera_id,
        expected_slot_capacity_bytes=camera.slot_capacity_bytes,
    )
    shutdown_elapsed = 0.0
    try:
        service.initialize()
        frame = _next_module_frame(service, source)
        assert frame.camera_session_id == session.bytes
        for _cycle in range(3):
            service._lose_backend(CaptureBackendError("HIL forced pipeline rebuild"), timed_out=False)
            frame = _next_module_frame(service, source, frame.frame_number)
            assert frame.camera_session_id == session.bytes
        assert service.metrics.snapshot().values["pipeline_restarts"] == 3
        started = time.monotonic()
        result = service.close()
        shutdown_elapsed = time.monotonic() - started
        assert result.completed and not result.failures
    finally:
        source.close()
        if service.state_machine.state.value != "STOPPED":
            service.close()
    assert shutdown_elapsed <= 5.0
    assert process.num_fds() <= initial_fds + 4
    assert process.memory_info().rss <= initial_rss + 64 * 1024 * 1024


def test_real_rtp_receiver_and_exact_frame_index_key() -> None:
    camera_id, camera = _hardware_camera()
    if camera.format in {CameraFormat.YUYV, CameraFormat.NV12} and not camera.allow_software_encode:
        pytest.skip("UNVERIFIED — CONFIGURED HARDWARE MODE IS CV-ONLY: RTP requires explicit software encode opt-in")
    port = _free_udp_port()
    payload_type = 110
    session = uuid4().bytes
    indices = []
    decoded = []
    invalid: list[str] = []
    receiver_format = CameraFormat.MJPEG if camera.format is CameraFormat.MJPEG else CameraFormat.H264
    receiver = GStreamerRtpReceiver(
        port,
        payload_type,
        on_packet=lambda _ssrc, _timestamp, _observed: None,
        on_packet_lost=lambda _count: None,
        on_decoded=decoded.append,
        on_invalid_decoded=invalid.append,
        on_encoded=lambda _unit: None,
        source_format=receiver_format,
    )
    backend = V4L2CaptureBackend(
        camera_id,
        camera,
        surface_stream=SurfaceRtpStream(
            camera_id,
            session,
            "127.0.0.1",
            port,
            payload_type,
            int.from_bytes(session[:4], "big"),
            indices.append,
        ),
    )
    try:
        receiver.start()
        time.sleep(0.2)
        backend.start()
        deadline = time.monotonic() + 8.0
        exact = None
        while time.monotonic() < deadline and exact is None:
            backend.poll(0.1)
            receiver.check_bus()
            for frame in decoded:
                exact = next(
                    (
                        index
                        for index in indices
                        if (index.rtp_ssrc, index.rtp_timestamp) == (frame.rtp_ssrc, frame.rtp_timestamp)
                    ),
                    None,
                )
                if exact is not None:
                    break
        assert exact is not None
        assert exact.camera_session_id == session
        assert not invalid
    finally:
        backend.stop()
        receiver.stop()


def test_real_degraded_shutdown_is_bounded() -> None:
    camera_id, camera = _hardware_camera()
    service = CameraService(camera_id, camera, lambda: V4L2CaptureBackend(camera_id, camera))
    try:
        service.initialize()
        service._lose_backend(CaptureBackendError("HIL forced degraded state"), timed_out=False)
        started = time.monotonic()
        result = service.close()
        elapsed = time.monotonic() - started
        assert result.completed and not result.failures
        assert elapsed <= 5.0
    finally:
        if service.state_machine.state.value != "STOPPED":
            service.close()


def test_manual_physical_disconnect_reconnect_identity_continuity() -> None:
    camera_id, camera = _hardware_camera()
    if os.environ.get("PURDUE_ROV_CV_HARDWARE_DISCONNECT") != "1":
        pytest.skip("UNVERIFIED — MANUAL DISCONNECT NOT REQUESTED: set PURDUE_ROV_CV_HARDWARE_DISCONNECT=1")
    assert camera.device_path is not None
    session = uuid4()
    service = CameraService(
        camera_id,
        camera,
        lambda: V4L2CaptureBackend(camera_id, camera),
        session_uuid=session,
    )
    source = SharedMemoryFrameSource(
        shared_memory_name(camera_id),
        camera_id=camera_id,
        expected_slot_capacity_bytes=camera.slot_capacity_bytes,
    )
    try:
        service.initialize()
        before = _next_module_frame(service, source)
        print(f"Unplug only {camera.device_path} now", flush=True)
        removal_deadline = time.monotonic() + 30.0
        while camera.device_path.exists() and time.monotonic() < removal_deadline:
            service.step()
        assert not camera.device_path.exists(), "configured stable link never disappeared"
        degraded_deadline = time.monotonic() + 5.0
        while service.state_machine.state.value != "DEGRADED" and time.monotonic() < degraded_deadline:
            service.step()
        assert service.state_machine.state.value == "DEGRADED"
        print(f"Reconnect the same camera at {camera.device_path}", flush=True)
        reconnect_deadline = time.monotonic() + 60.0
        while not camera.device_path.exists() and time.monotonic() < reconnect_deadline:
            service.step()
        assert camera.device_path.exists(), "configured stable identity did not return"
        after = _next_module_frame(service, source, before.frame_number)
        assert after.camera_session_id == session.bytes
        assert after.frame_number > before.frame_number
    finally:
        source.close()
        service.close()
