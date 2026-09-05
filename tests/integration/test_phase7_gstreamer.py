"""Real GStreamer RTP, broker, correlation, rebuild, and shutdown tests."""

from __future__ import annotations

import multiprocessing
import queue
import socket
import time
from pathlib import Path
from threading import Thread
from uuid import UUID, uuid4

import pytest
import zmq

from purdue_rov_cv.camera import CameraService, GStreamerCaptureBackend, SurfaceRtpStream
from purdue_rov_cv.config.models import CameraAdapter, CameraConfig, CameraFormat, CameraPathKind
from purdue_rov_cv.messaging.broker import DataBrokerService
from purdue_rov_cv.runtime.metrics import RuntimeMetrics
from purdue_rov_cv.runtime.shutdown import ShutdownToken, install_signal_handlers
from purdue_rov_cv.runtime.state import ComponentState
from purdue_rov_cv.video import (
    CorrelationQuality,
    FrameCorrelator,
    FrameIndexCache,
    FrameIndexPublisher,
    FrameIndexSubscriber,
    GStreamerRtpSender,
    VideoReceiverService,
)
from purdue_rov_cv.video.mapping import RtpFrameIndexMapper


def _require_gstreamer() -> None:
    try:
        import gi

        gi.require_version("Gst", "1.0")
        gi.require_version("GstRtp", "1.0")
        from gi.repository import Gst, GstRtp  # noqa: F401

        Gst.init(None)
        for element in ("x264enc", "rtph264pay", "rtpjitterbuffer", "rtph264depay", "avdec_h264"):
            if Gst.ElementFactory.find(element) is None:
                pytest.fail(f"required real GStreamer element is unavailable: {element}")
    except (ImportError, ValueError) as error:
        pytest.fail(
            "supported Python environment cannot import PyGObject Gst/GstRtp; "
            f"run scripts/setup_system_deps.sh and recreate .venv: {type(error).__name__}: {error}"
        )


def _tcp_endpoint() -> str:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return f"tcp://127.0.0.1:{sock.getsockname()[1]}"


def _camera(stream_index: int) -> CameraConfig:
    return CameraConfig(
        adapter=CameraAdapter.V4L2,
        device_path=Path("/dev/simulated"),
        device_path_kind=CameraPathKind.FALLBACK,
        format=CameraFormat.H264,
        width=160,
        height=120,
        frame_rate=20,
        stream_index=stream_index,
        stream_to_surface=True,
        cv_enabled=True,
        allow_software_encode=True,
        slot_capacity_bytes=160 * 120 * 3,
    )


def _receiver_process(camera_id: str, stream_index: int, status) -> None:
    service = VideoReceiverService(
        camera_id,
        _camera(stream_index),
        "tcp://127.0.0.1:65481",
        "tcp://127.0.0.1:65482",
        health_interval_ms=1_000,
    )
    install_signal_handlers(service.shutdown)
    previous = None
    try:
        service.initialize()
        while not service.shutdown.token.is_requested:
            service.step()
            current = service.state_machine.state.value
            if current != previous:
                status.put((camera_id, current))
                previous = current
    finally:
        result = service.close()
        status.put((camera_id, "CLOSED", result.completed, len(result.failures)))


@pytest.mark.timeout(15)
def test_real_broker_subscriber_drops_malformed_multipart_and_stays_alive() -> None:
    publisher_endpoint, subscriber_endpoint = _tcp_endpoint(), _tcp_endpoint()
    broker = DataBrokerService(publisher_endpoint, subscriber_endpoint)
    broker_thread = Thread(target=broker.run, name="phase7-malformed-broker")
    broker_thread.start()
    deadline = time.monotonic() + 5
    while broker.state_machine.state is not ComponentState.RUNNING and time.monotonic() < deadline:
        time.sleep(0.01)
    assert broker.state_machine.state is ComponentState.RUNNING

    metrics = RuntimeMetrics()
    shutdown = ShutdownToken()
    cache = FrameIndexCache("front_camera")
    correlator = FrameCorrelator(cache, lambda _frame: None, metrics=metrics)
    subscriber = FrameIndexSubscriber(
        subscriber_endpoint,
        "front_camera",
        96,
        correlator,
        metrics=metrics,
        shutdown=shutdown,
    )
    subscriber_thread = Thread(target=subscriber.run, name="phase7-malformed-subscriber")
    subscriber_thread.start()
    assert subscriber.ready.wait(2)

    context = zmq.Context()
    publisher: zmq.Socket[bytes] = context.socket(zmq.PUB)
    publisher.setsockopt(zmq.SNDHWM, 5)
    publisher.setsockopt(zmq.LINGER, 0)
    publisher.connect(publisher_endpoint)
    try:
        # Allow the exact per-camera subscription to traverse XPUB/XSUB, then
        # send an invalid one-part message that still matches that subscription.
        time.sleep(0.3)
        deadline = time.monotonic() + 3
        while metrics.snapshot().values["invalid_multipart_message"] == 0 and time.monotonic() < deadline:
            publisher.send_multipart([b"cv.frame_index.front_camera"])
            time.sleep(0.05)
        assert metrics.snapshot().values["invalid_multipart_message"] >= 1
        assert subscriber_thread.is_alive()
        assert len(cache) == 0
    finally:
        publisher.close(linger=0)
        context.term()
        shutdown.request("test complete")
        subscriber_thread.join(3)
        broker.request_shutdown("test complete")
        broker_thread.join(3)
    assert not subscriber_thread.is_alive()
    assert not broker_thread.is_alive()


@pytest.mark.timeout(30)
def test_production_camera_source_broker_receiver_exact_identity() -> None:
    _require_gstreamer()
    publisher_endpoint, subscriber_endpoint = _tcp_endpoint(), _tcp_endpoint()
    broker = DataBrokerService(publisher_endpoint, subscriber_endpoint)
    broker_thread = Thread(target=broker.run, name="phase7-production-broker")
    broker_thread.start()
    deadline = time.monotonic() + 5
    while broker.state_machine.state is not ComponentState.RUNNING and time.monotonic() < deadline:
        time.sleep(0.01)
    assert broker.state_machine.state is ComponentState.RUNNING

    camera_id = f"camera_{uuid4().hex}"
    camera_config = _camera(24)
    receiver = VideoReceiverService(
        camera_id,
        camera_config,
        subscriber_endpoint,
        publisher_endpoint,
        health_interval_ms=500,
    )
    display = receiver.decoded_fanout.subscribe("operator")
    receiver.initialize()
    assert receiver.subscriber.ready.wait(2)

    session = uuid4().bytes
    camera_metrics = RuntimeMetrics()
    publisher = FrameIndexPublisher(
        publisher_endpoint,
        camera_id,
        metrics=camera_metrics,
        shutdown=ShutdownToken(),
    )
    mapper = RtpFrameIndexMapper(camera_id, session)
    published = []

    def publish(value):
        published.append(value)
        publisher.publish(value)

    def backend_factory() -> GStreamerCaptureBackend:
        return GStreamerCaptureBackend(
            camera_config.width,
            camera_config.height,
            camera_config.frame_rate,
            surface_stream=SurfaceRtpStream(
                camera_id,
                session,
                "127.0.0.1",
                receiver.allocation.rtp_port,
                receiver.allocation.rtp_payload_type,
                int.from_bytes(session[:4], byteorder="big"),
                publish,
                mtu=300,
                mapper=mapper,
            ),
        )

    camera = CameraService(
        camera_id,
        camera_config,
        backend_factory,
        session_uuid=UUID(bytes=session),
        metrics=camera_metrics,
        frame_index_publisher=publisher,
    )
    try:
        camera.initialize()
        time.sleep(0.3)
        exact = None
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and exact is None:
            camera.step()
            receiver.step()
            candidate = display.receive(0)
            if candidate is not None and candidate.correlation.quality is CorrelationQuality.EXACT:
                exact = candidate
        assert exact is not None
        assert exact.correlation.identity is not None
        matching = next(
            value
            for value in published
            if (value.rtp_ssrc, value.rtp_timestamp) == (exact.frame.rtp_ssrc, exact.frame.rtp_timestamp)
        )
        assert exact.correlation.identity.camera_session_id == session
        assert exact.correlation.identity.frame_number == matching.frame_number
        deadline = time.monotonic() + 2
        while camera.next_frame_number <= matching.frame_number and time.monotonic() < deadline:
            camera.step()
        assert camera.next_frame_number > matching.frame_number
        assert camera.metrics.snapshot().values["shared_memory_write_count"] > 0
    finally:
        camera_result = camera.close()
        receiver_result = receiver.close()
        broker.request_shutdown("test complete")
        broker_thread.join(3)
    assert camera_result.completed and not camera_result.failures
    assert receiver_result.completed and not receiver_result.failures
    assert not broker_thread.is_alive()


@pytest.mark.timeout(30)
def test_real_rtp_broker_frame_index_exact_timeout_rebuild_recovery_and_shutdown() -> None:
    _require_gstreamer()
    publisher_endpoint, subscriber_endpoint = _tcp_endpoint(), _tcp_endpoint()
    broker = DataBrokerService(publisher_endpoint, subscriber_endpoint)
    broker_thread = Thread(target=broker.run, name="phase7-broker")
    broker_thread.start()
    deadline = time.monotonic() + 5
    while broker.state_machine.state is not ComponentState.RUNNING and time.monotonic() < deadline:
        time.sleep(0.01)
    assert broker.state_machine.state is ComponentState.RUNNING

    receiver = VideoReceiverService(
        "front_camera",
        _camera(20),
        subscriber_endpoint,
        publisher_endpoint,
        health_interval_ms=500,
    )
    display = receiver.decoded_fanout.subscribe("operator")
    receiver.initialize()
    assert receiver.subscriber.ready.wait(2)

    publisher_shutdown = ShutdownToken()
    publisher = FrameIndexPublisher(
        publisher_endpoint,
        "front_camera",
        metrics=RuntimeMetrics(),
        shutdown=publisher_shutdown,
    )
    publisher_thread = Thread(target=publisher.run, name="phase7-frame-index")
    publisher_thread.start()
    assert publisher.ready.wait(2)
    time.sleep(0.3)

    indices = []

    def remember(value):
        indices.append(value)

    sender = GStreamerRtpSender(
        "front_camera",
        uuid4().bytes,
        "127.0.0.1",
        receiver.allocation.rtp_port,
        receiver.allocation.rtp_payload_type,
        width=160,
        height=120,
        frame_rate=20,
        mtu=300,
        on_frame_index=remember,
    )
    recovered_sender = None
    try:
        sender.start()
        # Prove the bounded wait with real transport: withhold every index until
        # at least one RTP frame has already decoded.
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and (
            receiver.metrics.snapshot().values["decoded_frames"] == 0 or not indices
        ):
            receiver.step()
        assert receiver.metrics.snapshot().values["decoded_frames"] > 0
        assert indices
        publish_cursor = max(0, len(indices) - 5)
        exact = None
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and exact is None:
            while publish_cursor < len(indices):
                publisher.publish(indices[publish_cursor])
                publish_cursor += 1
            receiver.step()
            candidate = display.receive(0)
            if candidate is not None and candidate.correlation.quality is CorrelationQuality.EXACT:
                exact = candidate
        assert exact is not None, {
            "indices": len(indices),
            "cache": len(receiver.cache),
            "metrics": receiver.metrics.snapshot().values,
        }
        assert exact.correlation.identity is not None
        assert exact.correlation.identity.camera_id == "front_camera"
        index_by_key = {(value.rtp_ssrc, value.rtp_timestamp): value.frame_number for value in indices}
        assert (
            exact.correlation.identity.frame_number == index_by_key[(exact.frame.rtp_ssrc, exact.frame.rtp_timestamp)]
        )
        assert (exact.frame.rtp_ssrc, exact.frame.rtp_timestamp) in {
            (value.rtp_ssrc, value.rtp_timestamp) for value in indices
        }
        assert len({(value.rtp_ssrc, value.rtp_timestamp) for value in indices}) == len(indices)
        values = receiver.metrics.snapshot().values
        assert values["rtp_packets_received"] > values["decoded_frames"] > 0
        assert values["frame_index_hits"] >= 1
        assert receiver.state_machine.state is ComponentState.RUNNING

        sender.stop()
        deadline = time.monotonic() + 5
        while receiver.state_machine.state is not ComponentState.DEGRADED and time.monotonic() < deadline:
            receiver.step()
        assert receiver.state_machine.state is ComponentState.DEGRADED
        assert receiver.stream_status == "STREAM LOST"
        assert receiver.backend is None

        deadline = time.monotonic() + 3
        while receiver.backend is None and time.monotonic() < deadline:
            receiver.step()
        assert receiver.backend is not None
        assert receiver.metrics.snapshot().values["stream_restarts"] >= 1

        recovered_sender = GStreamerRtpSender(
            "front_camera",
            uuid4().bytes,
            "127.0.0.1",
            receiver.allocation.rtp_port,
            receiver.allocation.rtp_payload_type,
            width=160,
            height=120,
            frame_rate=20,
            on_frame_index=publisher.publish,
        )
        recovered_sender.start()
        deadline = time.monotonic() + 8
        while receiver.state_machine.state is not ComponentState.RUNNING and time.monotonic() < deadline:
            receiver.step()
        assert receiver.state_machine.state is ComponentState.RUNNING
    finally:
        sender.stop()
        if recovered_sender is not None:
            recovered_sender.stop()
        started = time.monotonic()
        result = receiver.close()
        assert time.monotonic() - started < 5
        assert result.completed and not result.failures
        publisher_shutdown.request("test complete")
        publisher_thread.join(3)
        broker.request_shutdown("test complete")
        broker_thread.join(3)
        if publisher_thread.is_alive():
            pytest.fail("FrameIndex publisher leaked")
        if broker_thread.is_alive():
            pytest.fail("broker leaked")


@pytest.mark.timeout(15)
def test_real_rtp_without_frame_index_is_delivered_unmatched() -> None:
    _require_gstreamer()
    receiver = VideoReceiverService(
        "front_camera",
        _camera(21),
        _tcp_endpoint(),
        _tcp_endpoint(),
        health_interval_ms=1_000,
    )
    display = receiver.decoded_fanout.subscribe("debug")
    receiver.initialize()
    sender = GStreamerRtpSender(
        "front_camera",
        uuid4().bytes,
        "127.0.0.1",
        receiver.allocation.rtp_port,
        receiver.allocation.rtp_payload_type,
        width=160,
        height=120,
        frame_rate=20,
        on_frame_index=lambda _value: None,
    )
    try:
        sender.start()
        unmatched = None
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline and unmatched is None:
            receiver.step()
            candidate = display.receive(0)
            if candidate is not None and candidate.correlation.quality is CorrelationQuality.UNMATCHED:
                unmatched = candidate
        assert unmatched is not None
        assert unmatched.correlation.identity is None
        assert receiver.metrics.snapshot().values["frame_index_misses"] >= 1
    finally:
        sender.stop()
        receiver.close()


@pytest.mark.timeout(30)
def test_two_real_receiver_processes_are_isolated_and_sigterm_bounded() -> None:
    _require_gstreamer()
    context = multiprocessing.get_context("spawn")
    status = context.Queue()
    receiver_a = context.Process(target=_receiver_process, args=("camera_a", 22, status), name="phase7-receiver-a")
    receiver_b = context.Process(target=_receiver_process, args=("camera_b", 23, status), name="phase7-receiver-b")
    receiver_a.start()
    receiver_b.start()
    sender = GStreamerRtpSender(
        "camera_a",
        uuid4().bytes,
        "127.0.0.1",
        5000 + 2 * 22,
        96 + 22,
        width=160,
        height=120,
        frame_rate=20,
        on_frame_index=lambda _value: None,
    )
    states: dict[str, set[str]] = {"camera_a": set(), "camera_b": set()}
    try:
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and not all("READY" in values for values in states.values()):
            try:
                item = status.get(timeout=1)
            except queue.Empty:
                continue
            states[item[0]].add(item[1])
        sender.start()
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline and (
            "RUNNING" not in states["camera_a"] or "DEGRADED" not in states["camera_b"]
        ):
            try:
                item = status.get(timeout=1)
            except queue.Empty:
                continue
            states[item[0]].add(item[1])
        assert "RUNNING" in states["camera_a"]
        assert "DEGRADED" in states["camera_b"]
        assert receiver_a.is_alive() and receiver_b.is_alive()

        started = time.monotonic()
        receiver_a.terminate()
        receiver_a.join(5)
        assert time.monotonic() - started < 5
        assert receiver_a.exitcode == 0
        assert receiver_b.is_alive()
    finally:
        sender.stop()
        if receiver_a.is_alive():
            receiver_a.terminate()
            receiver_a.join(5)
        if receiver_b.is_alive():
            receiver_b.terminate()
            receiver_b.join(5)
        if receiver_a.is_alive():
            receiver_a.kill()
            receiver_a.join(2)
        if receiver_b.is_alive():
            receiver_b.kill()
            receiver_b.join(2)
    assert receiver_b.exitcode == 0
