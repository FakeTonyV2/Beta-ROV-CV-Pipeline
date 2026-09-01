"""Real-process Phase 6 transfer, recovery, restart, shutdown, and isolation."""

from __future__ import annotations

import multiprocessing
import os
import time
from multiprocessing import resource_tracker, shared_memory
from pathlib import Path
from uuid import UUID, uuid4

import numpy as np
import pytest

from purdue_rov_cv.camera import CameraService, CaptureBackendError, CapturedFrame, GStreamerCaptureBackend
from purdue_rov_cv.camera.entrypoints import camera_entrypoint
from purdue_rov_cv.config.models import CameraAdapter, CameraConfig, CameraFormat, CameraPathKind
from purdue_rov_cv.frame_buffer import (
    HEADER_SIZE,
    FrameHeader,
    FrameWrite,
    LiveOwnerError,
    PixelFormat,
    ReadStatus,
    SharedMemoryFrameReader,
    SharedMemoryFrameWriter,
    SharedMemoryInvalid,
    segment_size,
    shared_memory_name,
)
from purdue_rov_cv.frame_buffer.buffer import _startup_lock
from purdue_rov_cv.runtime.exit_codes import ExitCode


def _config() -> CameraConfig:
    return CameraConfig(
        adapter=CameraAdapter.V4L2,
        device_path=Path("/dev/simulated"),
        device_path_kind=CameraPathKind.FALLBACK,
        format=CameraFormat.MJPEG,
        width=8,
        height=6,
        frame_rate=30,
        stream_index=0,
        stream_to_surface=False,
        cv_enabled=True,
        allow_software_encode=True,
        slot_capacity_bytes=256,
    )


def _pattern(number: int) -> FrameWrite:
    value = number % 251
    return FrameWrite(
        bytes([value]) * 144,
        8,
        6,
        24,
        PixelFormat.BGR8,
        number,
        time.time_ns(),
        time.monotonic_ns(),
    )


def _writer_process(camera_id: str, count: int, ready, start, finished, stop) -> None:
    writer = SharedMemoryFrameWriter(camera_id, 256, UUID(int=100).bytes)
    writer.open()
    ready.set()
    start.wait(5.0)
    for number in range(count):
        writer.write(_pattern(number))
        time.sleep(0.0005)
    finished.set()
    stop.wait(5.0)
    writer.close()


def _crash_owner(camera_id: str, ready) -> None:
    writer = SharedMemoryFrameWriter(camera_id, 256, UUID(int=200).bytes)
    writer.open()
    writer.write(_pattern(0))
    assert writer._memory is not None  # type: ignore[attr-defined]
    resource_tracker.unregister(writer._memory._name, "shared_memory")  # type: ignore[attr-defined]
    ready.set()
    os._exit(9)


def _reader_process(camera_id: str, ready, start, finished, results) -> None:
    reader = SharedMemoryFrameReader(camera_id)
    try:
        _attach_until(reader)
        ready.set()
        start.wait(5.0)
        accepted = 0
        deadline = time.monotonic() + 6.0
        while time.monotonic() < deadline:
            result = reader.read()
            if result.status is ReadStatus.FRAME:
                assert result.frame is not None
                expected = result.frame.frame_number % 251
                results.put((result.frame.frame_number, bool(np.all(result.frame.pixels == expected))))
                accepted += 1
            if finished.is_set() and result.status is ReadStatus.NO_FRAME:
                break
        results.put(("accepted", accepted))
    finally:
        reader.close()


def _reject_invalid_reader_process(camera_id: str) -> None:
    reader = SharedMemoryFrameReader(camera_id)
    try:
        reader.attach()
    except SharedMemoryInvalid:
        return
    finally:
        reader.close()
    raise AssertionError("malformed segment was unexpectedly accepted")


def _attach_during_creation_process(camera_id: str, started, finished, results) -> None:
    reader = SharedMemoryFrameReader(camera_id, unregister_from_resource_tracker=False)
    started.set()
    try:
        results.put((reader.attach(), None))
    except Exception as error:
        results.put((False, type(error).__name__))
    finally:
        reader.close()
        finished.set()


def _replacement_racer(camera_id: str, start, stop, results) -> None:
    writer = SharedMemoryFrameWriter(camera_id, 256, uuid4().bytes)
    start.wait(5.0)
    try:
        writer.open()
    except LiveOwnerError:
        results.put("live-owner")
        return
    try:
        results.put("created")
        stop.wait(5.0)
    finally:
        writer.close()


def _hold_writer(camera_id: str, ready, stop) -> None:
    writer = SharedMemoryFrameWriter(camera_id, 6_220_800, uuid4().bytes)
    try:
        writer.open()
        ready.set()
        stop.wait(10.0)
    finally:
        writer.close()


def _duplicate_camera_entrypoint(config_path: str, result) -> None:
    exit_code = camera_entrypoint(["--camera", "front_camera", "--config", config_path])
    result.put(exit_code)
    raise SystemExit(exit_code)


class _UnavailableBackend:
    def start(self) -> None:
        raise CaptureBackendError("injected unavailable backend")

    def poll(self, timeout_seconds: float) -> None:
        del timeout_seconds
        return None

    def stop(self) -> None:
        return None


def _degraded_camera_process(camera_id: str) -> None:
    service = CameraService(camera_id, _config(), _UnavailableBackend, install_signals=True)
    service.run()


class _FailAfterOneFrame:
    def __init__(self, backend: GStreamerCaptureBackend) -> None:
        self.backend = backend
        self.delivered = False

    def start(self) -> None:
        self.backend.start()

    def poll(self, timeout_seconds: float) -> CapturedFrame | None:
        if self.delivered:
            raise CaptureBackendError("injected GStreamer rebuild")
        frame = self.backend.poll(timeout_seconds)
        if frame is not None:
            self.delivered = True
        return frame

    def stop(self) -> None:
        self.backend.stop()


def _gstreamer_camera_process(camera_id: str, session: bytes) -> None:
    first = True

    def factory():
        nonlocal first
        backend = GStreamerCaptureBackend(8, 6, 30)
        if first:
            first = False
            return _FailAfterOneFrame(backend)
        return backend

    service = CameraService(
        camera_id,
        _config(),
        factory,
        session_uuid=UUID(bytes=session),
        install_signals=True,
    )
    service.run()


def _require_gstreamer() -> None:
    try:
        import gi

        gi.require_version("Gst", "1.0")
        from gi.repository import Gst

        Gst.init(None)
    except (ImportError, AttributeError, ValueError) as error:
        pytest.fail(
            "supported Python 3.12 environment cannot import PyGObject/GStreamer; "
            f"run scripts/setup_system_deps.sh and recreate .venv: {type(error).__name__}: {error}"
        )


class _LoopBackend:
    def __init__(self) -> None:
        self.number = 0

    def start(self) -> None:
        return None

    def poll(self, timeout_seconds: float) -> CapturedFrame:
        time.sleep(min(timeout_seconds, 0.01))
        value = self.number % 251
        self.number += 1
        return CapturedFrame(
            bytes([value]) * 144,
            8,
            6,
            24,
            PixelFormat.BGR8,
            time.time_ns(),
            time.monotonic_ns(),
        )

    def stop(self) -> None:
        return None


def _camera_process(camera_id: str, session: bytes) -> None:
    service = CameraService(
        camera_id,
        _config(),
        _LoopBackend,
        session_uuid=UUID(bytes=session),
        install_signals=True,
    )
    service.run()


def _attach_until(reader: SharedMemoryFrameReader, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if reader.attach():
            return
        time.sleep(0.01)
    raise AssertionError("reader did not attach before deadline")


def _read_until_frame(reader: SharedMemoryFrameReader, timeout: float = 5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = reader.read()
        if result.status is ReadStatus.FRAME:
            return result.frame
        if result.status is ReadStatus.NOT_ATTACHED:
            reader.attach()
        time.sleep(0.001)
    raise AssertionError("reader did not receive a frame before deadline")


def test_real_writer_reader_processes_never_accept_torn_patterns() -> None:
    context = multiprocessing.get_context("spawn")
    camera_id = f"camera_{uuid4().hex}"
    ready, start, finished, stop = (context.Event() for _ in range(4))
    writer = context.Process(
        target=_writer_process,
        args=(camera_id, 300, ready, start, finished, stop),
        name="phase6-writer",
    )
    reader = SharedMemoryFrameReader(camera_id, unregister_from_resource_tracker=False)
    writer.start()
    try:
        assert ready.wait(5.0)
        _attach_until(reader)
        start.set()
        accepted = 0
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            result = reader.read()
            if result.status is ReadStatus.FRAME:
                assert result.frame is not None
                expected = result.frame.frame_number % 251
                assert np.all(result.frame.pixels == expected)
                accepted += 1
            if finished.is_set() and result.status is ReadStatus.NO_FRAME:
                break
        assert accepted >= 5
        assert finished.is_set()
    finally:
        reader.close()
        stop.set()
        writer.join(5.0)
        if writer.is_alive():
            writer.kill()
            writer.join(2.0)
    assert writer.exitcode == 0


def test_dedicated_writer_and_reader_children_never_accept_torn_patterns() -> None:
    context = multiprocessing.get_context("spawn")
    camera_id = f"camera_{uuid4().hex}"
    writer_ready, reader_ready, start, finished, stop = (context.Event() for _ in range(5))
    results = context.Queue()
    writer = context.Process(
        target=_writer_process,
        args=(camera_id, 500, writer_ready, start, finished, stop),
        name="phase6-dedicated-writer",
    )
    reader = context.Process(
        target=_reader_process,
        args=(camera_id, reader_ready, start, finished, results),
        name="phase6-dedicated-reader",
    )
    writer.start()
    assert writer_ready.wait(5.0)
    reader.start()
    try:
        assert reader_ready.wait(5.0)
        start.set()
        reader.join(8.0)
        assert not reader.is_alive()
        records = []
        while True:
            record = results.get(timeout=2.0)
            records.append(record)
            if record[0] == "accepted":
                break
        accepted = next(value for label, value in records if label == "accepted")
        assert accepted >= 5
        assert all(valid for number, valid in records if isinstance(number, int))
    finally:
        stop.set()
        writer.join(5.0)
        if reader.is_alive():
            reader.kill()
            reader.join(2.0)
        if writer.is_alive():
            writer.kill()
            writer.join(2.0)
    assert reader.exitcode == 0
    assert writer.exitcode == 0


def test_rejected_reader_process_never_unlinks_camera_owned_segment() -> None:
    context = multiprocessing.get_context("spawn")
    camera_id = f"camera_{uuid4().hex}"
    owner = shared_memory.SharedMemory(name=shared_memory_name(camera_id), create=True, size=320)
    owner.buf[:] = bytes(owner.size)
    owner.buf[:8] = b"INVALID!"
    consumer = context.Process(
        target=_reject_invalid_reader_process,
        args=(camera_id,),
        name="phase6-invalid-consumer",
    )
    try:
        consumer.start()
        consumer.join(5.0)
        assert consumer.exitcode == 0
        verifier = shared_memory.SharedMemory(name=shared_memory_name(camera_id), create=False)
        verifier.close()
    finally:
        if consumer.is_alive():
            consumer.kill()
            consumer.join(2.0)
        owner.close()
        owner.unlink()


def test_reader_attachment_waits_for_creator_header_initialization() -> None:
    context = multiprocessing.get_context("spawn")
    camera_id = f"camera_{uuid4().hex}"
    name = shared_memory_name(camera_id)
    started, finished = context.Event(), context.Event()
    results = context.Queue()
    consumer = context.Process(
        target=_attach_during_creation_process,
        args=(camera_id, started, finished, results),
        name="phase6-creating-segment-consumer",
    )
    owner: shared_memory.SharedMemory | None = None
    try:
        with _startup_lock():
            owner = shared_memory.SharedMemory(name=name, create=True, size=segment_size(256))
            owner.buf[:] = bytes(owner.size)
            consumer.start()
            assert started.wait(5.0)
            assert not finished.wait(0.2)
            header = FrameHeader.initial(256, os.getpid(), uuid4().bytes)
            owner.buf[:HEADER_SIZE] = header.encode()
        consumer.join(5.0)
        assert consumer.exitcode == 0
        assert results.get(timeout=1.0) == (True, None)
    finally:
        if consumer.is_alive():
            consumer.kill()
            consumer.join(2.0)
        if owner is not None:
            owner.close()
            owner.unlink()


def test_crashed_owner_is_recovered_by_replacement_process_identity() -> None:
    context = multiprocessing.get_context("spawn")
    camera_id = f"camera_{uuid4().hex}"
    ready = context.Event()
    crashed = context.Process(target=_crash_owner, args=(camera_id, ready), name="phase6-crashed-owner")
    crashed.start()
    assert ready.wait(5.0)
    crashed.join(5.0)
    assert crashed.exitcode == 9
    stale = shared_memory.SharedMemory(name=shared_memory_name(camera_id), create=False)
    resource_tracker.unregister(stale._name, "shared_memory")  # type: ignore[attr-defined]
    stale.close()
    replacement_session = UUID(int=201).bytes
    replacement = SharedMemoryFrameWriter(camera_id, 256, replacement_session)
    reader = SharedMemoryFrameReader(camera_id, unregister_from_resource_tracker=False)
    try:
        replacement.open()
        replacement.write(_pattern(0))
        assert reader.attach()
        frame = _read_until_frame(reader)
        assert frame is not None
        assert frame.camera_session_id == replacement_session
        assert frame.frame_number == 0
    finally:
        reader.close()
        replacement.close()


def test_concurrent_replacements_serialize_stale_owner_recovery() -> None:
    context = multiprocessing.get_context("spawn")
    camera_id = f"camera_{uuid4().hex}"
    stale_ready = context.Event()
    crashed = context.Process(target=_crash_owner, args=(camera_id, stale_ready), name="phase6-race-stale-owner")
    crashed.start()
    assert stale_ready.wait(5.0)
    crashed.join(5.0)
    assert crashed.exitcode == 9

    start, stop = context.Event(), context.Event()
    results = context.Queue()
    replacements = [
        context.Process(
            target=_replacement_racer,
            args=(camera_id, start, stop, results),
            name=f"phase6-replacement-{index}",
        )
        for index in range(2)
    ]
    for process in replacements:
        process.start()
    try:
        start.set()
        outcomes = {results.get(timeout=5.0), results.get(timeout=5.0)}
        assert outcomes == {"created", "live-owner"}
        deadline = time.monotonic() + 2.0
        while sum(process.is_alive() for process in replacements) == 2 and time.monotonic() < deadline:
            time.sleep(0.01)
        assert sum(process.is_alive() for process in replacements) == 1
        verifier = shared_memory.SharedMemory(name=shared_memory_name(camera_id), create=False)
        verifier.close()
    finally:
        stop.set()
        for process in replacements:
            process.join(5.0)
            if process.is_alive():
                process.kill()
                process.join(2.0)
    assert all(process.exitcode == 0 for process in replacements)


def test_live_owner_duplicate_process_exits_78_without_unlinking_owner() -> None:
    context = multiprocessing.get_context("spawn")
    fixture = Path(__file__).parents[1] / "fixtures" / "config" / "valid" / "single_camera.yaml"
    ready, stop = context.Event(), context.Event()
    result = context.Queue()
    owner = context.Process(target=_hold_writer, args=("front_camera", ready, stop), name="phase6-live-owner")
    duplicate = context.Process(
        target=_duplicate_camera_entrypoint,
        args=(str(fixture), result),
        name="phase6-duplicate-owner",
    )
    reader = SharedMemoryFrameReader("front_camera", unregister_from_resource_tracker=False)
    owner.start()
    try:
        assert ready.wait(5.0)
        duplicate.start()
        duplicate.join(5.0)
        assert duplicate.exitcode == ExitCode.INVALID_CONFIGURATION
        assert result.get(timeout=1.0) == ExitCode.INVALID_CONFIGURATION
        assert owner.is_alive()
        assert reader.attach()
    finally:
        reader.close()
        stop.set()
        owner.join(5.0)
        if duplicate.is_alive():
            duplicate.kill()
            duplicate.join(2.0)
        if owner.is_alive():
            owner.kill()
            owner.join(2.0)
    assert owner.exitcode == 0


def test_reader_reattaches_after_killed_camera_and_keeps_old_private_frame() -> None:
    context = multiprocessing.get_context("spawn")
    camera_id = f"camera_{uuid4().hex}"
    session_a, session_b = uuid4().bytes, uuid4().bytes
    camera_a = context.Process(target=_camera_process, args=(camera_id, session_a), name="phase6-camera-a")
    reader = SharedMemoryFrameReader(camera_id, unregister_from_resource_tracker=False)
    camera_a.start()
    replacement: SharedMemoryFrameWriter | None = None
    try:
        _attach_until(reader)
        old_frame = _read_until_frame(reader)
        assert old_frame is not None and old_frame.camera_session_id == session_a
        old_pixels = np.array(old_frame.pixels, copy=True)
        camera_a.kill()
        camera_a.join(5.0)
        replacement = SharedMemoryFrameWriter(camera_id, 256, session_b)
        replacement.open()
        replacement.write(_pattern(0))
        assert reader.read().status is ReadStatus.NOT_ATTACHED
        assert reader.attach()
        new_frame = _read_until_frame(reader)
        assert new_frame is not None
        assert new_frame.camera_session_id == session_b
        assert new_frame.frame_number == 0
        assert np.array_equal(old_frame.pixels, old_pixels)
    finally:
        reader.close()
        if replacement is not None:
            replacement.close()
        if camera_a.is_alive():
            camera_a.kill()
            camera_a.join(2.0)


def test_sigterm_tears_down_camera_and_unlinks_within_five_seconds() -> None:
    context = multiprocessing.get_context("spawn")
    camera_id = f"camera_{uuid4().hex}"
    process = context.Process(target=_camera_process, args=(camera_id, uuid4().bytes), name="phase6-camera-shutdown")
    reader = SharedMemoryFrameReader(camera_id, unregister_from_resource_tracker=False)
    process.start()
    try:
        _attach_until(reader)
        assert _read_until_frame(reader) is not None
        reader.close()
        started = time.monotonic()
        process.terminate()
        process.join(5.0)
        assert time.monotonic() - started < 5.0
        assert process.exitcode == 0
        assert not reader.attach()
    finally:
        reader.close()
        if process.is_alive():
            process.kill()
            process.join(2.0)


def test_sigterm_during_degraded_backoff_unlinks_within_five_seconds() -> None:
    context = multiprocessing.get_context("spawn")
    camera_id = f"camera_{uuid4().hex}"
    process = context.Process(target=_degraded_camera_process, args=(camera_id,), name="phase6-degraded-shutdown")
    reader = SharedMemoryFrameReader(camera_id, unregister_from_resource_tracker=False)
    process.start()
    try:
        _attach_until(reader)
        assert reader.read().status is ReadStatus.NO_FRAME
        reader.close()
        started = time.monotonic()
        process.terminate()
        process.join(5.0)
        assert time.monotonic() - started < 5.0
        assert process.exitcode == 0
        assert not reader.attach()
    finally:
        reader.close()
        if process.is_alive():
            process.kill()
            process.join(2.0)


def test_killing_camera_a_does_not_stop_camera_b_or_cross_segments() -> None:
    context = multiprocessing.get_context("spawn")
    camera_a_id, camera_b_id = f"camera_{uuid4().hex}", f"camera_{uuid4().hex}"
    session_a, session_b = uuid4().bytes, uuid4().bytes
    camera_a = context.Process(target=_camera_process, args=(camera_a_id, session_a), name="phase6-isolation-a")
    camera_b = context.Process(target=_camera_process, args=(camera_b_id, session_b), name="phase6-isolation-b")
    reader_a = SharedMemoryFrameReader(camera_a_id, unregister_from_resource_tracker=False)
    reader_b = SharedMemoryFrameReader(camera_b_id, unregister_from_resource_tracker=False)
    camera_a.start()
    camera_b.start()
    cleanup_a: SharedMemoryFrameWriter | None = None
    try:
        _attach_until(reader_a)
        _attach_until(reader_b)
        frame_a = _read_until_frame(reader_a)
        frame_b = _read_until_frame(reader_b)
        assert frame_a is not None and frame_a.camera_session_id == session_a
        assert frame_b is not None and frame_b.camera_session_id == session_b
        camera_a.kill()
        camera_a.join(5.0)
        assert camera_b.is_alive()
        assert _read_until_frame(reader_b).camera_session_id == session_b
        cleanup_a = SharedMemoryFrameWriter(camera_a_id, 256, uuid4().bytes)
        cleanup_a.open()
    finally:
        reader_a.close()
        reader_b.close()
        if cleanup_a is not None:
            cleanup_a.close()
        if camera_a.is_alive():
            camera_a.kill()
            camera_a.join(2.0)
        if camera_b.is_alive():
            camera_b.terminate()
            camera_b.join(5.0)
    assert camera_b.exitcode == 0


def test_real_gstreamer_videotestsrc_receipt_and_null_teardown() -> None:
    _require_gstreamer()
    backend = GStreamerCaptureBackend(16, 12, 10)
    pipeline = None
    gst = None
    try:
        backend.start()
        pipeline = backend._pipeline  # type: ignore[attr-defined]
        gst = backend._gst  # type: ignore[attr-defined]
        deadline = time.monotonic() + 5.0
        frame = None
        while frame is None and time.monotonic() < deadline:
            frame = backend.poll(0.250)
        assert frame is not None
        assert (frame.width, frame.height, frame.pixel_format) == (16, 12, PixelFormat.BGR8)
        assert frame.capture_time_unix_ns > 0
        assert frame.capture_monotonic_ns > 0
    finally:
        backend.stop()
    assert pipeline is not None and gst is not None
    _result, current, _pending = pipeline.get_state(0)
    assert current == gst.State.NULL


def test_real_gstreamer_camera_service_rebuilds_into_shared_memory_and_tears_down() -> None:
    _require_gstreamer()
    context = multiprocessing.get_context("spawn")
    camera_id = f"camera_{uuid4().hex}"
    session = uuid4().bytes
    process = context.Process(
        target=_gstreamer_camera_process,
        args=(camera_id, session),
        name="phase6-real-gstreamer-camera",
    )
    reader = SharedMemoryFrameReader(camera_id, unregister_from_resource_tracker=False)
    process.start()
    try:
        _attach_until(reader)
        first = _read_until_frame(reader)
        assert first is not None
        assert first.camera_session_id == session
        assert first.frame_number == 0
        rebuilt = _read_until_frame(reader, timeout=8.0)
        assert rebuilt is not None
        assert rebuilt.camera_session_id == session
        assert rebuilt.frame_number > first.frame_number
        reader.close()
        process.terminate()
        process.join(5.0)
        assert process.exitcode == 0
        assert not reader.attach()
    finally:
        reader.close()
        if process.is_alive():
            process.kill()
            process.join(2.0)
