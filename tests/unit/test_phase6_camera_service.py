"""Camera state, timeout, rebuild, identity, and retry unit tests."""

from __future__ import annotations

from collections import deque
from pathlib import Path
from uuid import uuid4

import pytest

from purdue_rov_cv.camera import (
    CameraService,
    CaptureBackendError,
    CapturedFrame,
    GStreamerCaptureBackend,
    RetryController,
)
from purdue_rov_cv.camera.entrypoints import camera_entrypoint
from purdue_rov_cv.config.models import CameraAdapter, CameraConfig, CameraFormat, CameraPathKind
from purdue_rov_cv.frame_buffer import PixelFormat, SharedMemoryFrameWriter
from purdue_rov_cv.runtime.exit_codes import ExitCode
from purdue_rov_cv.runtime.metrics import RuntimeMetrics
from purdue_rov_cv.runtime.state import ComponentState


class _Clock:
    def __init__(self) -> None:
        self.seconds = 10.0

    def monotonic(self) -> float:
        return self.seconds

    def monotonic_ns(self) -> int:
        return int(self.seconds * 1_000_000_000)

    def advance(self, seconds: float) -> None:
        self.seconds += seconds


class _Backend:
    def __init__(
        self,
        events: list[CapturedFrame | Exception],
        *,
        fail_start: bool = False,
        fail_stop: bool = False,
    ) -> None:
        self.events = deque(events)
        self.fail_start = fail_start
        self.fail_stop = fail_stop
        self.started = False
        self.stopped = False

    def start(self) -> None:
        if self.fail_start:
            raise CaptureBackendError("injected start failure")
        self.started = True

    def poll(self, _timeout_seconds: float) -> CapturedFrame | None:
        if not self.events:
            return None
        event = self.events.popleft()
        if isinstance(event, Exception):
            raise event
        return event

    def stop(self) -> None:
        self.stopped = True
        if self.fail_stop:
            raise CaptureBackendError("injected stop failure")


def _config() -> CameraConfig:
    return CameraConfig(
        adapter=CameraAdapter.V4L2,
        device_path=Path("/dev/simulated"),
        device_path_kind=CameraPathKind.FALLBACK,
        format=CameraFormat.MJPEG,
        width=4,
        height=3,
        frame_rate=30,
        stream_index=0,
        stream_to_surface=False,
        cv_enabled=True,
        allow_software_encode=True,
        slot_capacity_bytes=64,
    )


def _frame(clock: _Clock, value: int) -> CapturedFrame:
    return CapturedFrame(
        bytes([value]) * 36,
        4,
        3,
        12,
        PixelFormat.BGR8,
        1_000,
        clock.monotonic_ns(),
    )


def _service(clock: _Clock, backends: list[_Backend]) -> CameraService:
    queue = deque(backends)
    return CameraService(
        f"camera_{uuid4().hex}",
        _config(),
        queue.popleft,
        session_uuid=uuid4(),
        metrics=RuntimeMetrics(monotonic=clock.monotonic),
        monotonic=clock.monotonic,
        monotonic_ns=clock.monotonic_ns,
    )


def test_retry_controller_uses_capped_exponential_sequence_and_resets() -> None:
    retry = RetryController()
    assert [retry.failed() for _ in range(10)] == [0.5, 1.0, 2.0, 4.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0]
    retry.succeeded()
    assert retry.failed() == 0.5


def test_gstreamer_pipeline_has_explicit_caps_and_bounded_dropping_appsink() -> None:
    description = GStreamerCaptureBackend(640, 480, 30).pipeline_description()
    assert "videotestsrc name=source is-live=true" in description
    assert "width=640,height=480,framerate=30/1" in description
    assert "format=BGR" in description
    assert "appsink" in description
    assert "max-buffers=1 drop=true" in description


def test_first_frame_runs_and_rebuild_preserves_session_and_numbering() -> None:
    clock = _Clock()
    failed = _Backend([_frame(clock, 1), CaptureBackendError("injected pipeline failure")])
    recovered = _Backend([_frame(clock, 2)])
    service = _service(clock, [failed, recovered])
    session = service.session_uuid
    try:
        service.initialize()
        assert service.state_machine.state is ComponentState.READY
        service.step()
        assert service.state_machine.state is ComponentState.RUNNING
        assert service.next_frame_number == 1
        service.step()
        assert failed.stopped
        assert service.state_machine.state is ComponentState.DEGRADED
        clock.advance(0.5)
        service.step()
        assert service.state_machine.state is ComponentState.READY
        service.step()
        assert service.state_machine.state is ComponentState.RUNNING
        assert service.session_uuid == session
        assert service.next_frame_number == 2
        snapshot = service.metrics.snapshot().values
        assert snapshot["pipeline_restarts"] == 1
        assert snapshot["frames_received"] == 2
        assert snapshot["shared_memory_write_count"] == 2
    finally:
        service.close()
    assert recovered.stopped
    assert service.state_machine.state is ComponentState.STOPPED


def test_two_second_timeout_is_one_event_and_requests_rebuild() -> None:
    clock = _Clock()
    backend = _Backend([])
    replacement = _Backend([])
    service = _service(clock, [backend, replacement])
    try:
        service.initialize()
        clock.advance(1.999)
        service.step()
        assert service.metrics.snapshot().values["frame_timeouts"] == 0
        clock.advance(0.001)
        service.step()
        assert service.state_machine.state is ComponentState.DEGRADED
        assert service.metrics.snapshot().values["frame_timeouts"] == 1
        service.step()
        assert service.metrics.snapshot().values["frame_timeouts"] == 1
        clock.advance(0.5)
        service.step()
        assert service.metrics.snapshot().values["pipeline_restarts"] == 1
    finally:
        service.close()


def test_failed_initial_and_rebuild_attempts_remain_degraded() -> None:
    clock = _Clock()
    service = _service(clock, [_Backend([], fail_start=True), _Backend([], fail_start=True), _Backend([])])
    try:
        service.initialize()
        assert service.state_machine.state is ComponentState.DEGRADED
        clock.advance(0.5)
        service.step()
        assert service.state_machine.state is ComponentState.DEGRADED
        clock.advance(1.0)
        service.step()
        assert service.state_machine.state is ComponentState.READY
        assert service.metrics.snapshot().values["pipeline_restarts"] == 2
    finally:
        service.close()


def test_backend_factory_failure_is_retried_in_degraded_state() -> None:
    clock = _Clock()
    recovered = _Backend([_frame(clock, 4)])
    attempts = 0

    def factory():
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise CaptureBackendError("injected factory failure")
        return recovered

    service = CameraService(
        f"camera_{uuid4().hex}",
        _config(),
        factory,
        metrics=RuntimeMetrics(monotonic=clock.monotonic),
        monotonic=clock.monotonic,
        monotonic_ns=clock.monotonic_ns,
    )
    try:
        service.initialize()
        assert service.state_machine.state is ComponentState.DEGRADED
        clock.advance(0.5)
        service.step()
        assert service.state_machine.state is ComponentState.READY
        service.step()
        assert service.state_machine.state is ComponentState.RUNNING
        assert service.metrics.snapshot().values["pipeline_restarts"] == 1
    finally:
        service.close()


def test_teardown_failure_does_not_bypass_rebuild_or_owned_cleanup() -> None:
    clock = _Clock()
    failed = _Backend([CaptureBackendError("injected pipeline failure")], fail_stop=True)
    recovered = _Backend([_frame(clock, 8)])
    service = _service(clock, [failed, recovered])
    service.initialize()
    service.step()
    assert failed.stopped
    assert service.state_machine.state is ComponentState.DEGRADED
    clock.advance(0.5)
    service.step()
    assert service.state_machine.state is ComponentState.READY
    result = service.close()
    assert result.completed
    assert not result.failures
    assert service.state_machine.state is ComponentState.STOPPED


def test_close_attempts_writer_cleanup_when_active_backend_stop_fails() -> None:
    clock = _Clock()
    backend = _Backend([], fail_stop=True)
    service = _service(clock, [backend])
    service.initialize()
    result = service.close()
    assert result.completed
    assert [failure.hook_name for failure in result.failures] == ["capture-backend"]
    assert service.state_machine.state is ComponentState.STOPPED
    assert not service.writer.created


def test_accepted_frame_updates_current_frame_metrics() -> None:
    clock = _Clock()
    captured = CapturedFrame(
        bytes([1, 2, 90, 91, 3, 4, 92, 93]),
        2,
        2,
        4,
        PixelFormat.GRAY8,
        1_000,
        clock.monotonic_ns(),
    )
    service = _service(clock, [_Backend([captured])])
    try:
        service.initialize()
        service.step()
        values = service.metrics.snapshot().values
        assert values["current_width"] == 2
        assert values["current_height"] == 2
        assert values["current_pixel_format"] == "GRAY8"
        assert values["last_frame_age_ms"] == 0.0
    finally:
        service.close()


def test_ready_can_degrade_when_pipeline_fails_before_first_frame() -> None:
    clock = _Clock()
    service = _service(clock, [_Backend([CaptureBackendError("early failure")])])
    try:
        service.initialize()
        assert service.state_machine.state is ComponentState.READY
        service.step()
        assert service.state_machine.state is ComponentState.DEGRADED
    finally:
        service.close()


def test_shutdown_interrupts_backoff_wait(monkeypatch: pytest.MonkeyPatch) -> None:
    clock = _Clock()
    service = _service(clock, [_Backend([], fail_start=True)])
    service.initialize()
    monkeypatch.setattr(service.shutdown.token, "wait", lambda _timeout: service.request_shutdown("test") or True)
    service.step()
    assert service.shutdown.token.is_requested
    service.close()


def test_camera_entrypoint_maps_live_owner_to_exit_78() -> None:
    fixture = Path(__file__).parents[1] / "fixtures" / "config" / "valid" / "single_camera.yaml"
    writer = SharedMemoryFrameWriter("front_camera", 6_220_800, uuid4().bytes)
    try:
        writer.open()
        assert (
            camera_entrypoint(["--camera", "front_camera", "--config", str(fixture)]) == ExitCode.INVALID_CONFIGURATION
        )
    finally:
        writer.close()
