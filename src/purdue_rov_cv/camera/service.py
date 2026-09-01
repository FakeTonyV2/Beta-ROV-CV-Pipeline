"""Production-style simulated camera process and rebuild supervisor."""

from __future__ import annotations

import time
from collections.abc import Callable
from uuid import UUID, uuid4

from purdue_rov_cv.config.models import CameraConfig
from purdue_rov_cv.frame_buffer import FrameWrite, SharedMemoryFrameWriter
from purdue_rov_cv.runtime.json_logging import StructuredJsonLogger
from purdue_rov_cv.runtime.metrics import RuntimeMetrics
from purdue_rov_cv.runtime.rate_limit import WarningRateLimiter
from purdue_rov_cv.runtime.shutdown import ShutdownCoordinator, ShutdownResult, install_signal_handlers
from purdue_rov_cv.runtime.state import ComponentState, ComponentStateMachine

from .backend import CaptureBackend, CaptureBackendError, CapturedFrame

FRAME_TIMEOUT_NS = 2_000_000_000
POLL_SECONDS = 0.100
RETRY_DELAYS_SECONDS = (0.5, 1.0, 2.0, 4.0, 5.0)

BackendFactory = Callable[[], CaptureBackend]


class RetryController:
    def __init__(self) -> None:
        self.consecutive_failures = 0

    def failed(self) -> float:
        delay = RETRY_DELAYS_SECONDS[min(self.consecutive_failures, len(RETRY_DELAYS_SECONDS) - 1)]
        self.consecutive_failures += 1
        return delay

    def succeeded(self) -> None:
        self.consecutive_failures = 0


class CameraService:
    """Own camera session identity, backend, shared memory, and lifecycle."""

    def __init__(
        self,
        camera_id: str,
        config: CameraConfig,
        backend_factory: BackendFactory,
        *,
        session_uuid: UUID | None = None,
        metrics: RuntimeMetrics | None = None,
        logger: StructuredJsonLogger | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        monotonic_ns: Callable[[], int] = time.monotonic_ns,
        install_signals: bool = False,
        writer_factory: Callable[..., SharedMemoryFrameWriter] = SharedMemoryFrameWriter,
    ) -> None:
        self.camera_id = camera_id
        self.config = config
        self.backend_factory = backend_factory
        self.session_uuid = session_uuid or uuid4()
        self.metrics = metrics or RuntimeMetrics(monotonic=monotonic)
        self.logger = logger
        self._monotonic = monotonic
        self._monotonic_ns = monotonic_ns
        self.state_machine = ComponentStateMachine(observer=self._observe_state)
        self.metrics.set_metadata("state", ComponentState.STARTING.value)
        self.shutdown = ShutdownCoordinator(state_machine=self.state_machine, monotonic=monotonic)
        self.writer = writer_factory(
            camera_id,
            config.slot_capacity_bytes,
            self.session_uuid.bytes,
            metrics=self.metrics,
        )
        self.shutdown.register("capture-backend", self._shutdown_backend, order=10)
        self.shutdown.register("shared-memory-writer", self._shutdown_writer, order=20)
        self._backend: CaptureBackend | None = None
        self._retry = RetryController()
        self._warning_limiter = WarningRateLimiter(interval_seconds=5.0, monotonic=monotonic)
        self._next_retry = 0.0
        self._last_accepted_ns = 0
        self._next_frame_number = 0
        self._install_signals = install_signals
        self._initialized = False
        self._fps_started = monotonic()
        self._fps_frames = 0

    @property
    def next_frame_number(self) -> int:
        return self._next_frame_number

    def _observe_state(self, _result: object) -> None:
        self.metrics.set_metadata("state", self.state_machine.state.value)

    def _log(self, level: str, event_code: str, message: str, *, frame_number: int | None = None) -> None:
        if self.logger is not None:
            self.logger.log(
                level,
                event_code,
                message,
                camera_id=self.camera_id,
                camera_session_id=self.session_uuid,
                frame_number=frame_number,
                context={"state": self.state_machine.state.value},
            )

    def _start_backend(self, *, rebuild: bool) -> bool:
        if rebuild:
            self.metrics.increment("pipeline_restarts")
        candidate: CaptureBackend | None = None
        try:
            candidate = self.backend_factory()
            candidate.start()
        except Exception as error:
            if candidate is not None:
                try:
                    candidate.stop()
                except Exception as teardown_error:
                    self._log(
                        "ERROR",
                        "CAMERA_PIPELINE_TEARDOWN_FAILED",
                        f"failed candidate cleanup: {type(teardown_error).__name__}: {teardown_error}",
                    )
            self._schedule_retry(error)
            return False
        self._backend = candidate
        self._retry.succeeded()
        self._last_accepted_ns = self._monotonic_ns()
        if self.state_machine.state is ComponentState.DEGRADED:
            self.state_machine.transition_to(ComponentState.READY)
        elif self.state_machine.state is ComponentState.STARTING:
            self.state_machine.transition_to(ComponentState.READY)
        self._log("INFO", "CAMERA_PIPELINE_READY", "simulated capture pipeline is ready")
        return True

    def _schedule_retry(self, error: BaseException) -> None:
        delay = self._retry.failed()
        self._next_retry = self._monotonic() + delay
        if self.state_machine.state not in {
            ComponentState.DEGRADED,
            ComponentState.STOPPING,
            ComponentState.STOPPED,
        }:
            self.state_machine.transition_to(ComponentState.DEGRADED)
        decision = self._warning_limiter.check(("CAMERA_PIPELINE_RETRY", type(error).__name__, str(error)))
        if decision.emit:
            self._log(
                "WARNING",
                "CAMERA_PIPELINE_RETRY",
                f"capture pipeline unavailable; retrying in {delay:g} seconds: {type(error).__name__}: {error}",
            )
        else:
            self.metrics.increment("warnings_suppressed")

    def initialize(self) -> None:
        if self._initialized:
            return
        self.writer.open()
        self.metrics.set_gauge("current_width", self.config.width)
        self.metrics.set_gauge("current_height", self.config.height)
        self.metrics.set_gauge("usb_device_present", False)
        self.metrics.set_metadata("current_pixel_format", "BGR8")
        self._initialized = True
        self._start_backend(rebuild=False)

    def _lose_backend(self, error: BaseException, *, timed_out: bool) -> None:
        backend = self._backend
        self._backend = None
        if backend is not None:
            try:
                backend.stop()
            except Exception as teardown_error:
                self._log(
                    "ERROR",
                    "CAMERA_PIPELINE_TEARDOWN_FAILED",
                    f"pipeline cleanup failed during recovery: {type(teardown_error).__name__}: {teardown_error}",
                )
        if timed_out:
            self.metrics.increment("frame_timeouts")
            self.metrics.set_gauge("frames_per_second", 0.0)
            self._log("WARNING", "CAMERA_FRAME_TIMEOUT", "no accepted frame arrived for two seconds")
        self._schedule_retry(error)

    def _accept(self, captured: CapturedFrame) -> bool:
        frame_number = self._next_frame_number
        try:
            self.writer.write(
                FrameWrite(
                    data=captured.data,
                    width=captured.width,
                    height=captured.height,
                    stride_bytes=captured.stride_bytes,
                    pixel_format=captured.pixel_format,
                    frame_number=frame_number,
                    capture_time_unix_ns=captured.capture_time_unix_ns,
                    capture_monotonic_ns=captured.capture_monotonic_ns,
                )
            )
        except (TypeError, ValueError) as error:
            self._log("ERROR", "CAMERA_FRAME_REJECTED", str(error), frame_number=frame_number)
            return False
        self._next_frame_number += 1
        self._last_accepted_ns = self._monotonic_ns()
        self.metrics.increment("frames_received")
        self.metrics.set_gauge("current_width", captured.width)
        self.metrics.set_gauge("current_height", captured.height)
        self.metrics.set_metadata("current_pixel_format", captured.pixel_format.name)
        self.metrics.set_gauge("last_frame_age_ms", 0.0)
        self._fps_frames += 1
        elapsed = self._monotonic() - self._fps_started
        if elapsed >= 1.0:
            self.metrics.set_gauge("frames_per_second", self._fps_frames / elapsed)
            self._fps_frames = 0
            self._fps_started = self._monotonic()
        if self.state_machine.state in {ComponentState.READY, ComponentState.DEGRADED}:
            self.state_machine.transition_to(ComponentState.RUNNING)
        return True

    def step(self) -> None:
        if not self._initialized:
            raise RuntimeError("camera service is not initialized")
        if self.shutdown.token.is_requested:
            return
        if self._backend is None:
            remaining = self._next_retry - self._monotonic()
            if remaining > 0:
                self.shutdown.token.wait(min(POLL_SECONDS, remaining))
                return
            self._start_backend(rebuild=True)
            return
        try:
            frame = self._backend.poll(POLL_SECONDS)
        except CaptureBackendError as error:
            self._lose_backend(error, timed_out=False)
            return
        if frame is not None:
            self._accept(frame)
            return
        age_ns = self._monotonic_ns() - self._last_accepted_ns
        self.metrics.set_gauge("last_frame_age_ms", max(0.0, age_ns / 1_000_000))
        if age_ns >= FRAME_TIMEOUT_NS:
            self._lose_backend(CaptureBackendError("frame timeout"), timed_out=True)

    def request_shutdown(self, reason: str = "requested") -> None:
        self.shutdown.request(reason)

    def _shutdown_backend(self) -> None:
        backend = self._backend
        self._backend = None
        if backend is not None:
            backend.stop()

    def _shutdown_writer(self) -> None:
        self.writer.close(unlink=True)

    def close(self) -> ShutdownResult:
        if self.state_machine.state not in {ComponentState.STOPPING, ComponentState.STOPPED}:
            self.shutdown.request("camera service close")
        return self.shutdown.run(timeout_seconds=4.5)

    def run(self) -> None:
        if self._install_signals:
            install_signal_handlers(self.shutdown)
        try:
            self.initialize()
            while not self.shutdown.token.is_requested:
                self.step()
        finally:
            result = self.close()
        if result.timed_out:
            raise CaptureBackendError("camera shutdown exceeded its 4.5-second cleanup deadline")
        if result.failures:
            detail = "; ".join(
                f"{failure.hook_name}: {failure.exception_type}: {failure.message}" for failure in result.failures
            )
            raise CaptureBackendError(f"camera cleanup failed: {detail}")


__all__ = [
    "FRAME_TIMEOUT_NS",
    "POLL_SECONDS",
    "RETRY_DELAYS_SECONDS",
    "BackendFactory",
    "CameraService",
    "RetryController",
]
