"""Surface video receiver lifecycle, timeout, rebuild, and recovery service."""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from threading import RLock, Thread
from typing import Protocol

from purdue_rov_cv.config.models import AppConfig, CameraConfig
from purdue_rov_cv.config.ports import StreamAllocation, derive_stream_allocation
from purdue_rov_cv.runtime.json_logging import StructuredJsonLogger
from purdue_rov_cv.runtime.metrics import RuntimeMetrics
from purdue_rov_cv.runtime.rate_limit import WarningRateLimiter
from purdue_rov_cv.runtime.shutdown import ShutdownCoordinator, ShutdownResult, install_signal_handlers
from purdue_rov_cv.runtime.state import ComponentState, ComponentStateMachine
from purdue_rov_cv.wire.errors import ErrorCode

from .cache import FrameIndexCache
from .correlation import FrameCorrelator
from .fanout import LocalVideoFanout
from .gstreamer import GStreamerRtpReceiver, VideoReceiverBackendError
from .health import VideoHealthPublisher
from .models import DecodedVideoFrame, EncodedAccessUnit, ReceivedVideoFrame
from .subscriber import FrameIndexSubscriber

RTP_TIMEOUT_NS = 2_000_000_000
REBUILD_RETRY_SECONDS = 1.0
SUPERVISOR_POLL_SECONDS = 0.050
RECOVERY_VALID_FRAMES = 5


class ReceiverBackend(Protocol):
    @property
    def running(self) -> bool: ...

    def start(self) -> None: ...

    def check_bus(self) -> None: ...

    def stop(self) -> None: ...


@dataclass(frozen=True, slots=True)
class ReceiverCallbacks:
    on_packet: Callable[[int, int, int], None]
    on_packet_lost: Callable[[int], None]
    on_decoded: Callable[[DecodedVideoFrame], None]
    on_invalid_decoded: Callable[[str], None]
    on_encoded: Callable[[EncodedAccessUnit], None]


BackendFactory = Callable[[ReceiverCallbacks], ReceiverBackend]


class VideoReceiverService:
    """One independently supervised receiver for one configured camera."""

    def __init__(
        self,
        camera_id: str,
        camera: CameraConfig,
        subscriber_endpoint: str,
        publisher_endpoint: str,
        *,
        health_interval_ms: int,
        backend_factory: BackendFactory | None = None,
        metrics: RuntimeMetrics | None = None,
        logger: StructuredJsonLogger | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        monotonic_ns: Callable[[], int] = time.monotonic_ns,
        install_signals: bool = False,
        approximate_debug: bool = False,
    ) -> None:
        if not camera.stream_to_surface:
            raise ValueError(f"camera {camera_id!r} is not configured to stream to the surface")
        self.camera_id = camera_id
        self.camera = camera
        self.allocation: StreamAllocation = derive_stream_allocation(camera_id, camera.stream_index)
        self.metrics = metrics or RuntimeMetrics(monotonic=monotonic)
        self.logger = logger
        self._monotonic = monotonic
        self._monotonic_ns = monotonic_ns
        self.state_machine = ComponentStateMachine(observer=self._observe_state)
        self.metrics.set_metadata("state", ComponentState.STARTING.value)
        self.shutdown = ShutdownCoordinator(state_machine=self.state_machine, monotonic=monotonic)
        self.decoded_fanout: LocalVideoFanout[ReceivedVideoFrame] = LocalVideoFanout()
        self.encoded_fanout: LocalVideoFanout[EncodedAccessUnit] = LocalVideoFanout()
        self.cache = FrameIndexCache(camera_id, monotonic_ns=monotonic_ns)
        self.correlator = FrameCorrelator(
            self.cache,
            self._deliver,
            metrics=self.metrics,
            approximate_debug=approximate_debug,
            monotonic_ns=monotonic_ns,
        )
        self.subscriber = FrameIndexSubscriber(
            subscriber_endpoint,
            camera_id,
            self.allocation.rtp_payload_type,
            self.correlator,
            metrics=self.metrics,
            shutdown=self.shutdown.token,
            logger=logger,
        )
        self.health = VideoHealthPublisher(
            publisher_endpoint,
            f"video_receiver_{camera.stream_index}",
            camera_id,
            interval_ms=health_interval_ms,
            metrics=self.metrics,
            state_machine=self.state_machine,
            shutdown=self.shutdown.token,
        )
        self._backend_factory = backend_factory or (
            lambda cb: GStreamerRtpReceiver(
                self.allocation.rtp_port,
                self.allocation.rtp_payload_type,
                on_packet=cb.on_packet,
                on_packet_lost=cb.on_packet_lost,
                on_decoded=cb.on_decoded,
                on_invalid_decoded=cb.on_invalid_decoded,
                on_encoded=cb.on_encoded,
                monotonic_ns=monotonic_ns,
            )
        )
        self._backend: ReceiverBackend | None = None
        self._subscriber_thread: Thread | None = None
        self._health_thread: Thread | None = None
        self._next_rebuild = 0.0
        self._last_packet_ns = 0
        self._last_decoded_ns = 0
        self._recovery_streak = 0
        self._stream_status = "STARTING"
        self._initialized = False
        self._install_signals = install_signals
        self._lock = RLock()
        self._warning_limiter = WarningRateLimiter(interval_seconds=5.0, monotonic=monotonic)
        self.shutdown.register("frame-index-subscriber", self._stop_subscriber, order=10)
        self.shutdown.register("health-publisher", self._stop_health, order=20)
        self.shutdown.register("gstreamer-receiver", self._stop_backend, order=30)
        self.shutdown.register("correlator", self.correlator.close, order=40)
        self.shutdown.register("local-fanout", self._close_fanout, order=50)

    @classmethod
    def from_config(
        cls,
        camera_id: str,
        config: AppConfig,
        *,
        backend_factory: BackendFactory | None = None,
        metrics: RuntimeMetrics | None = None,
        logger: StructuredJsonLogger | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        monotonic_ns: Callable[[], int] = time.monotonic_ns,
        install_signals: bool = False,
        approximate_debug: bool = False,
    ) -> VideoReceiverService:
        if camera_id not in config.cameras:
            raise ValueError(f"unknown configured camera: {camera_id}")
        return cls(
            camera_id,
            config.cameras[camera_id],
            config.messaging.broker.subscriber_endpoint,
            config.messaging.broker.publisher_endpoint,
            health_interval_ms=config.diagnostics.publish_interval_ms,
            backend_factory=backend_factory,
            metrics=metrics,
            logger=logger,
            monotonic=monotonic,
            monotonic_ns=monotonic_ns,
            install_signals=install_signals,
            approximate_debug=approximate_debug,
        )

    @property
    def stream_status(self) -> str:
        with self._lock:
            return self._stream_status

    @property
    def backend(self) -> ReceiverBackend | None:
        with self._lock:
            return self._backend

    def _observe_state(self, _result: object) -> None:
        self.metrics.set_metadata("state", self.state_machine.state.value)

    def _log(self, level: str, code: str, message: str, **context: object) -> None:
        if self.logger is not None:
            self.logger.log(level, code, message, camera_id=self.camera_id, context=context)

    def _start_backend(self, *, rebuild: bool) -> bool:
        if rebuild:
            self.metrics.increment("stream_restarts")
        candidate: ReceiverBackend | None = None
        try:
            callbacks = ReceiverCallbacks(
                self._packet,
                self._packet_lost,
                self._decoded,
                self._invalid_decoded,
                self.encoded_fanout.publish,
            )
            candidate = self._backend_factory(callbacks)
            candidate.start()
        except Exception as error:
            if candidate is not None:
                try:
                    candidate.stop()
                except Exception:
                    pass
            self._degrade(error, destroy=False)
            return False
        with self._lock:
            self._backend = candidate
            self._last_packet_ns = self._monotonic_ns()
            self._recovery_streak = 0
            self._stream_status = "WAITING FOR STREAM" if rebuild else "READY"
        if self.state_machine.state is ComponentState.STARTING:
            self.state_machine.transition_to(ComponentState.READY)
        self._log("INFO", "VIDEO_PIPELINE_READY", "surface RTP receiver pipeline is ready", rebuild=rebuild)
        return True

    def initialize(self) -> None:
        if self._initialized:
            return
        self._initialized = True
        self._subscriber_thread = Thread(target=self.subscriber.run, name=f"frame-index:{self.camera_id}", daemon=True)
        self._health_thread = Thread(target=self.health.run, name=f"video-health:{self.camera_id}", daemon=True)
        self._subscriber_thread.start()
        self._health_thread.start()
        self._start_backend(rebuild=False)

    def _packet(self, _ssrc: int, _timestamp: int, received_ns: int) -> None:
        with self._lock:
            self._last_packet_ns = received_ns
        self.metrics.increment("rtp_packets_received")

    def _packet_lost(self, count: int) -> None:
        self.metrics.increment("rtp_packets_lost", count)

    def _decoded(self, frame: DecodedVideoFrame) -> None:
        if self.shutdown.token.is_requested:
            return
        if frame.width <= 0 or frame.height <= 0 or not frame.pixels:
            self._invalid_decoded("decoded frame failed dimensions/data validation")
            return
        self.metrics.increment("decoded_frames")
        self.metrics.set_gauge("current_width", frame.width)
        self.metrics.set_gauge("current_height", frame.height)
        self.metrics.set_metadata("current_pixel_format", frame.pixel_format)
        self.metrics.set_gauge("last_frame_age_ms", 0.0)
        with self._lock:
            self._last_decoded_ns = frame.received_monotonic_ns
            self._stream_status = "VIDEO AVAILABLE"
            if self.state_machine.state is ComponentState.DEGRADED:
                self._recovery_streak += 1
                if self._recovery_streak >= RECOVERY_VALID_FRAMES:
                    self.state_machine.transition_to(ComponentState.RUNNING)
            elif self.state_machine.state is ComponentState.READY:
                self.state_machine.transition_to(ComponentState.RUNNING)
        self.correlator.submit_frame(frame)

    def _invalid_decoded(self, reason: str) -> None:
        with self._lock:
            self._recovery_streak = 0
        self._log("WARNING", "VIDEO_FRAME_INVALID", reason)

    def _deliver(self, result: ReceivedVideoFrame) -> None:
        self.decoded_fanout.publish(result)
        identity = result.correlation.identity
        self._log(
            "DEBUG",
            "VIDEO_FRAME_CORRELATED",
            "decoded frame correlation completed",
            rtp_ssrc=result.frame.rtp_ssrc,
            rtp_timestamp=result.frame.rtp_timestamp,
            correlation=result.correlation.quality.value,
            camera_session_id=None if identity is None else identity.camera_session_id.hex(),
            frame_number=None if identity is None else identity.frame_number,
        )

    def _degrade(self, error: BaseException, *, destroy: bool = True) -> None:
        if destroy:
            try:
                self._stop_backend()
            except Exception as teardown_error:
                self._log(
                    "ERROR",
                    "VIDEO_PIPELINE_TEARDOWN_FAILED",
                    "receiver teardown failed during recovery",
                    error=f"{type(teardown_error).__name__}: {teardown_error}",
                )
        self.correlator.reset_stream()
        with self._lock:
            self._stream_status = "STREAM LOST"
            self._recovery_streak = 0
            self._next_rebuild = self._monotonic() + REBUILD_RETRY_SECONDS
        if self.state_machine.state not in {
            ComponentState.DEGRADED,
            ComponentState.STOPPING,
            ComponentState.STOPPED,
        }:
            self.state_machine.transition_to(ComponentState.DEGRADED)
        self.metrics.set_metadata("last_error_code", ErrorCode.VIDEO_STREAM_LOST.value)
        self.metrics.set_metadata("last_error_message", f"{type(error).__name__}: {error}")
        warning = self._warning_limiter.check((type(error).__name__, str(error)))
        if warning.emit:
            self._log(
                "WARNING",
                ErrorCode.VIDEO_STREAM_LOST.value,
                "surface RTP stream lost; pipeline rebuild scheduled",
                error=f"{type(error).__name__}: {error}",
                previously_suppressed=warning.suppressed_count,
            )
        else:
            self.metrics.increment("warnings_suppressed")

    def _check_background_threads(self) -> None:
        if self.shutdown.token.is_requested:
            return
        for name, thread in (
            ("FrameIndex subscriber", self._subscriber_thread),
            ("video health publisher", self._health_thread),
        ):
            if thread is None or thread.is_alive():
                continue
            message = f"{name} exited unexpectedly"
            self.metrics.set_metadata("last_error_code", ErrorCode.INTERNAL_ERROR.value)
            self.metrics.set_metadata("last_error_message", message)
            self._log("ERROR", ErrorCode.INTERNAL_ERROR.value, message)
            if self.state_machine.state not in {
                ComponentState.ERROR,
                ComponentState.STOPPING,
                ComponentState.STOPPED,
            }:
                self.state_machine.transition_to(ComponentState.ERROR)
            raise VideoReceiverBackendError(message)

    def step(self) -> None:
        if not self._initialized:
            raise RuntimeError("video receiver is not initialized")
        if self.shutdown.token.is_requested:
            return
        self._check_background_threads()
        self.correlator.expire()
        with self._lock:
            backend = self._backend
            last_packet_ns = self._last_packet_ns
            last_decoded_ns = self._last_decoded_ns
            next_rebuild = self._next_rebuild
        now_ns = self._monotonic_ns()
        if last_decoded_ns:
            self.metrics.set_gauge("last_frame_age_ms", max(0.0, (now_ns - last_decoded_ns) / 1_000_000))
        if backend is None:
            remaining = next_rebuild - self._monotonic()
            if remaining > 0:
                self.shutdown.token.wait(min(SUPERVISOR_POLL_SECONDS, remaining))
                return
            self._start_backend(rebuild=True)
            return
        try:
            backend.check_bus()
        except Exception as error:
            self._degrade(error)
            return
        if now_ns - last_packet_ns >= RTP_TIMEOUT_NS:
            self._degrade(VideoReceiverBackendError("no RTP packet received for two seconds"))
            return
        self.shutdown.token.wait(SUPERVISOR_POLL_SECONDS)

    def request_shutdown(self, reason: str = "requested") -> None:
        self.shutdown.request(reason)

    def _stop_backend(self) -> None:
        with self._lock:
            backend = self._backend
            self._backend = None
        if backend is not None:
            backend.stop()

    def _join(self, thread: Thread | None, name: str) -> None:
        if thread is None:
            return
        thread.join(1.0)
        if thread.is_alive():
            raise VideoReceiverBackendError(f"{name} did not stop within one second")

    def _stop_subscriber(self) -> None:
        self._join(self._subscriber_thread, "FrameIndex subscriber")

    def _stop_health(self) -> None:
        self._join(self._health_thread, "video health publisher")

    def _close_fanout(self) -> None:
        self.decoded_fanout.close()
        self.encoded_fanout.close()

    def close(self) -> ShutdownResult:
        if self.state_machine.state not in {ComponentState.STOPPING, ComponentState.STOPPED}:
            self.shutdown.request("video receiver close")
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
            raise VideoReceiverBackendError("video receiver shutdown exceeded 4.5 seconds")
        if result.failures:
            detail = "; ".join(f"{item.hook_name}: {item.message}" for item in result.failures)
            raise VideoReceiverBackendError(f"video receiver cleanup failed: {detail}")


__all__ = [
    "BackendFactory",
    "REBUILD_RETRY_SECONDS",
    "RECOVERY_VALID_FRAMES",
    "RTP_TIMEOUT_NS",
    "ReceiverBackend",
    "ReceiverCallbacks",
    "SUPERVISOR_POLL_SECONDS",
    "VideoReceiverService",
]
