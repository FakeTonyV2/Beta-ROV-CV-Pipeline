"""Production recorder process: validated SUB, bounded handoff, MCAP lifecycle."""

from __future__ import annotations

import time
from collections.abc import Callable
from pathlib import Path
from threading import Event, Thread, get_ident

import zmq

from purdue_rov_cv.config.models import AppConfig, validate_identifier
from purdue_rov_cv.runtime.envelope import ReceivedMultipartValidator
from purdue_rov_cv.runtime.json_logging import StructuredJsonLogger
from purdue_rov_cv.runtime.metrics import RuntimeMetrics
from purdue_rov_cv.runtime.queues import QueueEvent, RecorderQueue
from purdue_rov_cv.runtime.shutdown import ShutdownCoordinator, ShutdownResult, ShutdownToken, install_signal_handlers
from purdue_rov_cv.runtime.state import ComponentState, ComponentStateMachine
from purdue_rov_cv.wire.errors import ErrorCode

from .disk import RUNTIME_MINIMUM_BYTES, DiskSpaceGuard
from .health import RecorderHealthPublisher
from .mcap_writer import McapSessionWriter, StructuredRecord, StructuredSink, StructuredWriterLoop

SUBSCRIBER_POLL_MS = 100
DISK_MONITOR_MIN_MS = 500
DISK_MONITOR_MAX_MS = 5_000

WriterFactory = Callable[[Path, int, str], StructuredSink]


def _default_writer_factory(path: Path, chunk_size_bytes: int, compression: str) -> StructuredSink:
    return McapSessionWriter(path, chunk_size_bytes=chunk_size_bytes, compression=compression)


def _configure_recorder_subscriber(socket: zmq.Socket[bytes], receive_hwm: int, max_message_bytes: int) -> None:
    socket.setsockopt(zmq.RCVHWM, receive_hwm)
    socket.setsockopt(zmq.RCVTIMEO, SUBSCRIBER_POLL_MS)
    socket.setsockopt(zmq.LINGER, 0)
    socket.setsockopt(zmq.MAXMSGSIZE, max_message_bytes)
    socket.setsockopt(zmq.RECONNECT_IVL, 250)
    socket.setsockopt(zmq.RECONNECT_IVL_MAX, 2_000)
    socket.setsockopt(zmq.TCP_KEEPALIVE, 1)
    socket.setsockopt(zmq.SUBSCRIBE, b"cv.")
    socket.setsockopt(zmq.SUBSCRIBE, b"system.")


class RecorderSubscriber:
    """Socket-confined production subscriber; only records fully validated input."""

    def __init__(
        self,
        endpoint: str,
        records: RecorderQueue[StructuredRecord],
        *,
        metrics: RuntimeMetrics,
        shutdown: ShutdownToken,
        context: zmq.Context,
        receive_hwm: int,
        max_message_bytes: int,
        unix_time_ns: Callable[[], int] = time.time_ns,
        ready: Event | None = None,
    ) -> None:
        self.endpoint = endpoint
        self.records = records
        self.metrics = metrics
        self.shutdown = shutdown
        self.context = context
        self.receive_hwm = receive_hwm
        self.max_message_bytes = max_message_bytes
        self.unix_time_ns = unix_time_ns
        self.ready = ready or Event()
        self.owner_thread_id: int | None = None

    def run(self) -> None:
        self.owner_thread_id = get_ident()
        validator = ReceivedMultipartValidator(self.metrics)
        socket: zmq.Socket[bytes] | None = None
        try:
            socket = self.context.socket(zmq.SUB)
            _configure_recorder_subscriber(socket, self.receive_hwm, self.max_message_bytes)
            socket.connect(self.endpoint)
            self.ready.set()
            while not self.shutdown.is_requested:
                try:
                    frames = socket.recv_multipart()
                except zmq.Again:
                    continue
                receive_time = self.unix_time_ns()
                result = validator.validate(frames)
                if not result.valid or result.envelope is None or result.topic is None or result.topic.topic is None:
                    continue
                self.records.offer(
                    StructuredRecord(
                        result.topic.topic,
                        result.envelope,
                        bytes(frames[1]),
                        receive_time,
                    )
                )
        finally:
            self.ready.set()
            if socket is not None:
                socket.close(linger=0)


class RecorderService:
    """Own the recorder context, worker threads, disk monitor, and session."""

    def __init__(
        self,
        *,
        recording_root: Path,
        session_label: str,
        subscriber_endpoint: str,
        publisher_endpoint: str,
        device_id: str,
        receive_hwm: int,
        max_message_bytes: int,
        health_interval_ms: int = 1_000,
        chunk_size_bytes: int = 1_048_576,
        compression: str = "zstd",
        metrics: RuntimeMetrics | None = None,
        logger: StructuredJsonLogger | None = None,
        disk_guard: DiskSpaceGuard | None = None,
        writer_factory: WriterFactory | None = None,
        install_signals: bool = False,
    ) -> None:
        validate_identifier(session_label)
        if not DISK_MONITOR_MIN_MS <= health_interval_ms <= DISK_MONITOR_MAX_MS:
            raise ValueError("disk/health interval must be between 500 and 5000 ms")
        self.recording_root = recording_root
        self.session_label = session_label
        self.session_directory = recording_root / session_label
        self.structured_path = self.session_directory / "structured.mcap"
        self.metrics = metrics or RuntimeMetrics()
        self.logger = logger
        self.disk_guard = disk_guard or DiskSpaceGuard()
        self.health_interval_seconds = health_interval_ms / 1000.0
        self.state_machine = ComponentStateMachine(observer=self._observe_state)
        self.metrics.set_metadata("state", ComponentState.STARTING.value)
        self.shutdown = ShutdownCoordinator(state_machine=self.state_machine)
        self.context = zmq.Context()
        self._accepting_done = Event()
        self.records: RecorderQueue[StructuredRecord] = RecorderQueue(
            event=self._queue_event,
            degrade=self._degrade,
            metrics=self.metrics,
        )
        self.subscriber = RecorderSubscriber(
            subscriber_endpoint,
            self.records,
            metrics=self.metrics,
            shutdown=self.shutdown.token,
            context=self.context,
            receive_hwm=receive_hwm,
            max_message_bytes=max_message_bytes,
        )
        self.health = RecorderHealthPublisher(
            publisher_endpoint,
            device_id=device_id,
            interval_ms=health_interval_ms,
            metrics=self.metrics,
            state_machine=self.state_machine,
            shutdown=self.shutdown.token,
            context=self.context,
        )
        self._chunk_size_bytes = chunk_size_bytes
        self._compression = compression
        self._writer_factory = writer_factory or _default_writer_factory
        self._subscriber_thread: Thread | None = None
        self._writer_thread: Thread | None = None
        self._health_thread: Thread | None = None
        self._writer_loop: StructuredWriterLoop | None = None
        self._initialized = False
        self._active = False
        self._worker_error: Exception | None = None
        self._abandoned_records = 0
        self._install_signals = install_signals
        self.shutdown.register("subscriber", self._stop_subscriber, order=10)
        self.shutdown.register("accepting-done", self._accepting_done.set, order=20)
        self.shutdown.register("writer", self._stop_writer, order=30)
        self.shutdown.register("health", self._stop_health, order=40)
        self.shutdown.register("zmq-context", self.context.term, order=50)

    @classmethod
    def from_config(
        cls,
        config: AppConfig,
        session_label: str,
        *,
        metrics: RuntimeMetrics | None = None,
        logger: StructuredJsonLogger | None = None,
        disk_guard: DiskSpaceGuard | None = None,
        writer_factory: WriterFactory | None = None,
        install_signals: bool = False,
    ) -> RecorderService:
        return cls(
            recording_root=config.recording.directory,
            session_label=session_label,
            subscriber_endpoint=config.messaging.broker.subscriber_endpoint,
            publisher_endpoint=config.messaging.broker.publisher_endpoint,
            device_id=config.device.device_id,
            receive_hwm=config.messaging.result_receive_hwm,
            max_message_bytes=config.messaging.max_message_bytes,
            health_interval_ms=config.diagnostics.publish_interval_ms,
            chunk_size_bytes=config.recording.structured.chunk_size_bytes,
            compression=config.recording.structured.compression,
            metrics=metrics,
            logger=logger,
            disk_guard=disk_guard,
            writer_factory=writer_factory,
            install_signals=install_signals,
        )

    @property
    def active(self) -> bool:
        return self._active

    def _observe_state(self, _result: object) -> None:
        self.metrics.set_metadata("state", self.state_machine.state.value)

    def _degrade(self, error_code: str) -> None:
        if self.state_machine.state in {ComponentState.READY, ComponentState.RUNNING}:
            self.state_machine.transition_to(ComponentState.DEGRADED)
        self.metrics.set_metadata("last_error_code", error_code)
        self.metrics.set_metadata("last_error_message", "recorder queue is full; newest record dropped")

    def _queue_event(self, event: QueueEvent) -> None:
        if self.logger is not None:
            self.logger.log(event.level, event.event_code, event.message, context=event.context)

    def _record_abandonment(self, count: int) -> None:
        self._abandoned_records = count
        message = f"bounded recorder shutdown abandoned {count} queued records"
        self.metrics.set_metadata("last_error_code", ErrorCode.INTERNAL_ERROR.value)
        self.metrics.set_metadata("last_error_message", message)
        if self.logger is not None:
            self.logger.log("ERROR", "RECORDER_SHUTDOWN_DROPPED", message, context={"record_count": count})

    def initialize(self) -> None:
        if self._initialized:
            return
        snapshot = self.disk_guard.allows_start(self.recording_root)
        self.metrics.set_gauge("disk_free_bytes", snapshot.free_bytes)
        # Video receivers may create their per-camera directories first. The
        # MCAP file's exclusive create remains the session collision guard.
        self.session_directory.mkdir(parents=True, exist_ok=True)
        sink: StructuredSink | None = None
        try:
            sink = self._writer_factory(
                self.structured_path,
                self._chunk_size_bytes,
                self._compression,
            )
            self._writer_loop = StructuredWriterLoop(
                self.records,
                sink,
                self.shutdown.token,
                accepting_done=self._accepting_done,
                on_abandoned=self._record_abandonment,
            )
            self._writer_thread = Thread(
                target=self._run_worker,
                args=("MCAP writer", self._writer_loop.run),
                name="recorder-mcap-writer",
                daemon=True,
            )
            self._subscriber_thread = Thread(
                target=self._run_worker,
                args=("recorder subscriber", self.subscriber.run),
                name="recorder-subscriber",
                daemon=True,
            )
            self._health_thread = Thread(
                target=self._run_worker,
                args=("recorder health publisher", self.health.run),
                name="recorder-health",
                daemon=True,
            )
            self._writer_thread.start()
            self._subscriber_thread.start()
            self._health_thread.start()
            if not self.subscriber.ready.wait(1.0):
                raise RuntimeError("recorder subscriber did not become ready")
            if not self.health.ready.wait(1.0):
                raise RuntimeError("recorder health publisher did not become ready")
            if self._worker_error is not None:
                raise RuntimeError("recorder worker failed during initialization") from self._worker_error
            if self._writer_thread is None or not self._writer_thread.is_alive():
                raise RuntimeError("MCAP writer exited during initialization")
            if self._subscriber_thread is None or not self._subscriber_thread.is_alive():
                raise RuntimeError("recorder subscriber exited during initialization")
            if self._health_thread is None or not self._health_thread.is_alive():
                raise RuntimeError("recorder health publisher exited during initialization")
        except Exception:
            self.shutdown.request("recorder initialization failed")
            self._accepting_done.set()
            if sink is not None and (self._writer_thread is None or not self._writer_thread.is_alive()):
                sink.finish()
            raise
        self._initialized = True
        self._active = True
        self.state_machine.transition_to(ComponentState.READY)
        self.state_machine.transition_to(ComponentState.RUNNING)

    def _run_worker(self, name: str, target: Callable[[], None]) -> None:
        try:
            target()
        except Exception as error:
            self._worker_error = error
            self.metrics.set_metadata("last_error_code", ErrorCode.INTERNAL_ERROR.value)
            self.metrics.set_metadata("last_error_message", f"{name}: {type(error).__name__}: {error}")
            if self.logger is not None:
                self.logger.log(
                    "ERROR",
                    ErrorCode.INTERNAL_ERROR.value,
                    f"{name} failed",
                    exception=error,
                )
            if self.state_machine.state not in {
                ComponentState.ERROR,
                ComponentState.STOPPING,
                ComponentState.STOPPED,
            }:
                self.state_machine.transition_to(ComponentState.ERROR)
            self.shutdown.request(f"{name} failed")

    def check_disk(self) -> bool:
        snapshot = self.disk_guard.runtime_is_low(self.session_directory)
        self.metrics.set_gauge("disk_free_bytes", snapshot.free_bytes)
        if snapshot.free_bytes >= RUNTIME_MINIMUM_BYTES:
            return True
        message = f"recording stopped: free space {snapshot.free_bytes} is below {RUNTIME_MINIMUM_BYTES} bytes"
        self.metrics.set_metadata("last_error_code", ErrorCode.DISK_SPACE_LOW.value)
        self.metrics.set_metadata("last_error_message", message)
        if self.state_machine.state in {ComponentState.READY, ComponentState.RUNNING}:
            self.state_machine.transition_to(ComponentState.DEGRADED)
        if self.logger is not None:
            self.logger.log("CRITICAL", ErrorCode.DISK_SPACE_LOW.value, message)
        self.health.publish_critical(ErrorCode.DISK_SPACE_LOW.value, message)
        self._active = False
        self.shutdown.request("disk space low")
        return False

    @staticmethod
    def _join(thread: Thread | None, name: str, timeout: float) -> None:
        if thread is None:
            return
        thread.join(timeout)
        if thread.is_alive():
            raise RuntimeError(f"{name} did not stop within {timeout:.1f} seconds")

    def _stop_subscriber(self) -> None:
        self._join(self._subscriber_thread, "recorder subscriber", 1.0)

    def _stop_writer(self) -> None:
        self._join(self._writer_thread, "MCAP writer", 3.5)

    def _stop_health(self) -> None:
        self._join(self._health_thread, "recorder health publisher", 0.4)

    def close(self) -> ShutdownResult:
        self._active = False
        if self.state_machine.state not in {ComponentState.STOPPING, ComponentState.STOPPED}:
            self.shutdown.request("recorder close")
        return self.shutdown.run(timeout_seconds=5.0)

    def run(self) -> None:
        if self._install_signals:
            install_signal_handlers(self.shutdown)
        try:
            self.initialize()
            while not self.shutdown.token.is_requested:
                if self._worker_error is not None:
                    raise RuntimeError("recorder worker failed") from self._worker_error
                if not self.check_disk():
                    break
                self.shutdown.token.wait(self.health_interval_seconds)
        finally:
            result = self.close()
        if result.timed_out:
            raise RuntimeError("recorder shutdown exceeded five seconds")
        if result.failures:
            raise RuntimeError("recorder cleanup failed: " + "; ".join(x.message for x in result.failures))
        if self._worker_error is not None:
            raise RuntimeError("recorder worker failed") from self._worker_error
        if self._abandoned_records:
            raise RuntimeError(f"recorder shutdown abandoned {self._abandoned_records} queued records")


__all__ = [
    "DISK_MONITOR_MAX_MS",
    "DISK_MONITOR_MIN_MS",
    "RecorderService",
    "RecorderSubscriber",
    "SUBSCRIBER_POLL_MS",
    "WriterFactory",
]
