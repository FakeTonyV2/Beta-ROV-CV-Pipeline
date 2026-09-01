"""Socket-confined nonblocking PUB result and health publication."""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from threading import Event

import zmq
from google.protobuf.message import Message
from purdue_rov.cv.v1 import diagnostics_pb2

from purdue_rov_cv.modules.base import Frame
from purdue_rov_cv.runtime.envelope import EnvelopeBuilder, EnvelopeBuildError
from purdue_rov_cv.runtime.json_logging import StructuredJsonLogger
from purdue_rov_cv.runtime.metrics import RuntimeMetrics
from purdue_rov_cv.runtime.publisher import PublisherSequence
from purdue_rov_cv.runtime.queues import CvResultQueue, ReceiveStatus
from purdue_rov_cv.runtime.shutdown import ShutdownToken
from purdue_rov_cv.runtime.state import ComponentStateMachine, to_wire_component_state
from purdue_rov_cv.wire.errors import ErrorCode

RESULT_PUBLISHER_HWM = 5
RESULT_PUBLISHER_MAX_MESSAGE_BYTES = 4 * 1024 * 1024


def configure_result_publisher(socket: zmq.Socket[bytes]) -> None:
    socket.setsockopt(zmq.SNDHWM, RESULT_PUBLISHER_HWM)
    socket.setsockopt(zmq.SNDTIMEO, 0)
    socket.setsockopt(zmq.LINGER, 0)
    socket.setsockopt(zmq.IMMEDIATE, 1)
    socket.setsockopt(zmq.RECONNECT_IVL, 250)
    socket.setsockopt(zmq.RECONNECT_IVL_MAX, 2_000)
    socket.setsockopt(zmq.TCP_KEEPALIVE, 1)
    socket.setsockopt(zmq.TCP_KEEPALIVE_IDLE, 5)
    socket.setsockopt(zmq.TCP_KEEPALIVE_INTVL, 1)
    socket.setsockopt(zmq.TCP_KEEPALIVE_CNT, 3)
    socket.setsockopt(zmq.MAXMSGSIZE, RESULT_PUBLISHER_MAX_MESSAGE_BYTES)


@dataclass(frozen=True)
class PublicationItem:
    payload: Message
    frame: Frame


class ResultPublisher:
    """Create and own the runner PUB socket in one dedicated thread."""

    def __init__(
        self,
        endpoint: str,
        *,
        topic: str,
        payload_type: str,
        task_id: str,
        module_id: str,
        device_id: str,
        health_interval_ms: int,
        queue: CvResultQueue[PublicationItem],
        metrics: RuntimeMetrics,
        state_machine: ComponentStateMachine,
        shutdown: ShutdownToken,
        context: zmq.Context,
        sequence: PublisherSequence | None = None,
        ready: Event | None = None,
        health_interval_ms_getter: Callable[[], int] | None = None,
        logger: StructuredJsonLogger | None = None,
    ) -> None:
        self.endpoint = endpoint
        self.topic = topic
        self.payload_type = payload_type
        self.task_id = task_id
        self.module_id = module_id
        self.device_id = device_id
        self.health_interval_seconds = health_interval_ms / 1_000.0
        self.queue = queue
        self.metrics = metrics
        self.state_machine = state_machine
        self.shutdown = shutdown
        self.context = context
        self.sequence = sequence or PublisherSequence()
        self.ready = ready or Event()
        self._health_interval_ms_getter = health_interval_ms_getter or (lambda: health_interval_ms)
        self.logger = logger

    @property
    def publisher_session_id(self) -> bytes:
        return self.sequence.session_id

    def _health(self) -> diagnostics_pb2.DiagnosticStatus:
        values = self.metrics.snapshot().values
        health = diagnostics_pb2.DiagnosticStatus(
            source_id=self.module_id,
            report_time_unix_ns=time.time_ns(),
            uptime_seconds=float(values["uptime_seconds"]),
            state=to_wire_component_state(self.state_machine.state),
            module=diagnostics_pb2.ModuleMetrics(
                frames_read=int(values["frames_read"]),
                frames_processed=int(values["frames_processed"]),
                frames_dropped_before_processing=int(values["frames_dropped_before_processing"]),
                processing_exceptions=int(values["processing_exceptions"]),
                processing_deadline_misses=int(values["processing_deadline_misses"]),
                average_processing_ms=float(values["average_processing_ms"]),
                p95_processing_ms=float(values["p95_processing_ms"]),
                results_published=int(values["results_published"]),
                results_dropped_local_queue=int(values["results_dropped_local_queue"]),
                zmq_send_dropped=int(values["zmq_send_dropped"]),
            ),
        )
        last_error_code = values["last_error_code"]
        last_error_message = values["last_error_message"]
        if isinstance(last_error_code, str):
            health.last_error_code = last_error_code
        if isinstance(last_error_message, str):
            health.last_error_message = last_error_message
        return health

    def _record_invalid_envelope(self, error: EnvelopeBuildError, item: PublicationItem | None = None) -> None:
        message = str(error)
        self.metrics.increment("invalid_messages")
        self.metrics.set_metadata("last_error_code", ErrorCode.INVALID_ENVELOPE.value)
        self.metrics.set_metadata("last_error_message", message)
        if self.logger is not None:
            self.logger.log(
                "ERROR",
                ErrorCode.INVALID_ENVELOPE.value,
                "module output failed envelope validation; publication dropped",
                camera_id=None if item is None else item.frame.camera_id,
                camera_session_id=None if item is None else item.frame.camera_session_id,
                frame_number=None if item is None else item.frame.frame_number,
                context={"task_id": self.task_id, "module_id": self.module_id, "validation": message},
            )

    def _send(self, socket: zmq.Socket[bytes], builder: EnvelopeBuilder, item: PublicationItem) -> None:
        built = builder.build(
            topic=self.topic,
            payload_type=self.payload_type,
            payload=item.payload,
            task_id=self.task_id,
            source_id=self.module_id,
            camera_id=item.frame.camera_id,
            camera_session_id=item.frame.camera_session_id,
            frame_number=item.frame.frame_number,
            capture_time_unix_ns=item.frame.capture_time_unix_ns,
        )
        try:
            socket.send_multipart(list(built.frames), flags=zmq.DONTWAIT)
        except zmq.Again:
            self.metrics.increment("zmq_send_dropped")
            return
        self.metrics.increment("results_published")
        self.metrics.increment("messages_sent")

    def _send_health(self, socket: zmq.Socket[bytes], builder: EnvelopeBuilder) -> None:
        built = builder.build(
            topic=f"cv.health.{self.module_id}",
            payload_type="diagnostic_status_v1",
            payload=self._health(),
            task_id=self.task_id,
            source_id=self.module_id,
        )
        try:
            socket.send_multipart(list(built.frames), flags=zmq.DONTWAIT)
        except zmq.Again:
            self.metrics.increment("zmq_send_dropped")
            return
        self.metrics.increment("messages_sent")

    def run(self) -> None:
        socket: zmq.Socket[bytes] = self.context.socket(zmq.PUB)
        configure_result_publisher(socket)
        socket.connect(self.endpoint)
        builder = EnvelopeBuilder(self.sequence)
        last_health = float("-inf")
        self.ready.set()
        try:
            while not self.shutdown.is_requested:
                received = self.queue.receive(timeout_seconds=0.100, shutdown=self.shutdown)
                if received.status is ReceiveStatus.ITEM:
                    assert received.item is not None
                    try:
                        self._send(socket, builder, received.item)
                    except EnvelopeBuildError as error:
                        self._record_invalid_envelope(error, received.item)
                now = time.monotonic()
                health_interval_seconds = self._health_interval_ms_getter() / 1_000.0
                if now - last_health >= health_interval_seconds:
                    try:
                        self._send_health(socket, builder)
                    except EnvelopeBuildError as error:
                        self._record_invalid_envelope(error)
                    last_health = now
        finally:
            socket.close(linger=0)


__all__ = [
    "PublicationItem",
    "RESULT_PUBLISHER_HWM",
    "RESULT_PUBLISHER_MAX_MESSAGE_BYTES",
    "ResultPublisher",
    "configure_result_publisher",
]
