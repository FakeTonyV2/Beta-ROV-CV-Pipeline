"""Recorder-owned health and critical system-event publisher."""

from __future__ import annotations

import queue
import time
from threading import Event

import psutil
import zmq
from google.protobuf.message import Message
from purdue_rov.cv.v1 import diagnostics_pb2, event_pb2

from purdue_rov_cv.module_runner.publisher import configure_result_publisher
from purdue_rov_cv.runtime.envelope import EnvelopeBuilder, EnvelopeBuildError
from purdue_rov_cv.runtime.metrics import RuntimeMetrics
from purdue_rov_cv.runtime.publisher import PublisherSequence
from purdue_rov_cv.runtime.shutdown import ShutdownToken
from purdue_rov_cv.runtime.state import ComponentStateMachine, to_wire_component_state

CRITICAL_SHUTDOWN_LINGER_MS = 250


class RecorderHealthPublisher:
    """Own exactly one PUB socket and keep disk/recorder failures observable."""

    def __init__(
        self,
        endpoint: str,
        *,
        device_id: str,
        interval_ms: int,
        metrics: RuntimeMetrics,
        state_machine: ComponentStateMachine,
        shutdown: ShutdownToken,
        context: zmq.Context,
        ready: Event | None = None,
    ) -> None:
        self.endpoint = endpoint
        self.device_id = device_id
        self.interval_seconds = interval_ms / 1000.0
        self.metrics = metrics
        self.state_machine = state_machine
        self.shutdown = shutdown
        self.context = context
        self.ready = ready or Event()
        self._events: queue.Queue[event_pb2.SystemEvent] = queue.Queue(maxsize=64)

    def publish_critical(self, error_code: str, message: str) -> None:
        value = event_pb2.SystemEvent(
            event_type="disk_space_low",
            source_id="recorder",
            event_time_unix_ns=time.time_ns(),
            error_code=error_code,
            message=message,
        )
        try:
            self._events.put_nowait(value)
        except queue.Full:
            # The low-disk transition is idempotent and produces one event.
            # Retaining the first critical event is safer than replacing it.
            self.metrics.increment("priority_messages_dropped")

    def _update_process_metrics(self) -> None:
        process = psutil.Process()
        self.metrics.set_gauge("process_cpu_percent", process.cpu_percent())
        self.metrics.set_gauge("resident_memory_bytes", process.memory_info().rss)
        self.metrics.set_gauge("thread_count", process.num_threads())

    def health(self) -> diagnostics_pb2.DiagnosticStatus:
        self._update_process_metrics()
        values = self.metrics.snapshot().values
        health = diagnostics_pb2.DiagnosticStatus(
            source_id="recorder",
            report_time_unix_ns=time.time_ns(),
            process_cpu_percent=float(values["process_cpu_percent"]),
            resident_memory_bytes=int(values["resident_memory_bytes"]),
            thread_count=int(values["thread_count"]),
            uptime_seconds=float(values["uptime_seconds"]),
            state=to_wire_component_state(self.state_machine.state),
            messaging=diagnostics_pb2.MessagingMetrics(
                messages_sent=int(values["messages_sent"]),
                messages_received=int(values["messages_received"]),
                invalid_messages=int(values["invalid_messages"]),
                unknown_payload_types=int(values["unknown_payload_types"]),
                observed_sequence_gaps=int(values["observed_sequence_gaps"]),
                reconnect_count=int(values["reconnect_count"]),
            ),
            system=diagnostics_pb2.SystemMetrics(disk_free_bytes=int(values["disk_free_bytes"])),
        )
        if isinstance(values["last_error_code"], str):
            health.last_error_code = values["last_error_code"]
        if isinstance(values["last_error_message"], str):
            health.last_error_message = values["last_error_message"]
        return health

    def _send(
        self, socket: zmq.Socket[bytes], builder: EnvelopeBuilder, topic: str, payload_type: str, payload: Message
    ) -> None:
        try:
            built = builder.build(
                topic=topic,
                payload_type=payload_type,
                payload=payload,
                task_id="recorder",
                source_id="recorder",
            )
            socket.send_multipart(list(built.frames), flags=zmq.DONTWAIT)
            self.metrics.increment("messages_sent")
        except (EnvelopeBuildError, zmq.Again):
            self.metrics.increment("zmq_send_dropped")

    def _send_pending_events(self, socket: zmq.Socket[bytes], builder: EnvelopeBuilder) -> None:
        while True:
            try:
                event = self._events.get_nowait()
            except queue.Empty:
                return
            self._send(socket, builder, "system.event.disk_space_low", "system_event_v1", event)

    def run(self) -> None:
        socket: zmq.Socket[bytes] | None = None
        try:
            socket = self.context.socket(zmq.PUB)
            configure_result_publisher(socket)
            socket.connect(self.endpoint)
            builder = EnvelopeBuilder(PublisherSequence())
            next_health = time.monotonic()
            self.ready.set()
            while True:
                self._send_pending_events(socket, builder)
                now = time.monotonic()
                if self.shutdown.is_requested:
                    # publish_critical() enqueues before requesting shutdown, but
                    # that can race an iteration which already observed an empty
                    # queue. Re-drain at the shutdown boundary before final health.
                    self._send_pending_events(socket, builder)
                    self._send(
                        socket,
                        builder,
                        "cv.health.recorder",
                        "diagnostic_status_v1",
                        self.health(),
                    )
                    break
                if now >= next_health:
                    self._send(
                        socket,
                        builder,
                        "cv.health.recorder",
                        "diagnostic_status_v1",
                        self.health(),
                    )
                    next_health = now + self.interval_seconds
                self.shutdown.wait(min(0.050, max(0.0, next_health - now)))
        finally:
            self.ready.set()
            if socket is not None:
                # A critical event can be the message that requests shutdown.
                # Give its already-queued ZeroMQ frame a bounded opportunity to
                # reach the broker instead of discarding it at socket close.
                socket.close(linger=CRITICAL_SHUTDOWN_LINGER_MS)


__all__ = ["RecorderHealthPublisher"]
