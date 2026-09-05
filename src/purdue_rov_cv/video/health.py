"""Periodic Phase 3 health publication for a surface video receiver."""

from __future__ import annotations

import time
from threading import Event

import zmq
from purdue_rov.cv.v1 import diagnostics_pb2

from purdue_rov_cv.module_runner.publisher import configure_result_publisher
from purdue_rov_cv.runtime.envelope import EnvelopeBuilder, EnvelopeBuildError
from purdue_rov_cv.runtime.metrics import RuntimeMetrics
from purdue_rov_cv.runtime.publisher import PublisherSequence
from purdue_rov_cv.runtime.shutdown import ShutdownToken
from purdue_rov_cv.runtime.state import ComponentStateMachine, to_wire_component_state


class VideoHealthPublisher:
    def __init__(
        self,
        endpoint: str,
        source_id: str,
        camera_id: str,
        *,
        interval_ms: int,
        metrics: RuntimeMetrics,
        state_machine: ComponentStateMachine,
        shutdown: ShutdownToken,
        sequence: PublisherSequence | None = None,
        ready: Event | None = None,
    ) -> None:
        self.endpoint = endpoint
        self.source_id = source_id
        self.camera_id = camera_id
        self.interval_seconds = interval_ms / 1_000.0
        self.metrics = metrics
        self.state_machine = state_machine
        self.shutdown = shutdown
        self.sequence = sequence or PublisherSequence()
        self.ready = ready or Event()

    def health(self) -> diagnostics_pb2.DiagnosticStatus:
        values = self.metrics.snapshot().values
        health = diagnostics_pb2.DiagnosticStatus(
            source_id=self.source_id,
            report_time_unix_ns=time.time_ns(),
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
            video=diagnostics_pb2.VideoMetrics(
                rtp_packets_received=int(values["rtp_packets_received"]),
                rtp_packets_lost=int(values["rtp_packets_lost"]),
                decoded_frames=int(values["decoded_frames"]),
                frame_index_hits=int(values["frame_index_hits"]),
                frame_index_misses=int(values["frame_index_misses"]),
                stream_restarts=int(values["stream_restarts"]),
                last_frame_age_ms=max(0, int(values["last_frame_age_ms"])),
            ),
        )
        if isinstance(values["last_error_code"], str):
            health.last_error_code = values["last_error_code"]
        if isinstance(values["last_error_message"], str):
            health.last_error_message = values["last_error_message"]
        return health

    def run(self) -> None:
        context = zmq.Context()
        socket: zmq.Socket[bytes] | None = None
        try:
            socket = context.socket(zmq.PUB)
            configure_result_publisher(socket)
            socket.connect(self.endpoint)
            builder = EnvelopeBuilder(self.sequence)
            self.ready.set()
            while not self.shutdown.is_requested:
                try:
                    built = builder.build(
                        topic=f"cv.health.{self.source_id}",
                        payload_type="diagnostic_status_v1",
                        payload=self.health(),
                        task_id="video_receiver",
                        source_id=self.source_id,
                        camera_id=self.camera_id,
                    )
                    socket.send_multipart(list(built.frames), flags=zmq.DONTWAIT)
                    self.metrics.increment("messages_sent")
                except (EnvelopeBuildError, zmq.Again):
                    self.metrics.increment("zmq_send_dropped")
                self.shutdown.wait(self.interval_seconds)
        finally:
            self.ready.set()
            if socket is not None:
                socket.close(linger=0)
            context.term()


__all__ = ["VideoHealthPublisher"]
