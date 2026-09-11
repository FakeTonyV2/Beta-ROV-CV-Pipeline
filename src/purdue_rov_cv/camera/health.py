"""Periodic canonical health publication for a camera process."""

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


class CameraHealthPublisher:
    def __init__(
        self,
        endpoint: str,
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
            source_id=self.camera_id,
            report_time_unix_ns=time.time_ns(),
            uptime_seconds=float(values["uptime_seconds"]),
            state=to_wire_component_state(self.state_machine.state),
            camera=diagnostics_pb2.CameraMetrics(
                frames_received=int(values["frames_received"]),
                frames_per_second=float(values["frames_per_second"]),
                frame_timeouts=int(values["frame_timeouts"]),
                pipeline_restarts=int(values["pipeline_restarts"]),
                shared_memory_write_count=int(values["shared_memory_write_count"]),
                current_width=int(values["current_width"]),
                current_height=int(values["current_height"]),
                current_pixel_format=str(values["current_pixel_format"]),
                usb_device_present=bool(values["usb_device_present"]),
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
                        topic=f"cv.health.{self.camera_id}",
                        payload_type="diagnostic_status_v1",
                        payload=self.health(),
                        task_id="camera",
                        source_id=self.camera_id,
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


__all__ = ["CameraHealthPublisher"]
