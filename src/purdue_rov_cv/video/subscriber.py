"""Production validated ZeroMQ SUB path for per-camera FrameIndex traffic."""

from __future__ import annotations

from collections.abc import Sequence
from threading import Event

import zmq
from purdue_rov.cv.v1 import frame_index_pb2

from purdue_rov_cv.runtime.envelope import ReceivedMultipartValidator
from purdue_rov_cv.runtime.json_logging import StructuredJsonLogger
from purdue_rov_cv.runtime.metrics import RuntimeMetrics
from purdue_rov_cv.runtime.shutdown import ShutdownToken

from .cache import CacheInsertResult
from .correlation import FrameCorrelator

FRAME_INDEX_RCVHWM = 5
FRAME_INDEX_RCVTIMEO_MS = 250
FRAME_INDEX_MAX_MESSAGE_BYTES = 4 * 1024 * 1024


def configure_frame_index_subscriber(socket: zmq.Socket[bytes], camera_id: str) -> None:
    socket.setsockopt(zmq.RCVHWM, FRAME_INDEX_RCVHWM)
    socket.setsockopt(zmq.RCVTIMEO, FRAME_INDEX_RCVTIMEO_MS)
    socket.setsockopt(zmq.LINGER, 0)
    socket.setsockopt(zmq.RECONNECT_IVL, 250)
    socket.setsockopt(zmq.RECONNECT_IVL_MAX, 2_000)
    socket.setsockopt(zmq.TCP_KEEPALIVE, 1)
    socket.setsockopt(zmq.TCP_KEEPALIVE_IDLE, 5)
    socket.setsockopt(zmq.TCP_KEEPALIVE_INTVL, 1)
    socket.setsockopt(zmq.TCP_KEEPALIVE_CNT, 3)
    socket.setsockopt(zmq.MAXMSGSIZE, FRAME_INDEX_MAX_MESSAGE_BYTES)
    socket.setsockopt(zmq.SUBSCRIBE, f"cv.frame_index.{camera_id}".encode())


class FrameIndexSubscriber:
    """Own its socket and context in its run thread."""

    def __init__(
        self,
        endpoint: str,
        camera_id: str,
        expected_rtp_payload_type: int,
        correlator: FrameCorrelator,
        *,
        metrics: RuntimeMetrics,
        shutdown: ShutdownToken,
        logger: StructuredJsonLogger | None = None,
        ready: Event | None = None,
    ) -> None:
        self.endpoint = endpoint
        self.camera_id = camera_id
        self.expected_rtp_payload_type = expected_rtp_payload_type
        self.correlator = correlator
        self.metrics = metrics
        self.shutdown = shutdown
        self.logger = logger
        self.ready = ready or Event()
        self.validator = ReceivedMultipartValidator(metrics)
        self.topic = f"cv.frame_index.{camera_id}"

    def process_frames(self, frames: Sequence[bytes]) -> bool:
        if len(frames) == 2 and isinstance(frames[0], bytes) and frames[0] != self.topic.encode():
            self.metrics.increment("invalid_messages")
            return False
        validation = self.validator.validate(frames)
        if not validation.valid:
            return False
        assert validation.topic is not None and validation.topic.topic == self.topic
        if not isinstance(validation.payload, frame_index_pb2.FrameIndex):
            self.metrics.increment("invalid_messages")
            return False
        if validation.payload.rtp_payload_type != self.expected_rtp_payload_type:
            self.metrics.increment("invalid_messages")
            return False
        try:
            result = self.correlator.submit_index(validation.payload)
        except (TypeError, ValueError) as error:
            self.metrics.increment("invalid_messages")
            if self.logger is not None:
                self.logger.log(
                    "WARNING",
                    "INVALID_FRAME_INDEX",
                    "validated FrameIndex was rejected by the correlation cache",
                    camera_id=self.camera_id,
                    context={"error": f"{type(error).__name__}: {error}"},
                )
            return False
        if result is CacheInsertResult.CONFLICT:
            self.metrics.increment("invalid_messages")
            if self.logger is not None:
                self.logger.log(
                    "WARNING",
                    "FRAME_INDEX_CONFLICT",
                    "conflicting canonical identities used the same RTP key",
                    camera_id=self.camera_id,
                    camera_session_id=validation.payload.camera_session_id,
                    frame_number=validation.payload.frame_number,
                    context={
                        "rtp_ssrc": validation.payload.rtp_ssrc,
                        "rtp_timestamp": validation.payload.rtp_timestamp,
                    },
                )
            return False
        return True

    def run(self) -> None:
        context = zmq.Context()
        socket: zmq.Socket[bytes] | None = None
        try:
            socket = context.socket(zmq.SUB)
            configure_frame_index_subscriber(socket, self.camera_id)
            socket.connect(self.endpoint)
            self.ready.set()
            while not self.shutdown.is_requested:
                try:
                    frames = socket.recv_multipart()
                except zmq.Again:
                    self.correlator.expire()
                    continue
                self.process_frames(frames)
                self.correlator.expire()
        finally:
            self.ready.set()
            if socket is not None:
                socket.close(linger=0)
            context.term()


__all__ = [
    "FRAME_INDEX_MAX_MESSAGE_BYTES",
    "FRAME_INDEX_RCVHWM",
    "FRAME_INDEX_RCVTIMEO_MS",
    "FrameIndexSubscriber",
    "configure_frame_index_subscriber",
]
