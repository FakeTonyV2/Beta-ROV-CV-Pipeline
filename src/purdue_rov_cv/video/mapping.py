"""Canonical bounded source-frame to RTP identity mapping."""

from __future__ import annotations

import time
from collections import OrderedDict, deque
from collections.abc import Callable
from dataclasses import dataclass
from threading import RLock

from purdue_rov.cv.v1 import frame_index_pb2

SENDER_MAPPING_CAPACITY = 256


@dataclass(frozen=True, slots=True)
class SourceFrameIdentity:
    frame_number: int
    capture_time_unix_ns: int
    capture_monotonic_ns: int


class RtpFrameIndexMapper:
    """Bridge source PTS through an encoder to deduplicated RTP identity."""

    def __init__(
        self,
        camera_id: str,
        camera_session_id: bytes,
        *,
        capacity: int = SENDER_MAPPING_CAPACITY,
        time_ns: Callable[[], int] = time.time_ns,
        monotonic_ns: Callable[[], int] = time.monotonic_ns,
    ) -> None:
        if not camera_id:
            raise ValueError("camera_id is required")
        if len(camera_session_id) != 16:
            raise ValueError("camera_session_id must be a 16-byte UUID")
        if capacity <= 0:
            raise ValueError("mapping capacity must be positive")
        self.camera_id = camera_id
        self.camera_session_id = camera_session_id
        self.capacity = capacity
        self._time_ns = time_ns
        self._monotonic_ns = monotonic_ns
        self._sources: OrderedDict[int, SourceFrameIdentity] = OrderedDict()
        self._encoder_inputs: deque[SourceFrameIdentity] = deque(maxlen=capacity)
        self._encoded: OrderedDict[int, SourceFrameIdentity] = OrderedDict()
        self._published: OrderedDict[tuple[int, int], None] = OrderedDict()
        self._next_frame_number = 0
        self._lock = RLock()

    @property
    def encoded_mapping_count(self) -> int:
        with self._lock:
            return len(self._encoded)

    def observe_source(self, presentation_timestamp_ns: int) -> SourceFrameIdentity:
        with self._lock:
            identity = SourceFrameIdentity(
                self._next_frame_number,
                self._time_ns(),
                self._monotonic_ns(),
            )
            self._next_frame_number += 1
            self._sources[presentation_timestamp_ns] = identity
            self._sources.move_to_end(presentation_timestamp_ns)
            while len(self._sources) > self.capacity:
                self._sources.popitem(last=False)
            return identity

    def source_for_pts(self, presentation_timestamp_ns: int) -> SourceFrameIdentity | None:
        with self._lock:
            return self._sources.get(presentation_timestamp_ns)

    def observe_encoder_input(self, presentation_timestamp_ns: int) -> bool:
        with self._lock:
            source = self._sources.get(presentation_timestamp_ns)
            if source is None:
                return False
            self._encoder_inputs.append(source)
            return True

    def observe_encoded_output(self, presentation_timestamp_ns: int) -> bool:
        with self._lock:
            if not self._encoder_inputs:
                return False
            source = self._encoder_inputs.popleft()
            self._encoded[presentation_timestamp_ns] = source
            self._encoded.move_to_end(presentation_timestamp_ns)
            while len(self._encoded) > self.capacity:
                self._encoded.popitem(last=False)
            return True

    def frame_index_for_packet(
        self,
        presentation_timestamp_ns: int,
        rtp_ssrc: int,
        rtp_timestamp: int,
        rtp_payload_type: int,
    ) -> frame_index_pb2.FrameIndex | None:
        key = (rtp_ssrc & 0xFFFFFFFF, rtp_timestamp & 0xFFFFFFFF)
        with self._lock:
            if key in self._published:
                return None
            source = self._encoded.get(presentation_timestamp_ns)
            if source is None:
                return None
            self._published[key] = None
            while len(self._published) > self.capacity:
                self._published.popitem(last=False)
            return frame_index_pb2.FrameIndex(
                camera_id=self.camera_id,
                camera_session_id=self.camera_session_id,
                frame_number=source.frame_number,
                capture_time_unix_ns=source.capture_time_unix_ns,
                capture_monotonic_ns=source.capture_monotonic_ns,
                rtp_timestamp=key[1],
                rtp_ssrc=key[0],
                rtp_payload_type=rtp_payload_type,
            )

    def clear(self) -> None:
        with self._lock:
            self._sources.clear()
            self._encoder_inputs.clear()
            self._encoded.clear()
            self._published.clear()


__all__ = ["RtpFrameIndexMapper", "SENDER_MAPPING_CAPACITY", "SourceFrameIdentity"]
