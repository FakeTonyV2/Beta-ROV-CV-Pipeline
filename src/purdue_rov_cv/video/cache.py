"""Bounded per-receiver FrameIndex cache keyed by RTP stream identity."""

from __future__ import annotations

import time
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from threading import RLock

from purdue_rov.cv.v1 import frame_index_pb2

from .models import ResolvedFrameIdentity

FRAME_INDEX_CAPACITY = 512
FRAME_INDEX_TTL_NS = 2_000_000_000
RTP_CLOCK_RATE = 90_000
APPROXIMATE_WINDOW_TICKS = RTP_CLOCK_RATE // 20

RtpKey = tuple[int, int]


class CacheInsertResult(StrEnum):
    INSERTED = "INSERTED"
    DUPLICATE = "DUPLICATE"
    CONFLICT = "CONFLICT"


@dataclass(frozen=True, slots=True)
class FrameIndexEntry:
    rtp_ssrc: int
    rtp_timestamp: int
    camera_session_id: bytes
    frame_number: int
    capture_time_unix_ns: int
    expiration_monotonic_ns: int
    conflicted: bool = False

    @property
    def identity_tuple(self) -> tuple[bytes, int]:
        return self.camera_session_id, self.frame_number

    def resolved(self, camera_id: str) -> ResolvedFrameIdentity:
        return ResolvedFrameIdentity(
            camera_id,
            self.camera_session_id,
            self.frame_number,
            self.capture_time_unix_ns,
        )


def _uint32_distance(left: int, right: int) -> int:
    """Return the wrap-aware absolute distance between two RTP timestamps."""
    forward = (left - right) & 0xFFFFFFFF
    backward = (right - left) & 0xFFFFFFFF
    return min(forward, backward)


class FrameIndexCache:
    """One camera's deterministic TTL/capacity cache.

    A conflicting identity poisons its key until expiry. This avoids mapping a
    restarted stream to either of two impossible identities.
    """

    def __init__(
        self,
        camera_id: str,
        *,
        capacity: int = FRAME_INDEX_CAPACITY,
        ttl_ns: int = FRAME_INDEX_TTL_NS,
        monotonic_ns: Callable[[], int] = time.monotonic_ns,
    ) -> None:
        if capacity <= 0 or ttl_ns <= 0:
            raise ValueError("cache capacity and TTL must be positive")
        self.camera_id = camera_id
        self.capacity = capacity
        self.ttl_ns = ttl_ns
        self._monotonic_ns = monotonic_ns
        self._entries: OrderedDict[RtpKey, FrameIndexEntry] = OrderedDict()
        self._lock = RLock()

    def __len__(self) -> int:
        with self._lock:
            return len(self._entries)

    def clear(self) -> None:
        """Discard every mapping at a receiver stream boundary."""
        with self._lock:
            self._entries.clear()

    def expire(self, now_ns: int | None = None) -> int:
        now = self._monotonic_ns() if now_ns is None else now_ns
        removed = 0
        with self._lock:
            for key, entry in tuple(self._entries.items()):
                if entry.expiration_monotonic_ns <= now:
                    del self._entries[key]
                    removed += 1
        return removed

    def insert(self, value: frame_index_pb2.FrameIndex, now_ns: int | None = None) -> CacheInsertResult:
        if value.camera_id != self.camera_id:
            raise ValueError("FrameIndex camera_id does not match this receiver")
        if len(value.camera_session_id) != 16:
            raise ValueError("camera_session_id must be a 16-byte UUID")
        now = self._monotonic_ns() if now_ns is None else now_ns
        key = (int(value.rtp_ssrc), int(value.rtp_timestamp))
        candidate = FrameIndexEntry(
            *key,
            bytes(value.camera_session_id),
            int(value.frame_number),
            int(value.capture_time_unix_ns),
            now + self.ttl_ns,
        )
        with self._lock:
            self.expire(now)
            previous = self._entries.get(key)
            if previous is not None:
                if previous.conflicted or previous.identity_tuple != candidate.identity_tuple:
                    self._entries[key] = FrameIndexEntry(
                        previous.rtp_ssrc,
                        previous.rtp_timestamp,
                        previous.camera_session_id,
                        previous.frame_number,
                        previous.capture_time_unix_ns,
                        max(previous.expiration_monotonic_ns, candidate.expiration_monotonic_ns),
                        True,
                    )
                    self._entries.move_to_end(key)
                    return CacheInsertResult.CONFLICT
                return CacheInsertResult.DUPLICATE
            self._entries[key] = candidate
            while len(self._entries) > self.capacity:
                self._entries.popitem(last=False)
            return CacheInsertResult.INSERTED

    def exact(self, rtp_ssrc: int, rtp_timestamp: int, now_ns: int | None = None) -> FrameIndexEntry | None:
        now = self._monotonic_ns() if now_ns is None else now_ns
        key = (rtp_ssrc & 0xFFFFFFFF, rtp_timestamp & 0xFFFFFFFF)
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                return None
            if entry.expiration_monotonic_ns <= now:
                del self._entries[key]
                return None
            return None if entry.conflicted else entry

    def consume_exact(
        self,
        rtp_ssrc: int,
        rtp_timestamp: int,
        now_ns: int | None = None,
    ) -> FrameIndexEntry | None:
        """Return and remove one valid exact mapping.

        An RTP frame identity is one-shot. Keeping a resolved entry available
        would let a duplicate decoded frame or a restarted stream reuse stale
        canonical identity.
        """
        now = self._monotonic_ns() if now_ns is None else now_ns
        key = (rtp_ssrc & 0xFFFFFFFF, rtp_timestamp & 0xFFFFFFFF)
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                return None
            if entry.expiration_monotonic_ns <= now:
                del self._entries[key]
                return None
            if entry.conflicted:
                return None
            del self._entries[key]
            return entry

    def nearest(
        self,
        rtp_ssrc: int,
        rtp_timestamp: int,
        *,
        maximum_ticks: int = APPROXIMATE_WINDOW_TICKS,
        now_ns: int | None = None,
    ) -> FrameIndexEntry | None:
        now = self._monotonic_ns() if now_ns is None else now_ns
        self.expire(now)
        best: tuple[int, FrameIndexEntry] | None = None
        with self._lock:
            for entry in self._entries.values():
                if entry.conflicted or entry.rtp_ssrc != (rtp_ssrc & 0xFFFFFFFF):
                    continue
                distance = _uint32_distance(entry.rtp_timestamp, rtp_timestamp & 0xFFFFFFFF)
                if distance <= maximum_ticks and (best is None or distance < best[0]):
                    best = distance, entry
        return None if best is None else best[1]


__all__ = [
    "APPROXIMATE_WINDOW_TICKS",
    "CacheInsertResult",
    "FRAME_INDEX_CAPACITY",
    "FRAME_INDEX_TTL_NS",
    "FrameIndexCache",
    "FrameIndexEntry",
    "RTP_CLOCK_RATE",
    "RtpKey",
]
