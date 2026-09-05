"""Asynchronous decoded-frame to FrameIndex correlation."""

from __future__ import annotations

import time
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass
from threading import RLock

from purdue_rov.cv.v1 import frame_index_pb2

from purdue_rov_cv.runtime.metrics import RuntimeMetrics

from .cache import CacheInsertResult, FrameIndexCache
from .models import CorrelationQuality, DecodedVideoFrame, FrameCorrelation, ReceivedVideoFrame

CORRELATION_WAIT_NS = 100_000_000
PENDING_FRAME_CAPACITY = 64


@dataclass(frozen=True, slots=True)
class _PendingFrame:
    frame: DecodedVideoFrame
    deadline_monotonic_ns: int


class FrameCorrelator:
    """Resolve exact mappings without blocking a GStreamer streaming thread."""

    def __init__(
        self,
        cache: FrameIndexCache,
        deliver: Callable[[ReceivedVideoFrame], None],
        *,
        metrics: RuntimeMetrics,
        approximate_debug: bool = False,
        wait_ns: int = CORRELATION_WAIT_NS,
        pending_capacity: int = PENDING_FRAME_CAPACITY,
        monotonic_ns: Callable[[], int] = time.monotonic_ns,
    ) -> None:
        if wait_ns < 0 or pending_capacity <= 0:
            raise ValueError("correlation wait must be non-negative and pending capacity positive")
        self.cache = cache
        self.metrics = metrics
        self.approximate_debug = approximate_debug
        self.wait_ns = wait_ns
        self.pending_capacity = pending_capacity
        self._monotonic_ns = monotonic_ns
        self._deliver = deliver
        self._pending: OrderedDict[int, _PendingFrame] = OrderedDict()
        self._next_id = 0
        self._lock = RLock()
        self._closed = False

    @property
    def pending_count(self) -> int:
        with self._lock:
            return len(self._pending)

    def _exact_result(self, frame: DecodedVideoFrame, now_ns: int) -> ReceivedVideoFrame | None:
        entry = self.cache.consume_exact(frame.rtp_ssrc, frame.rtp_timestamp, now_ns)
        if entry is None:
            return None
        return ReceivedVideoFrame(
            frame,
            FrameCorrelation(CorrelationQuality.EXACT, entry.resolved(self.cache.camera_id)),
        )

    def _unmatched_or_approximate(self, frame: DecodedVideoFrame, now_ns: int) -> ReceivedVideoFrame:
        if self.approximate_debug:
            entry = self.cache.nearest(frame.rtp_ssrc, frame.rtp_timestamp, now_ns=now_ns)
            if entry is not None:
                return ReceivedVideoFrame(
                    frame,
                    FrameCorrelation(CorrelationQuality.APPROXIMATE, entry.resolved(self.cache.camera_id)),
                )
        return ReceivedVideoFrame(frame, FrameCorrelation(CorrelationQuality.UNMATCHED))

    def _publish(self, result: ReceivedVideoFrame) -> None:
        if result.correlation.quality is CorrelationQuality.EXACT:
            self.metrics.increment("frame_index_hits")
        elif result.correlation.quality is CorrelationQuality.UNMATCHED:
            self.metrics.increment("frame_index_misses")
        self._deliver(result)

    def submit_frame(self, frame: DecodedVideoFrame) -> None:
        now = self._monotonic_ns()
        deliveries: list[ReceivedVideoFrame] = []
        with self._lock:
            if self._closed:
                return
            exact = self._exact_result(frame, now)
            if exact is not None:
                deliveries.append(exact)
            elif self.wait_ns == 0:
                deliveries.append(self._unmatched_or_approximate(frame, now))
            else:
                pending_id = self._next_id
                self._next_id += 1
                self._pending[pending_id] = _PendingFrame(frame, now + self.wait_ns)
                while len(self._pending) > self.pending_capacity:
                    _old_id, old = self._pending.popitem(last=False)
                    deliveries.append(self._unmatched_or_approximate(old.frame, now))
        for result in deliveries:
            self._publish(result)

    def submit_index(
        self,
        value: frame_index_pb2.FrameIndex,
        now_ns: int | None = None,
    ) -> CacheInsertResult:
        now = self._monotonic_ns() if now_ns is None else now_ns
        deliveries: list[ReceivedVideoFrame] = []
        with self._lock:
            if self._closed:
                return CacheInsertResult.DUPLICATE
            inserted = self.cache.insert(value, now)
            if inserted is not CacheInsertResult.CONFLICT:
                key = (int(value.rtp_ssrc), int(value.rtp_timestamp))
                for pending_id, pending in tuple(self._pending.items()):
                    if (pending.frame.rtp_ssrc, pending.frame.rtp_timestamp) != key:
                        continue
                    exact = self._exact_result(pending.frame, now)
                    if exact is not None:
                        deliveries.append(exact)
                        del self._pending[pending_id]
        for result in deliveries:
            self._publish(result)
        return inserted

    def expire(self, now_ns: int | None = None) -> int:
        now = self._monotonic_ns() if now_ns is None else now_ns
        deliveries: list[ReceivedVideoFrame] = []
        with self._lock:
            self.cache.expire(now)
            for pending_id, pending in tuple(self._pending.items()):
                if pending.deadline_monotonic_ns <= now:
                    deliveries.append(self._unmatched_or_approximate(pending.frame, now))
                    del self._pending[pending_id]
        for result in deliveries:
            self._publish(result)
        return len(deliveries)

    def close(self) -> None:
        with self._lock:
            self._closed = True
            self._pending.clear()
            self.cache.clear()

    def reset_stream(self) -> None:
        """Cancel in-flight work and stale mappings for a replacement stream."""
        with self._lock:
            if self._closed:
                return
            self._pending.clear()
            self.cache.clear()


__all__ = ["CORRELATION_WAIT_NS", "PENDING_FRAME_CAPACITY", "FrameCorrelator"]
