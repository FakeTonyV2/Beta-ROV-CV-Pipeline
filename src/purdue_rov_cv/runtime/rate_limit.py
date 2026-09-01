"""Thread-safe monotonic warning suppression with bounded key storage."""

from __future__ import annotations

import time
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass
from threading import Lock
from typing import Hashable


@dataclass(frozen=True)
class WarningDecision:
    emit: bool
    suppressed_count: int


@dataclass
class _Entry:
    last_emitted: float
    last_seen: float
    suppressed: int = 0


class WarningRateLimiter:
    """Emit each stable warning key at most once per interval."""

    def __init__(
        self,
        *,
        interval_seconds: float = 1.0,
        max_keys: int = 1024,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if interval_seconds <= 0 or max_keys <= 0:
            raise ValueError("interval_seconds and max_keys must be positive")
        self._interval = interval_seconds
        self._max_keys = max_keys
        self._monotonic = monotonic
        self._entries: OrderedDict[Hashable, _Entry] = OrderedDict()
        self._lock = Lock()

    def check(self, key: Hashable) -> WarningDecision:
        now = self._monotonic()
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                self._entries[key] = _Entry(now, now)
                self._entries.move_to_end(key)
                while len(self._entries) > self._max_keys:
                    self._entries.popitem(last=False)
                return WarningDecision(True, 0)
            entry.last_seen = now
            self._entries.move_to_end(key)
            if now - entry.last_emitted < self._interval:
                entry.suppressed += 1
                return WarningDecision(False, entry.suppressed)
            suppressed = entry.suppressed
            entry.last_emitted = now
            entry.suppressed = 0
            return WarningDecision(True, suppressed)

    @property
    def tracked_keys(self) -> int:
        with self._lock:
            return len(self._entries)


__all__ = ["WarningDecision", "WarningRateLimiter"]
