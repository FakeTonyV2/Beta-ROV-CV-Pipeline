"""Bounded monotonic command-status retention for target modules."""

from __future__ import annotations

import time
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from threading import RLock

from purdue_rov.cv.v1 import control_pb2

COMMAND_STATUS_TTL_SECONDS = 10 * 60.0
COMMAND_STATUS_CAPACITY = 1_024


@dataclass(frozen=True)
class CachedCommandResult:
    command_id: bytes
    response: control_pb2.CommandResponse
    stored_monotonic: float


class CommandReservationStatus(StrEnum):
    RESERVED = "RESERVED"
    DUPLICATE = "DUPLICATE"
    CAPACITY_FULL = "CAPACITY_FULL"


_PENDING_STATUSES = frozenset(
    {
        control_pb2.COMMAND_STATUS_RECEIVED,
        control_pb2.COMMAND_STATUS_ACCEPTED,
    }
)


def _copy_response(response: control_pb2.CommandResponse) -> control_pb2.CommandResponse:
    copied = control_pb2.CommandResponse()
    copied.CopyFrom(response)
    return copied


class CommandStatusCache:
    """FIFO capacity eviction with TTL measured only by a monotonic clock."""

    def __init__(
        self,
        *,
        ttl_seconds: float = COMMAND_STATUS_TTL_SECONDS,
        capacity: int = COMMAND_STATUS_CAPACITY,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if ttl_seconds <= 0 or capacity <= 0:
            raise ValueError("ttl_seconds and capacity must be positive")
        self._ttl_seconds = ttl_seconds
        self._capacity = capacity
        self._monotonic = monotonic
        self._entries: OrderedDict[bytes, CachedCommandResult] = OrderedDict()
        self._lock = RLock()

    @staticmethod
    def _is_pending(entry: CachedCommandResult) -> bool:
        return entry.response.status in _PENDING_STATUSES

    def _purge_expired(self, now: float) -> None:
        expired = [
            key
            for key, entry in self._entries.items()
            if not self._is_pending(entry) and now - entry.stored_monotonic >= self._ttl_seconds
        ]
        for key in expired:
            del self._entries[key]

    def _make_room(self) -> bool:
        while len(self._entries) >= self._capacity:
            evictable = next(
                (key for key, entry in self._entries.items() if not self._is_pending(entry)),
                None,
            )
            if evictable is None:
                return False
            del self._entries[evictable]
        return True

    def put(self, response: control_pb2.CommandResponse) -> bool:
        if len(response.command_id) != 16:
            raise ValueError("cached command_id must be a 16-byte UUID")
        now = self._monotonic()
        with self._lock:
            self._purge_expired(now)
            command_id = bytes(response.command_id)
            if command_id not in self._entries and not self._make_room():
                return False
            self._entries[command_id] = CachedCommandResult(command_id, _copy_response(response), now)
            self._entries.move_to_end(command_id)
            return True

    def try_reserve(self, response: control_pb2.CommandResponse) -> CommandReservationStatus:
        """Atomically distinguish a duplicate from exhausted active capacity."""

        if len(response.command_id) != 16:
            raise ValueError("reserved command_id must be a 16-byte UUID")
        now = self._monotonic()
        command_id = bytes(response.command_id)
        with self._lock:
            self._purge_expired(now)
            if command_id in self._entries:
                return CommandReservationStatus.DUPLICATE
            if not self._make_room():
                return CommandReservationStatus.CAPACITY_FULL
            self._entries[command_id] = CachedCommandResult(command_id, _copy_response(response), now)
            return CommandReservationStatus.RESERVED

    def reserve(self, response: control_pb2.CommandResponse) -> bool:
        """Atomically retain an initial status only when its UUID is unseen.

        The response normally carries ``COMMAND_STATUS_RECEIVED``.  Keeping the
        complete canonical response makes an in-flight command visible through
        ``GET_COMMAND_STATUS`` without introducing a second status model.
        """
        return self.try_reserve(response) is CommandReservationStatus.RESERVED

    def get(self, command_id: bytes) -> control_pb2.CommandResponse | None:
        if len(command_id) != 16:
            return None
        now = self._monotonic()
        with self._lock:
            self._purge_expired(now)
            entry = self._entries.get(bytes(command_id))
            return None if entry is None else _copy_response(entry.response)

    def __len__(self) -> int:
        now = self._monotonic()
        with self._lock:
            self._purge_expired(now)
            return len(self._entries)


__all__ = [
    "COMMAND_STATUS_CAPACITY",
    "COMMAND_STATUS_TTL_SECONDS",
    "CachedCommandResult",
    "CommandReservationStatus",
    "CommandStatusCache",
]
