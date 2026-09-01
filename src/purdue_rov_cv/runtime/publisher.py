"""Thread-safe publisher session identity and attempted-publication sequence."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from threading import Lock
from uuid import UUID, uuid4


@dataclass(frozen=True)
class PublicationIdentity:
    publisher_session_id: bytes
    sequence_number: int


class PublisherSequence:
    """Consume one sequence number before every attempted publication."""

    def __init__(self, *, uuid_factory: Callable[[], UUID] = uuid4) -> None:
        self._session_uuid = uuid_factory()
        self._next_sequence = 0
        self._lock = Lock()

    @property
    def session_uuid(self) -> UUID:
        return self._session_uuid

    @property
    def session_id(self) -> bytes:
        return self._session_uuid.bytes

    def next_attempt(self) -> PublicationIdentity:
        with self._lock:
            sequence = self._next_sequence
            self._next_sequence += 1
        return PublicationIdentity(self.session_id, sequence)


__all__ = ["PublicationIdentity", "PublisherSequence"]
