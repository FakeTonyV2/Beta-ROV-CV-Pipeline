"""Single-thread-friendly module registration and heartbeat registry."""

from __future__ import annotations

import time
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass, replace

from purdue_rov.cv.v1 import registration_pb2

HEARTBEAT_EXPIRY_SECONDS = 3.5


@dataclass(frozen=True)
class RegistrationRecord:
    module_id: str
    session_id: bytes
    routing_identity: bytes
    supported_commands: frozenset[str]
    current_state: int
    process_id: int
    host_device_id: str
    registration_monotonic: float
    last_heartbeat_monotonic: float
    available: bool


@dataclass(frozen=True)
class RegistrationDecision:
    accepted: bool
    replaced_session: bool = False
    reason: str = ""
    record: RegistrationRecord | None = None


class ModuleRegistrationRegistry:
    """Keep the latest never-retired execution authoritative per module ID.

    A valid, previously unseen session replaces the current session. The old
    session is retired and can no longer re-register, heartbeat, or respond.
    Router services mutate this object only from their socket-owning thread.
    """

    def __init__(
        self,
        *,
        heartbeat_expiry_seconds: float = HEARTBEAT_EXPIRY_SECONDS,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if heartbeat_expiry_seconds <= 0:
            raise ValueError("heartbeat_expiry_seconds must be positive")
        self._heartbeat_expiry_seconds = heartbeat_expiry_seconds
        self._monotonic = monotonic
        self._records: dict[str, RegistrationRecord] = {}
        # A retired execution must never regain authority during this router
        # process. The configured module-ID set bounds the number of sets; UUIDs
        # are retained for the registry lifetime rather than forgotten after an
        # arbitrary number of restarts.
        self._retired: defaultdict[str, set[bytes]] = defaultdict(set)

    def register(
        self,
        registration: registration_pb2.ModuleRegistration,
        routing_identity: bytes,
    ) -> RegistrationDecision:
        now = self._monotonic()
        module_id = registration.module_id
        session_id = bytes(registration.module_session_id)
        current = self._records.get(module_id)
        if session_id in self._retired[module_id]:
            return RegistrationDecision(False, reason="session has been retired")
        replaced_session = current is not None and current.session_id != session_id
        if replaced_session:
            assert current is not None
            self._retired[module_id].add(current.session_id)
        record = RegistrationRecord(
            module_id=module_id,
            session_id=session_id,
            routing_identity=bytes(routing_identity),
            supported_commands=frozenset(registration.supported_command_types),
            current_state=registration.current_state,
            process_id=registration.process_id,
            host_device_id=registration.host_device_id,
            registration_monotonic=now,
            last_heartbeat_monotonic=now,
            available=True,
        )
        self._records[module_id] = record
        return RegistrationDecision(True, replaced_session, record=record)

    def heartbeat(self, module_id: str, session_id: bytes, routing_identity: bytes) -> bool:
        record = self._records.get(module_id)
        if record is None or record.session_id != session_id or record.routing_identity != routing_identity:
            return False
        self._records[module_id] = replace(
            record,
            last_heartbeat_monotonic=self._monotonic(),
            available=True,
        )
        return True

    def expire(self) -> tuple[str, ...]:
        now = self._monotonic()
        expired: list[str] = []
        for module_id, record in tuple(self._records.items()):
            if record.available and now - record.last_heartbeat_monotonic >= self._heartbeat_expiry_seconds:
                self._records[module_id] = replace(record, available=False)
                expired.append(module_id)
        return tuple(expired)

    def resolve(self, module_id: str) -> RegistrationRecord | None:
        self.expire()
        return self._records.get(module_id)

    def mark_unavailable(self, module_id: str, session_id: bytes) -> bool:
        record = self._records.get(module_id)
        if record is None or record.session_id != session_id:
            return False
        self._records[module_id] = replace(record, available=False)
        return True

    def is_current_session(self, module_id: str, session_id: bytes, routing_identity: bytes) -> bool:
        record = self._records.get(module_id)
        return record is not None and record.session_id == session_id and record.routing_identity == routing_identity

    def snapshot(self) -> dict[str, RegistrationRecord]:
        self.expire()
        return dict(self._records)


__all__ = [
    "HEARTBEAT_EXPIRY_SECONDS",
    "ModuleRegistrationRegistry",
    "RegistrationDecision",
    "RegistrationRecord",
]
