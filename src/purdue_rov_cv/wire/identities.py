"""Pure validation helpers for canonical ZeroMQ DEALER identities."""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

MAX_DEALER_IDENTITY_BYTES = 128


@dataclass(frozen=True)
class IdentityValidationResult:
    identity: str | None
    errors: tuple[str, ...] = ()

    @property
    def valid(self) -> bool:
        return not self.errors


def _canonical_uuid(value: str) -> bool:
    try:
        return str(UUID(value)) == value
    except ValueError:
        return False


def validate_dealer_identity(identity: str | bytes) -> IdentityValidationResult:
    if isinstance(identity, bytes):
        try:
            identity = identity.decode("utf-8")
        except UnicodeDecodeError:
            return IdentityValidationResult(None, ("identity is not valid UTF-8",))
    if not isinstance(identity, str):
        return IdentityValidationResult(None, ("identity must be str or UTF-8 bytes",))
    if len(identity.encode("utf-8")) > MAX_DEALER_IDENTITY_BYTES:
        return IdentityValidationResult(identity, ("identity exceeds 128 bytes",))
    if identity.startswith("client:") and _canonical_uuid(identity.removeprefix("client:")):
        return IdentityValidationResult(identity)
    parts = identity.split(":")
    if len(parts) == 3 and parts[0] == "module" and parts[1] and _canonical_uuid(parts[2]):
        return IdentityValidationResult(identity)
    return IdentityValidationResult(identity, ("identity must be client:<UUID> or module:<module_id>:<UUID>",))
