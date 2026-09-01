"""Canonical process exit statuses for service-supervisor boundaries."""

from dataclasses import dataclass
from enum import IntEnum


class ExitCode(IntEnum):
    CLEAN_SHUTDOWN = 0
    INVALID_ARGUMENTS = 64
    INTERNAL_SOFTWARE_FAILURE = 70
    IO_FAILURE = 74
    TEMPORARY_FAILURE = 75
    INVALID_CONFIGURATION = 78


@dataclass(frozen=True)
class EscalationRequest:
    exit_code: ExitCode
    reason: str
    event_code: str


__all__ = ["EscalationRequest", "ExitCode"]
