"""Structured failures for deterministic configuration handling."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from purdue_rov_cv.wire.errors import ErrorCode


class ConfigIssueKind(StrEnum):
    """Configuration-specific diagnostic kinds, not wire error codes."""

    CONFIG_FILE_NOT_FOUND = "CONFIG_FILE_NOT_FOUND"
    CONFIG_FILE_READ_ERROR = "CONFIG_FILE_READ_ERROR"
    CONFIG_YAML_INVALID = "CONFIG_YAML_INVALID"
    CONFIG_EMPTY = "CONFIG_EMPTY"
    CONFIG_ROOT_INVALID = "CONFIG_ROOT_INVALID"
    CONFIG_SCHEMA_INVALID = "CONFIG_SCHEMA_INVALID"
    CONFIG_STATIC_INVALID = "CONFIG_STATIC_INVALID"
    CONFIG_ENV_INVALID = "CONFIG_ENV_INVALID"
    HARDWARE_PROBE_UNAVAILABLE = "HARDWARE_PROBE_UNAVAILABLE"


@dataclass(frozen=True)
class ConfigIssue:
    code: str
    path: str
    message: str
    values: dict[str, Any] = field(default_factory=dict)


class ConfigurationError(Exception):
    """Base class for a readable, structured configuration failure."""

    error_code = ErrorCode.CONFIG_INVALID

    def __init__(self, issues: list[ConfigIssue] | tuple[ConfigIssue, ...]):
        self.issues = tuple(issues)
        super().__init__("; ".join(f"{issue.code} {issue.path}: {issue.message}" for issue in self.issues))


class ConfigFileError(ConfigurationError):
    pass


class ConfigYamlError(ConfigurationError):
    pass


class ConfigSchemaError(ConfigurationError):
    pass


class ConfigStaticValidationError(ConfigurationError):
    pass
