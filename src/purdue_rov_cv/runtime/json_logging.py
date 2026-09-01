"""Explicit one-record-per-line structured JSON logging for journald/stdout."""

from __future__ import annotations

import json
import math
import sys
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from threading import Lock
from typing import Any, TextIO
from uuid import UUID

from purdue_rov_cv.config.models import LogLevel


@dataclass(frozen=True)
class LogContext:
    device_id: str
    process_name: str
    source_id: str
    publisher_session_id: UUID | bytes | str | None


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        if math.isfinite(value):
            return value
        if math.isnan(value):
            return "NaN"
        return "Infinity" if value > 0 else "-Infinity"
    if isinstance(value, Enum):
        return _json_safe(value.value)
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, Path):
        return value.as_posix()
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))}
    if isinstance(value, (list, tuple, set, frozenset)):
        sequence = sorted(value, key=repr) if isinstance(value, (set, frozenset)) else value
        return [_json_safe(item) for item in sequence]
    return repr(value)


def _uuid_safe(value: UUID | bytes | str | None) -> str | None:
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, bytes):
        return str(UUID(bytes=value)) if len(value) == 16 else value.hex()
    return value


class StructuredJsonLogger:
    """Emit required fields on every line; absent optional context is JSON null."""

    def __init__(
        self,
        context: LogContext,
        *,
        stream: TextIO = sys.stdout,
        utc_now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._context = context
        self._stream = stream
        self._utc_now = utc_now
        self._lock = Lock()

    def log(
        self,
        level: LogLevel | str,
        event_code: str,
        message: str,
        *,
        camera_id: str | None = None,
        camera_session_id: UUID | bytes | str | None = None,
        frame_number: int | None = None,
        command_id: UUID | bytes | str | None = None,
        command_type: str | None = None,
        target_id: str | None = None,
        exception: BaseException | None = None,
        context: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        canonical_level = LogLevel(level)
        record: dict[str, Any] = {
            "timestamp_utc": self._utc_now().astimezone(UTC).isoformat().replace("+00:00", "Z"),
            "level": canonical_level.value,
            "device_id": self._context.device_id,
            "process_name": self._context.process_name,
            "source_id": self._context.source_id,
            "event_code": event_code,
            "message": message,
            "publisher_session_id": _uuid_safe(self._context.publisher_session_id),
            "camera_id": camera_id,
            "camera_session_id": _uuid_safe(camera_session_id),
            "frame_number": frame_number,
            "command_id": _uuid_safe(command_id),
            "command_type": command_type,
            "target_id": target_id,
            "context": _json_safe(context or {}),
            "exception": None,
        }
        if exception is not None:
            record["exception"] = {
                "type": type(exception).__name__,
                "message": str(exception),
            }
        line = json.dumps(record, sort_keys=True, separators=(",", ":"), allow_nan=False)
        with self._lock:
            self._stream.write(line + "\n")
            self._stream.flush()
        return record

    def receive_timeout(self, message: str = "queue receive timeout") -> dict[str, Any]:
        """Expected polling timeouts are always DEBUG records."""
        return self.log(LogLevel.DEBUG, "QUEUE_RECEIVE_TIMEOUT", message)


def configure_json_logger(
    *,
    device_id: str,
    process_name: str,
    source_id: str,
    publisher_session_id: UUID | bytes | str | None,
    stream: TextIO = sys.stdout,
) -> StructuredJsonLogger:
    """Construct a logger explicitly without mutating Python's root logger."""
    return StructuredJsonLogger(
        LogContext(device_id, process_name, source_id, publisher_session_id),
        stream=stream,
    )


__all__ = ["LogContext", "LogLevel", "StructuredJsonLogger", "configure_json_logger"]
