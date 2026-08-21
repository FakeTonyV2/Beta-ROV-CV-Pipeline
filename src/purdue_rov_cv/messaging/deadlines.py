"""Canonical command completion limits used by control clients."""

from __future__ import annotations

from purdue_rov.cv.v1 import control_pb2

COMMAND_COMPLETION_SECONDS: dict[str, float] = {
    "get_status": 1.0,
    "request_debug_snapshot": 2.0,
    "set_dynamic_config": 3.0,
    "start_recording": 3.0,
    "stop_recording": 3.0,
    "start": 5.0,
    "stop": 5.0,
    "set_mode": 5.0,
    "reset": 10.0,
    "get_command_status": 1.0,
}


def completion_timeout_seconds(request: control_pb2.CommandRequest) -> float:
    command_type = request.WhichOneof("command")
    if command_type not in COMMAND_COMPLETION_SECONDS:
        raise ValueError("request has no canonical command")
    canonical = COMMAND_COMPLETION_SECONDS[command_type]
    if request.requested_timeout_ms == 0:
        return canonical
    # The specification is ambiguous about extensions. A caller may shorten a
    # deadline but cannot silently extend the safety-oriented canonical limit.
    return min(canonical, request.requested_timeout_ms / 1_000.0)


__all__ = ["COMMAND_COMPLETION_SECONDS", "completion_timeout_seconds"]
