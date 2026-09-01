"""Documented application framing layered over ROUTER/DEALER identity frames.

DEALER peers send exactly ``[kind, protobuf-or-session-payload]``. A ROUTER
therefore receives ``[routing_identity, kind, payload]`` and prepends the
routing identity when replying. The broker never uses this control framing.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

REGISTER_MODULE = b"REGISTER_MODULE"
REGISTER_MODULE_RESPONSE = b"REGISTER_MODULE_RESPONSE"
MODULE_HEARTBEAT = b"MODULE_HEARTBEAT"
COMMAND_REQUEST = b"COMMAND_REQUEST"
COMMAND_RESPONSE = b"COMMAND_RESPONSE"

CLIENT_MESSAGE_KINDS = frozenset({COMMAND_REQUEST})
MODULE_MESSAGE_KINDS = frozenset({REGISTER_MODULE, MODULE_HEARTBEAT, COMMAND_RESPONSE})


@dataclass(frozen=True)
class RouterMessage:
    routing_identity: bytes
    kind: bytes
    payload: bytes


def parse_router_message(frames: Sequence[bytes]) -> RouterMessage | None:
    """Separate the ROUTER identity from the two application frames."""
    if len(frames) != 3 or not all(isinstance(frame, bytes) for frame in frames):
        return None
    return RouterMessage(frames[0], frames[1], frames[2])


def parse_dealer_message(frames: Sequence[bytes]) -> tuple[bytes, bytes] | None:
    if len(frames) != 2 or not all(isinstance(frame, bytes) for frame in frames):
        return None
    return frames[0], frames[1]


__all__ = [
    "CLIENT_MESSAGE_KINDS",
    "COMMAND_REQUEST",
    "COMMAND_RESPONSE",
    "MODULE_HEARTBEAT",
    "MODULE_MESSAGE_KINDS",
    "REGISTER_MODULE",
    "REGISTER_MODULE_RESPONSE",
    "RouterMessage",
    "parse_dealer_message",
    "parse_router_message",
]
