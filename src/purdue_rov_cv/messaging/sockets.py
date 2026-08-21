"""Canonical Phase 4 ZeroMQ socket settings and DEALER identities."""

from __future__ import annotations

from uuid import UUID

import zmq

from purdue_rov_cv.wire.identities import validate_dealer_identity

BROKER_HWM = 100
CONTROL_ROUTER_HWM = 100
DEALER_HWM = 32
TRANSPORT_MAX_MESSAGE_BYTES = 4 * 1024 * 1024
CONTROL_RECEIVE_TIMEOUT_MS = 250
CONTROL_SEND_TIMEOUT_MS = 200


def configure_xsub(socket: zmq.Socket[bytes]) -> None:
    socket.setsockopt(zmq.RCVHWM, BROKER_HWM)
    socket.setsockopt(zmq.SNDHWM, BROKER_HWM)
    socket.setsockopt(zmq.LINGER, 0)
    socket.setsockopt(zmq.MAXMSGSIZE, TRANSPORT_MAX_MESSAGE_BYTES)
    socket.setsockopt(zmq.TCP_KEEPALIVE, 1)


def configure_xpub(socket: zmq.Socket[bytes]) -> None:
    socket.setsockopt(zmq.RCVHWM, BROKER_HWM)
    socket.setsockopt(zmq.SNDHWM, BROKER_HWM)
    socket.setsockopt(zmq.LINGER, 0)
    socket.setsockopt(zmq.MAXMSGSIZE, TRANSPORT_MAX_MESSAGE_BYTES)
    socket.setsockopt(zmq.TCP_KEEPALIVE, 1)
    socket.setsockopt(zmq.XPUB_VERBOSE, 1)


def configure_router(socket: zmq.Socket[bytes], endpoint: str) -> None:
    socket.setsockopt(zmq.RCVHWM, CONTROL_ROUTER_HWM)
    socket.setsockopt(zmq.SNDHWM, CONTROL_ROUTER_HWM)
    socket.setsockopt(zmq.RCVTIMEO, CONTROL_RECEIVE_TIMEOUT_MS)
    socket.setsockopt(zmq.SNDTIMEO, CONTROL_SEND_TIMEOUT_MS)
    socket.setsockopt(zmq.LINGER, 0)
    socket.setsockopt(zmq.ROUTER_MANDATORY, 1)
    if endpoint.startswith("tcp://"):
        socket.setsockopt(zmq.TCP_KEEPALIVE, 1)


def configure_dealer(socket: zmq.Socket[bytes], identity: str) -> None:
    validation = validate_dealer_identity(identity)
    if not validation.valid:
        raise ValueError("; ".join(validation.errors))
    socket.setsockopt(zmq.IDENTITY, identity.encode("utf-8"))
    socket.setsockopt(zmq.RCVHWM, DEALER_HWM)
    socket.setsockopt(zmq.SNDHWM, DEALER_HWM)
    socket.setsockopt(zmq.RCVTIMEO, CONTROL_RECEIVE_TIMEOUT_MS)
    socket.setsockopt(zmq.SNDTIMEO, CONTROL_SEND_TIMEOUT_MS)
    socket.setsockopt(zmq.LINGER, 0)
    socket.setsockopt(zmq.IMMEDIATE, 1)
    socket.setsockopt(zmq.RECONNECT_IVL, 250)
    socket.setsockopt(zmq.RECONNECT_IVL_MAX, 2_000)


def module_identity(module_id: str, session_uuid: UUID) -> str:
    identity = f"module:{module_id}:{session_uuid}"
    validation = validate_dealer_identity(identity)
    if not validation.valid:
        raise ValueError("; ".join(validation.errors))
    return identity


def client_identity(client_uuid: UUID) -> str:
    identity = f"client:{client_uuid}"
    validation = validate_dealer_identity(identity)
    if not validation.valid:
        raise ValueError("; ".join(validation.errors))
    return identity


__all__ = [
    "BROKER_HWM",
    "CONTROL_RECEIVE_TIMEOUT_MS",
    "CONTROL_ROUTER_HWM",
    "CONTROL_SEND_TIMEOUT_MS",
    "DEALER_HWM",
    "TRANSPORT_MAX_MESSAGE_BYTES",
    "client_identity",
    "configure_dealer",
    "configure_router",
    "configure_xpub",
    "configure_xsub",
    "module_identity",
]
