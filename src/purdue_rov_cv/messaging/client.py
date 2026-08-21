"""Minimal real DEALER control client with no automatic command resend."""

from __future__ import annotations

import os
import time
from collections.abc import Callable
from threading import get_ident
from uuid import UUID, uuid4

import zmq
from google.protobuf.message import DecodeError
from purdue_rov.cv.v1 import control_pb2

from purdue_rov_cv.runtime.json_logging import StructuredJsonLogger
from purdue_rov_cv.runtime.metrics import RuntimeMetrics
from purdue_rov_cv.wire.errors import ErrorCode
from purdue_rov_cv.wire.validators import validate_command_request, validate_command_response

from .deadlines import completion_timeout_seconds
from .protocol import COMMAND_REQUEST, COMMAND_RESPONSE, parse_dealer_message
from .sockets import client_identity, configure_dealer

COMMAND_ACK_TIMEOUT_SECONDS = 0.5


class ControlClient:
    """Thread-confined client fixture that preserves the caller's command UUID."""

    def __init__(
        self,
        endpoint: str,
        *,
        client_uuid: UUID | None = None,
        acknowledgement_timeout_seconds: float = COMMAND_ACK_TIMEOUT_SECONDS,
        metrics: RuntimeMetrics | None = None,
        logger: StructuredJsonLogger | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if acknowledgement_timeout_seconds <= 0:
            raise ValueError("acknowledgement_timeout_seconds must be positive")
        self.endpoint = endpoint
        self.client_uuid = client_uuid or uuid4()
        self.identity = client_identity(self.client_uuid)
        self.acknowledgement_timeout_seconds = acknowledgement_timeout_seconds
        self.metrics = metrics or RuntimeMetrics(monotonic=monotonic)
        self.logger = logger
        self._monotonic = monotonic
        self._sleep = sleep
        self._owner_process_id = os.getpid()
        self._owner_thread_id = get_ident()
        self._context = zmq.Context()
        self._socket = self._open_socket()
        self._unknown_commands: set[bytes] = set()
        self._queried_unknown_commands: set[bytes] = set()
        self.send_attempts = 0

    def _assert_owner(self) -> None:
        if os.getpid() != self._owner_process_id:
            raise RuntimeError("ZeroMQ client context/socket used after fork")
        if get_ident() != self._owner_thread_id:
            raise RuntimeError("ZeroMQ client socket used from a non-owning thread")

    def _open_socket(self) -> zmq.Socket[bytes]:
        socket: zmq.Socket[bytes] = self._context.socket(zmq.DEALER)
        configure_dealer(socket, self.identity)
        socket.connect(self.endpoint)
        return socket

    def reconnect(self) -> None:
        self._assert_owner()
        self._socket.close(linger=0)
        self._socket = self._open_socket()
        self.metrics.increment("reconnect_count")

    def _outcome_unknown(self, request: control_pb2.CommandRequest) -> control_pb2.CommandResponse:
        self._unknown_commands.add(bytes(request.command_id))
        if self.logger is not None:
            self.logger.log(
                "WARNING",
                "COMMAND_ACK_TIMEOUT",
                "initial acknowledgement timed out; command was not resent",
                command_id=request.command_id,
                command_type=request.WhichOneof("command"),
                target_id=request.target_id,
            )
        return control_pb2.CommandResponse(
            command_id=request.command_id,
            target_id=request.target_id,
            status=control_pb2.COMMAND_STATUS_OUTCOME_UNKNOWN,
            error_code=ErrorCode.COMMAND_OUTCOME_UNKNOWN.value,
            message="initial command acknowledgement timed out; command was not resent",
            response_time_unix_ns=time.time_ns(),
        )

    def _receive_correlated(self, command_id: bytes, deadline: float) -> control_pb2.CommandResponse | None:
        while True:
            remaining_ms = max(0, int((deadline - self._monotonic()) * 1_000))
            if remaining_ms <= 0 or not self._socket.poll(remaining_ms, zmq.POLLIN):
                return None
            frames = self._socket.recv_multipart()
            message = parse_dealer_message(frames)
            if message is None or message[0] != COMMAND_RESPONSE:
                self.metrics.increment("invalid_messages")
                continue
            response = control_pb2.CommandResponse()
            try:
                response.ParseFromString(message[1])
            except (DecodeError, TypeError, ValueError):
                self.metrics.increment("invalid_messages")
                continue
            if not validate_command_response(response).valid or response.command_id != command_id:
                self.metrics.increment("invalid_messages")
                continue
            self.metrics.increment("messages_received")
            return response

    def send_command(self, request: control_pb2.CommandRequest) -> control_pb2.CommandResponse:
        self._assert_owner()
        validation = validate_command_request(request)
        if not validation.valid:
            raise ValueError("; ".join(error.detail for error in validation.errors))
        self.send_attempts += 1
        try:
            self._socket.send_multipart([COMMAND_REQUEST, request.SerializeToString()])
        except zmq.Again:
            self.reconnect()
            return self._outcome_unknown(request)
        self.metrics.increment("messages_sent")
        response = self._receive_correlated(
            bytes(request.command_id),
            self._monotonic() + self.acknowledgement_timeout_seconds,
        )
        if response is None:
            self.reconnect()
            return self._outcome_unknown(request)
        return response

    def execute_command(self, request: control_pb2.CommandRequest) -> control_pb2.CommandResponse:
        """Wait for completion and status-poll without ever resending *request*."""
        response = self.send_command(request)
        if response.status not in {
            control_pb2.COMMAND_STATUS_RECEIVED,
            control_pb2.COMMAND_STATUS_ACCEPTED,
        }:
            return response
        completed = self._receive_correlated(
            bytes(request.command_id),
            self._monotonic() + completion_timeout_seconds(request),
        )
        if completed is not None:
            return completed
        if self.logger is not None:
            self.logger.log(
                "WARNING",
                "COMMAND_COMPLETION_TIMEOUT",
                "accepted command remains pending; beginning bounded status lookup",
                command_id=request.command_id,
                command_type=request.WhichOneof("command"),
                target_id=request.target_id,
            )
        polling_deadline = self._monotonic() + 10.0
        while self._monotonic() < polling_deadline:
            status = self.get_command_status(request.target_id, bytes(request.command_id))
            if not (
                status.status == control_pb2.COMMAND_STATUS_REJECTED and status.error_code == ErrorCode.INVALID_COMMAND
            ) and status.status not in {
                control_pb2.COMMAND_STATUS_RECEIVED,
                control_pb2.COMMAND_STATUS_ACCEPTED,
                control_pb2.COMMAND_STATUS_OUTCOME_UNKNOWN,
            }:
                result = control_pb2.CommandResponse()
                result.CopyFrom(status)
                result.command_id = request.command_id
                return result
            remaining = polling_deadline - self._monotonic()
            if remaining > 0:
                self._sleep(min(1.0, remaining))
        return control_pb2.CommandResponse(
            command_id=request.command_id,
            target_id=request.target_id,
            status=control_pb2.COMMAND_STATUS_OUTCOME_UNKNOWN,
            error_code=ErrorCode.COMMAND_OUTCOME_UNKNOWN.value,
            message="accepted command did not reach a final result within status-polling limit",
            response_time_unix_ns=time.time_ns(),
        )

    def get_command_status(self, target_id: str, target_command_id: bytes) -> control_pb2.CommandResponse:
        self._assert_owner()
        canonical_id = bytes(target_command_id)
        if canonical_id in self._unknown_commands:
            if canonical_id in self._queried_unknown_commands:
                raise RuntimeError("only one status query is permitted after acknowledgement timeout")
            self._queried_unknown_commands.add(canonical_id)
        request = control_pb2.CommandRequest(
            command_id=uuid4().bytes,
            target_id=target_id,
            issued_time_unix_ns=time.time_ns(),
            get_command_status=control_pb2.GetCommandStatus(target_command_id=canonical_id),
        )
        return self.send_command(request)

    def close(self) -> None:
        self._assert_owner()
        self._socket.close(linger=0)
        self._context.term()

    def __enter__(self) -> ControlClient:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()


__all__ = ["COMMAND_ACK_TIMEOUT_SECONDS", "ControlClient"]
