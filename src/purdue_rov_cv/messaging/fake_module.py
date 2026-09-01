"""Deterministic simulated target module using a real DEALER socket."""

from __future__ import annotations

import os
import time
from collections.abc import Callable, Collection
from dataclasses import dataclass
from uuid import UUID, uuid4

import zmq
from google.protobuf.message import DecodeError
from purdue_rov.cv.v1 import control_pb2, registration_pb2

from purdue_rov_cv.runtime.exit_codes import EscalationRequest, ExitCode
from purdue_rov_cv.runtime.json_logging import StructuredJsonLogger
from purdue_rov_cv.runtime.metrics import RuntimeMetrics
from purdue_rov_cv.runtime.rate_limit import WarningRateLimiter
from purdue_rov_cv.runtime.shutdown import ShutdownCoordinator, install_signal_handlers
from purdue_rov_cv.runtime.state import ComponentState, ComponentStateMachine, to_wire_component_state
from purdue_rov_cv.wire.errors import ErrorCode
from purdue_rov_cv.wire.validators import (
    COMMAND_ONEOF_NAMES,
    validate_command_request,
    validate_module_registration_response,
)

from .cache import CommandReservationStatus, CommandStatusCache
from .protocol import (
    COMMAND_REQUEST,
    COMMAND_RESPONSE,
    MODULE_HEARTBEAT,
    REGISTER_MODULE,
    REGISTER_MODULE_RESPONSE,
    parse_dealer_message,
)
from .sockets import configure_dealer, module_identity

REGISTRATION_ACK_TIMEOUT_SECONDS = 0.5
REGISTRATION_RETRY_SECONDS = 1.0
REGISTRATION_MAX_ATTEMPTS = 10
HEARTBEAT_INTERVAL_SECONDS = 1.0

STATE_CHANGING_COMMANDS = frozenset(
    {
        "start",
        "stop",
        "set_mode",
        "set_dynamic_config",
        "start_recording",
        "stop_recording",
        "reset",
    }
)


@dataclass(frozen=True)
class RegistrationRetryState:
    attempts: int
    next_attempt_monotonic: float
    acknowledgement_deadline_monotonic: float | None
    escalation: EscalationRequest | None = None


class RegistrationRetryController:
    """Pure timing policy; socket code owns sending and process boundaries own exit."""

    def __init__(
        self,
        *,
        retry_seconds: float = REGISTRATION_RETRY_SECONDS,
        acknowledgement_timeout_seconds: float = REGISTRATION_ACK_TIMEOUT_SECONDS,
        maximum_attempts: int = REGISTRATION_MAX_ATTEMPTS,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if retry_seconds <= 0 or acknowledgement_timeout_seconds <= 0 or maximum_attempts <= 0:
            raise ValueError("registration retry settings must be positive")
        self._retry_seconds = retry_seconds
        self._acknowledgement_timeout_seconds = acknowledgement_timeout_seconds
        self._maximum_attempts = maximum_attempts
        self._monotonic = monotonic
        self._attempts = 0
        self._next_attempt = monotonic()
        self._ack_deadline: float | None = None
        self._escalation: EscalationRequest | None = None

    @property
    def state(self) -> RegistrationRetryState:
        return RegistrationRetryState(self._attempts, self._next_attempt, self._ack_deadline, self._escalation)

    def should_attempt(self) -> bool:
        return self._escalation is None and self._monotonic() >= self._next_attempt and self._ack_deadline is None

    def attempted(self) -> None:
        now = self._monotonic()
        self._attempts += 1
        self._next_attempt = now + self._retry_seconds
        self._ack_deadline = now + self._acknowledgement_timeout_seconds

    def update(self) -> EscalationRequest | None:
        now = self._monotonic()
        if self._ack_deadline is not None and now >= self._ack_deadline:
            self._ack_deadline = None
            if self._attempts >= self._maximum_attempts:
                self._escalation = EscalationRequest(
                    ExitCode.TEMPORARY_FAILURE,
                    "module registration failed after ten attempts",
                    "MODULE_REGISTRATION_FAILED",
                )
        return self._escalation

    def acknowledged(self) -> None:
        self._ack_deadline = None


def _response(
    request: control_pb2.CommandRequest,
    status: control_pb2.CommandStatus,
    *,
    state: ComponentState,
    error_code: ErrorCode | None = None,
    message: str = "",
) -> control_pb2.CommandResponse:
    return control_pb2.CommandResponse(
        command_id=request.command_id,
        target_id=request.target_id,
        status=status,
        error_code="" if error_code is None else error_code.value,
        message=message,
        resulting_state=state.value,
        response_time_unix_ns=time.time_ns(),
    )


class FakeModuleService:
    """A reusable Phase 4 fixture, not a task-specific inference implementation."""

    def __init__(
        self,
        module_endpoint: str,
        *,
        module_id: str,
        task_id: str,
        host_device_id: str,
        supported_commands: Collection[str] = COMMAND_ONEOF_NAMES,
        session_uuid: UUID | None = None,
        heartbeat_interval_seconds: float = HEARTBEAT_INTERVAL_SECONDS,
        registration_retry_seconds: float = REGISTRATION_RETRY_SECONDS,
        registration_ack_timeout_seconds: float = REGISTRATION_ACK_TIMEOUT_SECONDS,
        registration_max_attempts: int = REGISTRATION_MAX_ATTEMPTS,
        metrics: RuntimeMetrics | None = None,
        logger: StructuredJsonLogger | None = None,
        warning_limiter: WarningRateLimiter | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        install_signals: bool = False,
    ) -> None:
        invalid_commands = set(supported_commands) - COMMAND_ONEOF_NAMES
        if invalid_commands:
            raise ValueError(f"non-canonical supported commands: {sorted(invalid_commands)}")
        if heartbeat_interval_seconds <= 0:
            raise ValueError("heartbeat_interval_seconds must be positive")
        self.module_endpoint = module_endpoint
        self.module_id = module_id
        self.task_id = task_id
        self.host_device_id = host_device_id
        self.supported_commands = frozenset(supported_commands)
        self.session_uuid = session_uuid or uuid4()
        self.identity = module_identity(module_id, self.session_uuid)
        self.heartbeat_interval_seconds = heartbeat_interval_seconds
        self.metrics = metrics or RuntimeMetrics(monotonic=monotonic)
        self.logger = logger
        self.warning_limiter = warning_limiter or WarningRateLimiter(monotonic=monotonic)
        self.state_machine = ComponentStateMachine()
        self.shutdown = ShutdownCoordinator(state_machine=self.state_machine, monotonic=monotonic)
        self.cache = CommandStatusCache(monotonic=monotonic)
        self._monotonic = monotonic
        self._install_signals = install_signals
        self._retry_settings = (
            registration_retry_seconds,
            registration_ack_timeout_seconds,
            registration_max_attempts,
        )
        self.execution_counts: dict[str, int] = {}
        self.registration_succeeded = False

    def request_shutdown(self, reason: str = "requested") -> None:
        self.shutdown.request(reason)

    def _registration(self) -> registration_pb2.ModuleRegistration:
        return registration_pb2.ModuleRegistration(
            module_id=self.module_id,
            task_id=self.task_id,
            module_session_id=self.session_uuid.bytes,
            supported_command_types=sorted(self.supported_commands),
            current_state=to_wire_component_state(self.state_machine.state),
            process_id=os.getpid(),
            host_device_id=self.host_device_id,
        )

    def _send_response(
        self,
        socket: zmq.Socket[bytes],
        response: control_pb2.CommandResponse,
        *,
        cache_result: bool,
    ) -> None:
        if cache_result:
            self.cache.put(response)
        try:
            socket.send_multipart([COMMAND_RESPONSE, response.SerializeToString()])
        except zmq.Again as error:
            warning = self.warning_limiter.check("CONTROL_RESPONSE_SEND_TIMEOUT")
            if warning.emit and self.logger is not None:
                self.logger.log(
                    "WARNING",
                    "CONTROL_RESPONSE_SEND_TIMEOUT",
                    "command response exceeded the 200 ms send deadline; cached result retained",
                    command_id=response.command_id,
                    target_id=response.target_id,
                    exception=error,
                    context={"previously_suppressed": warning.suppressed_count},
                )
            elif not warning.emit:
                self.metrics.increment("warnings_suppressed")
            return
        self.metrics.increment("messages_sent")

    def _status_lookup(self, request: control_pb2.CommandRequest) -> control_pb2.CommandResponse:
        cached = self.cache.get(request.get_command_status.target_command_id)
        if cached is None:
            return _response(
                request,
                control_pb2.COMMAND_STATUS_REJECTED,
                state=self.state_machine.state,
                error_code=ErrorCode.INVALID_COMMAND,
                message="command status is unknown or expired",
            )
        return control_pb2.CommandResponse(
            command_id=request.command_id,
            target_id=request.target_id,
            status=cached.status,
            error_code=cached.error_code,
            message=cached.message,
            resulting_state=cached.resulting_state,
            response_time_unix_ns=time.time_ns(),
        )

    def _execute(
        self,
        request: control_pb2.CommandRequest,
        *,
        already_reserved: bool = False,
    ) -> control_pb2.CommandResponse:
        validation = validate_command_request(request)
        if not validation.valid or request.target_id != self.module_id:
            return _response(
                request,
                control_pb2.COMMAND_STATUS_REJECTED,
                state=self.state_machine.state,
                error_code=ErrorCode.INVALID_COMMAND,
                message="invalid command",
            )
        command_type = request.WhichOneof("command")
        assert command_type is not None
        if command_type == "get_command_status":
            return self._status_lookup(request)
        if (
            command_type in STATE_CHANGING_COMMANDS
            and not already_reserved
            and self.cache.get(bytes(request.command_id)) is not None
        ):
            return _response(
                request,
                control_pb2.COMMAND_STATUS_REJECTED,
                state=self.state_machine.state,
                error_code=ErrorCode.DUPLICATE_COMMAND_ID,
                message="command ID already executed; use get_command_status",
            )
        self.execution_counts[command_type] = self.execution_counts.get(command_type, 0) + 1
        transition = None
        if command_type == "start":
            transition = self.state_machine.transition_to(ComponentState.RUNNING)
        elif command_type == "stop":
            transition = self.state_machine.transition_to(ComponentState.READY)
        elif command_type == "reset":
            transition = self.state_machine.reset_from_error()
        if transition is not None and not transition.accepted:
            return _response(
                request,
                control_pb2.COMMAND_STATUS_REJECTED,
                state=self.state_machine.state,
                error_code=ErrorCode.INVALID_STATE_TRANSITION,
                message=transition.detail,
            )
        return _response(
            request,
            control_pb2.COMMAND_STATUS_COMPLETED,
            state=self.state_machine.state,
            message=f"executed {command_type} count={self.execution_counts[command_type]}",
        )

    def _handle_command(self, socket: zmq.Socket[bytes], payload: bytes) -> None:
        request = control_pb2.CommandRequest()
        try:
            request.ParseFromString(payload)
        except (DecodeError, TypeError, ValueError):
            self.metrics.increment("invalid_messages")
            return
        if validate_command_request(request).valid and request.target_id == self.module_id:
            self.metrics.increment("messages_received")
        else:
            self.metrics.increment("invalid_messages")
        command_type = request.WhichOneof("command")
        duplicate = False
        reserved = False
        if (
            validate_command_request(request).valid
            and request.target_id == self.module_id
            and command_type in STATE_CHANGING_COMMANDS
            and len(request.command_id) == 16
        ):
            reservation = self.cache.try_reserve(
                _response(
                    request,
                    control_pb2.COMMAND_STATUS_RECEIVED,
                    state=self.state_machine.state,
                    message="command reserved for execution",
                )
            )
            if reservation is CommandReservationStatus.CAPACITY_FULL:
                self._send_response(
                    socket,
                    _response(
                        request,
                        control_pb2.COMMAND_STATUS_REJECTED,
                        state=self.state_machine.state,
                        error_code=ErrorCode.MODULE_BUSY,
                        message="command-status cache is full of active commands",
                    ),
                    cache_result=False,
                )
                return
            reserved = reservation is CommandReservationStatus.RESERVED
            duplicate = reservation is CommandReservationStatus.DUPLICATE
        response = self._execute(request, already_reserved=reserved)
        cache_result = len(request.command_id) == 16 and command_type != "get_command_status" and not duplicate
        self._send_response(socket, response, cache_result=cache_result)

    def _handle_registration_response(self, payload: bytes, retry: RegistrationRetryController) -> bool:
        response = registration_pb2.ModuleRegistrationResponse()
        try:
            response.ParseFromString(payload)
        except (DecodeError, TypeError, ValueError):
            self.metrics.increment("invalid_messages")
            return False
        if not validate_module_registration_response(response).valid:
            self.metrics.increment("invalid_messages")
            return False
        self.metrics.increment("messages_received")
        if not response.accepted:
            return False
        retry.acknowledged()
        self.registration_succeeded = True
        self.state_machine.transition_to(ComponentState.READY)
        return True

    def run(self) -> ExitCode:
        if self._install_signals:
            install_signal_handlers(self.shutdown)
        context = zmq.Context()
        socket: zmq.Socket[bytes] | None = None
        exit_code = ExitCode.CLEAN_SHUTDOWN
        try:
            socket = context.socket(zmq.DEALER)
            configure_dealer(socket, self.identity)
            socket.connect(self.module_endpoint)
            retry_seconds, ack_seconds, maximum_attempts = self._retry_settings
            retry = RegistrationRetryController(
                retry_seconds=retry_seconds,
                acknowledgement_timeout_seconds=ack_seconds,
                maximum_attempts=maximum_attempts,
                monotonic=self._monotonic,
            )
            next_heartbeat = self._monotonic()
            poller = zmq.Poller()
            poller.register(socket, zmq.POLLIN)
            while not self.shutdown.token.is_requested:
                if not self.registration_succeeded and retry.should_attempt():
                    try:
                        socket.send_multipart([REGISTER_MODULE, self._registration().SerializeToString()])
                        self.metrics.increment("messages_sent")
                    except zmq.Again:
                        pass
                    retry.attempted()
                escalation = retry.update() if not self.registration_succeeded else None
                if escalation is not None:
                    exit_code = escalation.exit_code
                    break
                now = self._monotonic()
                if self.registration_succeeded and now >= next_heartbeat:
                    try:
                        socket.send_multipart([MODULE_HEARTBEAT, self.session_uuid.bytes])
                        self.metrics.increment("messages_sent")
                    except zmq.Again:
                        pass
                    next_heartbeat = now + self.heartbeat_interval_seconds
                events = dict(poller.poll(50))
                if socket not in events:
                    continue
                frames = socket.recv_multipart()
                message = parse_dealer_message(frames)
                if message is None:
                    self.metrics.increment("invalid_messages")
                    continue
                kind, payload = message
                if kind == REGISTER_MODULE_RESPONSE and not self.registration_succeeded:
                    if self._handle_registration_response(payload, retry):
                        next_heartbeat = self._monotonic()
                elif kind == COMMAND_REQUEST and self.registration_succeeded:
                    self._handle_command(socket, payload)
                else:
                    self.metrics.increment("invalid_messages")
        finally:
            if self.state_machine.state in {
                ComponentState.STARTING,
                ComponentState.READY,
                ComponentState.RUNNING,
                ComponentState.DEGRADED,
            }:
                self.shutdown.request("fake module loop stopped")
            if socket is not None:
                socket.close(linger=0)
            context.term()
            if self.state_machine.state is ComponentState.STOPPING:
                result = self.shutdown.run(timeout_seconds=5.0)
                if exit_code is ExitCode.CLEAN_SHUTDOWN:
                    exit_code = result.exit_code
        return exit_code


__all__ = [
    "HEARTBEAT_INTERVAL_SECONDS",
    "REGISTRATION_ACK_TIMEOUT_SECONDS",
    "REGISTRATION_MAX_ATTEMPTS",
    "REGISTRATION_RETRY_SECONDS",
    "STATE_CHANGING_COMMANDS",
    "FakeModuleService",
    "RegistrationRetryController",
    "RegistrationRetryState",
]
