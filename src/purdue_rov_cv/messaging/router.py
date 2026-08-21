"""Real two-sided ROUTER control service."""

from __future__ import annotations

import time
from collections.abc import Callable, Collection
from dataclasses import dataclass
from threading import get_ident
from typing import Protocol
from uuid import UUID

import zmq
from google.protobuf.message import DecodeError
from purdue_rov.cv.v1 import control_pb2, registration_pb2

from purdue_rov_cv.config.models import AppConfig
from purdue_rov_cv.runtime.json_logging import StructuredJsonLogger
from purdue_rov_cv.runtime.metrics import RuntimeMetrics
from purdue_rov_cv.runtime.rate_limit import WarningRateLimiter
from purdue_rov_cv.runtime.shutdown import ShutdownCoordinator, install_signal_handlers
from purdue_rov_cv.runtime.state import ComponentState, ComponentStateMachine
from purdue_rov_cv.wire.errors import ErrorCode
from purdue_rov_cv.wire.identities import validate_dealer_identity
from purdue_rov_cv.wire.validators import (
    validate_command_request,
    validate_command_response,
    validate_module_registration,
)

from .protocol import (
    COMMAND_REQUEST,
    COMMAND_RESPONSE,
    MODULE_HEARTBEAT,
    REGISTER_MODULE,
    REGISTER_MODULE_RESPONSE,
    RouterMessage,
    parse_router_message,
)
from .registry import HEARTBEAT_EXPIRY_SECONDS, ModuleRegistrationRegistry
from .sockets import configure_router


class ReadySignal(Protocol):
    def set(self) -> None: ...


_FINAL_COMMAND_STATUSES = frozenset(
    {
        control_pb2.COMMAND_STATUS_COMPLETED,
        control_pb2.COMMAND_STATUS_REJECTED,
        control_pb2.COMMAND_STATUS_FAILED,
        control_pb2.COMMAND_STATUS_OUTCOME_UNKNOWN,
    }
)


@dataclass(frozen=True)
class PendingRoute:
    client_identity: bytes
    module_id: str
    module_session_id: bytes
    module_routing_identity: bytes
    created_monotonic: float
    command_type: str = ""


def _module_identity_parts(identity: bytes) -> tuple[str, bytes] | None:
    validation = validate_dealer_identity(identity)
    if not validation.valid or validation.identity is None or not validation.identity.startswith("module:"):
        return None
    _, module_id, session_text = validation.identity.split(":", 2)
    return module_id, UUID(session_text).bytes


def _valid_client_identity(identity: bytes) -> bool:
    validation = validate_dealer_identity(identity)
    return validation.valid and validation.identity is not None and validation.identity.startswith("client:")


def _rejected_response(
    request: control_pb2.CommandRequest,
    error_code: ErrorCode,
    message: str,
) -> control_pb2.CommandResponse:
    return control_pb2.CommandResponse(
        command_id=request.command_id,
        target_id=request.target_id,
        status=control_pb2.COMMAND_STATUS_REJECTED,
        error_code=error_code.value,
        message=message,
        response_time_unix_ns=time.time_ns(),
    )


class ControlRouterService:
    """Own the client and module ROUTER sockets in one routing thread."""

    def __init__(
        self,
        client_endpoint: str,
        module_endpoint: str,
        *,
        device_id: str,
        allowed_module_ids: Collection[str],
        heartbeat_expiry_seconds: float = HEARTBEAT_EXPIRY_SECONDS,
        metrics: RuntimeMetrics | None = None,
        logger: StructuredJsonLogger | None = None,
        warning_limiter: WarningRateLimiter | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        ready_signal: ReadySignal | None = None,
        install_signals: bool = False,
    ) -> None:
        self.client_endpoint = client_endpoint
        self.module_endpoint = module_endpoint
        self.device_id = device_id
        self.allowed_module_ids = frozenset(allowed_module_ids)
        self.metrics = metrics or RuntimeMetrics(monotonic=monotonic)
        self.logger = logger
        self.warning_limiter = warning_limiter or WarningRateLimiter(monotonic=monotonic)
        self.state_machine = ComponentStateMachine()
        self.shutdown = ShutdownCoordinator(state_machine=self.state_machine, monotonic=monotonic)
        self.registry = ModuleRegistrationRegistry(
            heartbeat_expiry_seconds=heartbeat_expiry_seconds,
            monotonic=monotonic,
        )
        self._monotonic = monotonic
        self._ready_signal = ready_signal
        self._install_signals = install_signals
        self._owner_thread_id: int | None = None
        self._pending: dict[bytes, PendingRoute] = {}

    @classmethod
    def from_config(
        cls,
        config: AppConfig,
        *,
        heartbeat_expiry_seconds: float = HEARTBEAT_EXPIRY_SECONDS,
        metrics: RuntimeMetrics | None = None,
        logger: StructuredJsonLogger | None = None,
        warning_limiter: WarningRateLimiter | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        install_signals: bool = False,
    ) -> ControlRouterService:
        return cls(
            config.messaging.control.client_endpoint,
            config.messaging.control.module_endpoint,
            device_id=config.device.device_id,
            allowed_module_ids=config.tasks,
            heartbeat_expiry_seconds=heartbeat_expiry_seconds,
            metrics=metrics,
            logger=logger,
            warning_limiter=warning_limiter,
            monotonic=monotonic,
            install_signals=install_signals,
        )

    @property
    def owner_thread_id(self) -> int | None:
        return self._owner_thread_id

    def request_shutdown(self, reason: str = "requested") -> None:
        self.shutdown.request(reason)

    def _invalid(self, event_code: str, message: str, *, context: dict[str, object] | None = None) -> None:
        self.metrics.increment("invalid_messages")
        warning = self.warning_limiter.check((event_code, message))
        if warning.emit and self.logger is not None:
            log_context = dict(context or {})
            log_context["previously_suppressed"] = warning.suppressed_count
            self.logger.log("WARNING", event_code, message, context=log_context)
        elif not warning.emit:
            self.metrics.increment("warnings_suppressed")

    def _send(self, socket: zmq.Socket[bytes], frames: list[bytes]) -> None:
        socket.send_multipart(frames)
        self.metrics.increment("messages_sent")

    def _send_to_peer(
        self,
        socket: zmq.Socket[bytes],
        frames: list[bytes],
        *,
        event_code: str,
    ) -> bool:
        """Keep peer-local backpressure or disconnects from stopping the router."""
        failure: zmq.ZMQError
        try:
            self._send(socket, frames)
        except zmq.Again as caught:
            failure = caught
            detail = "peer send exceeded the configured 200 ms deadline"
        except zmq.ZMQError as caught:
            if caught.errno != zmq.EHOSTUNREACH:
                raise
            failure = caught
            detail = "mandatory routing identity is no longer reachable"
        else:
            return True
        warning = self.warning_limiter.check(event_code)
        if warning.emit and self.logger is not None:
            self.logger.log(
                "WARNING",
                event_code,
                detail,
                exception=failure,
                context={
                    "routing_identity": frames[0],
                    "previously_suppressed": warning.suppressed_count,
                },
            )
        elif not warning.emit:
            self.metrics.increment("warnings_suppressed")
        return False

    def _reply_registration(
        self,
        socket: zmq.Socket[bytes],
        identity: bytes,
        *,
        accepted: bool,
        error_code: ErrorCode | None = None,
        message: str = "",
    ) -> bool:
        response = registration_pb2.ModuleRegistrationResponse(
            accepted=accepted,
            error_code="" if error_code is None else error_code.value,
            message=message,
        )
        return self._send_to_peer(
            socket,
            [identity, REGISTER_MODULE_RESPONSE, response.SerializeToString()],
            event_code="MODULE_REGISTRATION_RESPONSE_SEND_FAILED",
        )

    def _handle_registration(self, socket: zmq.Socket[bytes], message: RouterMessage) -> None:
        identity_parts = _module_identity_parts(message.routing_identity)
        if identity_parts is None:
            self._invalid("CONTROL_IDENTITY_INVALID", "registration used an invalid module identity")
            return
        registration = registration_pb2.ModuleRegistration()
        try:
            registration.ParseFromString(message.payload)
        except (DecodeError, TypeError, ValueError):
            self._invalid("MODULE_REGISTRATION_INVALID", "registration protobuf parsing failed")
            self._reply_registration(
                socket,
                message.routing_identity,
                accepted=False,
                error_code=ErrorCode.INVALID_ENVELOPE,
                message="malformed registration",
            )
            return
        validation = validate_module_registration(registration)
        identity_module_id, identity_session_id = identity_parts
        consistent = (
            identity_module_id == registration.module_id
            and identity_session_id == registration.module_session_id
            and registration.task_id == registration.module_id
            and registration.module_id in self.allowed_module_ids
            and registration.host_device_id == self.device_id
        )
        if not validation.valid or not consistent:
            self._invalid(
                "MODULE_REGISTRATION_INVALID",
                "registration fields or routing identity are inconsistent",
                context={"module_id": registration.module_id},
            )
            self._reply_registration(
                socket,
                message.routing_identity,
                accepted=False,
                error_code=ErrorCode.INVALID_ENVELOPE,
                message="invalid or inconsistent registration",
            )
            return
        decision = self.registry.register(registration, message.routing_identity)
        if not decision.accepted:
            self._invalid(
                "MODULE_REGISTRATION_STALE",
                decision.reason,
                context={"module_id": registration.module_id, "session_uuid": str(UUID(bytes=identity_session_id))},
            )
            self._reply_registration(
                socket,
                message.routing_identity,
                accepted=False,
                error_code=ErrorCode.INVALID_ENVELOPE,
                message=decision.reason,
            )
            return
        self.metrics.increment("messages_received")
        self._reply_registration(socket, message.routing_identity, accepted=True, message="registered")
        if self.logger is not None:
            self.logger.log(
                "INFO",
                "MODULE_REGISTERED",
                "module registration accepted",
                target_id=registration.module_id,
                context={
                    "module_id": registration.module_id,
                    "session_uuid": str(UUID(bytes=registration.module_session_id)),
                    "routing_identity": message.routing_identity.decode("utf-8"),
                    "replaced_session": decision.replaced_session,
                },
            )

    def _handle_heartbeat(self, message: RouterMessage) -> None:
        identity_parts = _module_identity_parts(message.routing_identity)
        if identity_parts is None or len(message.payload) != 16:
            self._invalid("MODULE_HEARTBEAT_INVALID", "heartbeat identity or session is invalid")
            return
        module_id, identity_session_id = identity_parts
        if identity_session_id != message.payload or not self.registry.heartbeat(
            module_id,
            message.payload,
            message.routing_identity,
        ):
            self._invalid(
                "MODULE_HEARTBEAT_STALE",
                "heartbeat does not belong to the current registered session",
                context={"module_id": module_id, "session_uuid": message.payload.hex()},
            )
            return
        self.metrics.increment("messages_received")

    def _reply_client(
        self,
        socket: zmq.Socket[bytes],
        client_identity: bytes,
        response: control_pb2.CommandResponse,
    ) -> bool:
        return self._send_to_peer(
            socket,
            [client_identity, COMMAND_RESPONSE, response.SerializeToString()],
            event_code="CONTROL_CLIENT_RESPONSE_SEND_FAILED",
        )

    def _reject_client(
        self,
        socket: zmq.Socket[bytes],
        client_identity: bytes,
        request: control_pb2.CommandRequest,
        error_code: ErrorCode,
        message: str,
    ) -> None:
        self._reply_client(socket, client_identity, _rejected_response(request, error_code, message))
        if self.logger is not None:
            self.logger.log(
                "INFO",
                "CONTROL_COMMAND_REJECTED",
                message,
                command_id=request.command_id,
                command_type=request.WhichOneof("command"),
                target_id=request.target_id,
                context={"error_code": error_code.value},
            )

    def _handle_client_command(
        self,
        client_socket: zmq.Socket[bytes],
        module_socket: zmq.Socket[bytes],
        message: RouterMessage,
    ) -> None:
        if not _valid_client_identity(message.routing_identity):
            self._invalid("CONTROL_IDENTITY_INVALID", "command used an invalid client identity")
            return
        request = control_pb2.CommandRequest()
        try:
            request.ParseFromString(message.payload)
        except (DecodeError, TypeError, ValueError):
            self._invalid("CONTROL_COMMAND_MALFORMED", "command protobuf parsing failed")
            return
        validation = validate_command_request(request)
        if not validation.valid:
            self._invalid(
                "CONTROL_COMMAND_INVALID",
                "command failed structural validation",
                context={"errors": [error.detail for error in validation.errors]},
            )
            if len(request.command_id) == 16 and request.target_id:
                self._reject_client(
                    client_socket,
                    message.routing_identity,
                    request,
                    ErrorCode.INVALID_COMMAND,
                    "invalid command structure",
                )
            return
        self.metrics.increment("messages_received")
        command_type = request.WhichOneof("command")
        assert command_type is not None
        if bytes(request.command_id) in self._pending:
            self._reject_client(
                client_socket,
                message.routing_identity,
                request,
                ErrorCode.DUPLICATE_COMMAND_ID,
                "command ID is already pending; use get_command_status",
            )
            return
        record = self.registry.resolve(request.target_id)
        if record is None or not record.available:
            self._reject_client(
                client_socket,
                message.routing_identity,
                request,
                ErrorCode.TARGET_UNAVAILABLE,
                "target is unavailable",
            )
            return
        if command_type not in record.supported_commands:
            self._reject_client(
                client_socket,
                message.routing_identity,
                request,
                ErrorCode.INVALID_COMMAND,
                "target does not advertise this command",
            )
            return
        try:
            self._send(module_socket, [record.routing_identity, COMMAND_REQUEST, message.payload])
        except zmq.Again:
            self._reject_client(
                client_socket,
                message.routing_identity,
                request,
                ErrorCode.TARGET_SEND_TIMEOUT,
                "target send exceeded 200 ms",
            )
            return
        except zmq.ZMQError as error:
            if error.errno != zmq.EHOSTUNREACH:
                raise
            self.registry.mark_unavailable(record.module_id, record.session_id)
            self._reject_client(
                client_socket,
                message.routing_identity,
                request,
                ErrorCode.TARGET_UNAVAILABLE,
                "target routing identity is unavailable",
            )
            return
        self._pending[bytes(request.command_id)] = PendingRoute(
            client_identity=message.routing_identity,
            module_id=record.module_id,
            module_session_id=record.session_id,
            module_routing_identity=record.routing_identity,
            created_monotonic=self._monotonic(),
            command_type=command_type,
        )
        if self.logger is not None:
            self.logger.log(
                "INFO",
                "CONTROL_COMMAND_FORWARDED",
                "command forwarded to current module session",
                command_id=request.command_id,
                command_type=command_type,
                target_id=request.target_id,
            )

    def _handle_module_response(
        self,
        client_socket: zmq.Socket[bytes],
        message: RouterMessage,
    ) -> None:
        identity_parts = _module_identity_parts(message.routing_identity)
        response = control_pb2.CommandResponse()
        try:
            response.ParseFromString(message.payload)
        except (DecodeError, TypeError, ValueError):
            self._invalid("CONTROL_RESPONSE_MALFORMED", "response protobuf parsing failed")
            return
        validation = validate_command_response(response)
        pending = self._pending.get(bytes(response.command_id))
        if identity_parts is None or not validation.valid or pending is None:
            self._invalid("CONTROL_RESPONSE_INVALID", "response is invalid or has no pending client route")
            return
        module_id, session_id = identity_parts
        if (
            module_id != pending.module_id
            or response.target_id != pending.module_id
            or session_id != pending.module_session_id
            or message.routing_identity != pending.module_routing_identity
            or not self.registry.is_current_session(module_id, session_id, message.routing_identity)
        ):
            self._invalid("CONTROL_RESPONSE_STALE", "response came from a stale or inconsistent module session")
            return
        self.metrics.increment("messages_received")
        self._reply_client(client_socket, pending.client_identity, response)
        if pending.command_type == "get_command_status" or response.status in _FINAL_COMMAND_STATUSES:
            self._pending.pop(bytes(response.command_id), None)

    def _receive_module(self, client_socket: zmq.Socket[bytes], module_socket: zmq.Socket[bytes]) -> None:
        frames = module_socket.recv_multipart()
        message = parse_router_message(frames)
        if message is None:
            self._invalid("CONTROL_FRAMING_INVALID", "module message must contain identity, kind, and payload")
            return
        if message.kind == REGISTER_MODULE:
            self._handle_registration(module_socket, message)
        elif message.kind == MODULE_HEARTBEAT:
            self._handle_heartbeat(message)
        elif message.kind == COMMAND_RESPONSE:
            self._handle_module_response(client_socket, message)
        else:
            self._invalid("CONTROL_MESSAGE_KIND_INVALID", "unknown module message kind")

    def _receive_client(self, client_socket: zmq.Socket[bytes], module_socket: zmq.Socket[bytes]) -> None:
        frames = client_socket.recv_multipart()
        message = parse_router_message(frames)
        if message is None or message.kind != COMMAND_REQUEST:
            self._invalid("CONTROL_FRAMING_INVALID", "client message must contain identity, command kind, and payload")
            return
        self._handle_client_command(client_socket, module_socket, message)

    def _expire_pending(self) -> None:
        cutoff = self._monotonic() - 60.0
        for command_id, pending in tuple(self._pending.items()):
            if pending.created_monotonic <= cutoff:
                del self._pending[command_id]

    def run(self) -> None:
        """Bind both ROUTER sockets and poll them in this calling thread."""
        self._owner_thread_id = get_ident()
        if self._install_signals:
            install_signal_handlers(self.shutdown)
        context = zmq.Context()
        client_socket: zmq.Socket[bytes] | None = None
        module_socket: zmq.Socket[bytes] | None = None
        try:
            client_socket = context.socket(zmq.ROUTER)
            module_socket = context.socket(zmq.ROUTER)
            configure_router(client_socket, self.client_endpoint)
            configure_router(module_socket, self.module_endpoint)
            client_socket.bind(self.client_endpoint)
            module_socket.bind(self.module_endpoint)
            self.state_machine.transition_to(ComponentState.READY)
            self.state_machine.transition_to(ComponentState.RUNNING)
            if self._ready_signal is not None:
                self._ready_signal.set()
            poller = zmq.Poller()
            poller.register(client_socket, zmq.POLLIN)
            poller.register(module_socket, zmq.POLLIN)
            while not self.shutdown.token.is_requested:
                events = dict(poller.poll(100))
                if module_socket in events:
                    self._receive_module(client_socket, module_socket)
                if client_socket in events:
                    self._receive_client(client_socket, module_socket)
                self.registry.expire()
                self._expire_pending()
        finally:
            if self.state_machine.state in {
                ComponentState.STARTING,
                ComponentState.READY,
                ComponentState.RUNNING,
                ComponentState.DEGRADED,
            }:
                self.shutdown.request("control router loop stopped")
            if module_socket is not None:
                module_socket.close(linger=0)
            if client_socket is not None:
                client_socket.close(linger=0)
            context.term()
            if self.state_machine.state is ComponentState.STOPPING:
                self.shutdown.run(timeout_seconds=5.0)


__all__ = ["ControlRouterService", "PendingRoute", "ReadySignal"]
