"""Reusable production runner for one configured CV task instance."""

from __future__ import annotations

import os
import time
from collections.abc import Callable
from dataclasses import dataclass
from threading import Event, Lock, Thread
from typing import cast
from uuid import UUID, uuid4

import zmq
from google.protobuf.json_format import MessageToDict
from google.protobuf.message import DecodeError, Message
from purdue_rov.cv.v1 import control_pb2, registration_pb2
from pydantic import ValidationError

from purdue_rov_cv.config.models import (
    AppConfig,
    DebugSnapshotsConfig,
    DiagnosticsConfig,
    DynamicTaskConfig,
    TaskConfig,
)
from purdue_rov_cv.config.policy import ChangeClass, classify_field_path
from purdue_rov_cv.messaging.cache import CommandReservationStatus, CommandStatusCache
from purdue_rov_cv.messaging.fake_module import (
    HEARTBEAT_INTERVAL_SECONDS,
    REGISTRATION_ACK_TIMEOUT_SECONDS,
    REGISTRATION_MAX_ATTEMPTS,
    REGISTRATION_RETRY_SECONDS,
    STATE_CHANGING_COMMANDS,
    RegistrationRetryController,
)
from purdue_rov_cv.messaging.protocol import (
    COMMAND_REQUEST,
    COMMAND_RESPONSE,
    MODULE_HEARTBEAT,
    REGISTER_MODULE,
    REGISTER_MODULE_RESPONSE,
    parse_dealer_message,
)
from purdue_rov_cv.messaging.sockets import configure_dealer, module_identity
from purdue_rov_cv.modules.base import CVModule, DynamicConfig, Frame, ModuleContext, NativeValue
from purdue_rov_cv.runtime.exit_codes import EscalationRequest, ExitCode
from purdue_rov_cv.runtime.json_logging import StructuredJsonLogger
from purdue_rov_cv.runtime.metrics import RuntimeMetrics
from purdue_rov_cv.runtime.publisher import PublisherSequence
from purdue_rov_cv.runtime.queues import (
    ControlCommandQueue,
    ControlResultQueue,
    CvResultQueue,
    FrameInputQueue,
    QueueEvent,
    ReceiveStatus,
)
from purdue_rov_cv.runtime.shutdown import ShutdownCoordinator, install_signal_handlers
from purdue_rov_cv.runtime.state import ComponentState, ComponentStateMachine, to_wire_component_state
from purdue_rov_cv.wire.errors import ErrorCode
from purdue_rov_cv.wire.payloads import PAYLOAD_REGISTRY
from purdue_rov_cv.wire.validators import (
    validate_command_request,
    validate_module_registration_response,
)

from .frame_source import FrameSource, FrameSourceInvalid
from .publisher import PublicationItem, ResultPublisher
from .supervision import ProcessingSupervisor, WorkerWatchdog


@dataclass(frozen=True)
class RunnerSettings:
    registration_retry_seconds: float = REGISTRATION_RETRY_SECONDS
    registration_ack_timeout_seconds: float = REGISTRATION_ACK_TIMEOUT_SECONDS
    registration_max_attempts: int = REGISTRATION_MAX_ATTEMPTS
    heartbeat_interval_seconds: float = HEARTBEAT_INTERVAL_SECONDS
    frame_attach_retry_seconds: float = 0.100
    watchdog_minimum_seconds: float = 10.0
    control_poll_ms: int = 50

    def __post_init__(self) -> None:
        numeric = (
            self.registration_retry_seconds,
            self.registration_ack_timeout_seconds,
            self.heartbeat_interval_seconds,
            self.frame_attach_retry_seconds,
            self.watchdog_minimum_seconds,
        )
        if any(value <= 0 for value in numeric) or self.registration_max_attempts <= 0:
            raise ValueError("runner timing settings must be positive")
        if not 1 <= self.control_poll_ms <= 250:
            raise ValueError("control_poll_ms must be between 1 and 250")


class ModuleInitializationError(RuntimeError):
    """Module initialization failed before runtime threads could start."""

    def __init__(self, error: Exception, *, artifact_related: bool) -> None:
        self.original = error
        self.artifact_related = artifact_related
        super().__init__(f"{type(error).__name__}: {error}")


@dataclass(frozen=True)
class _DynamicUpdate:
    task: TaskConfig
    diagnostics: DiagnosticsConfig
    debug_snapshots: DebugSnapshotsConfig
    module_values: DynamicConfig | None
    rollback_values: DynamicConfig | None


RUNNER_SUPPORTED_COMMANDS = frozenset(
    {
        "get_status",
        "start",
        "stop",
        "set_dynamic_config",
        "reset",
        "get_command_status",
    }
)


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


class ModuleRunnerService:
    """Compose Phase 1-4 contracts around exactly one ``CVModule`` instance.

    Thread ownership is fixed: frame ingress owns the shared-memory attachment,
    the worker alone calls all module hooks and ``process()``, the publisher owns
    PUB, and the calling/main thread owns DEALER plus watchdog evaluation.
    """

    def __init__(
        self,
        config: AppConfig,
        task_id: str,
        module: CVModule,
        frame_source: FrameSource,
        *,
        settings: RunnerSettings | None = None,
        session_uuid: UUID | None = None,
        metrics: RuntimeMetrics | None = None,
        logger: StructuredJsonLogger | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        monotonic_ns: Callable[[], int] = time.monotonic_ns,
        publisher_sequence: PublisherSequence | None = None,
        install_signals: bool = False,
    ) -> None:
        if task_id not in config.tasks:
            raise ValueError(f"unknown task: {task_id}")
        task = config.tasks[task_id]
        if not task.enabled:
            raise ValueError(f"task is disabled: {task_id}")
        self.config = config
        self.task_id = task_id
        self.module_id = task_id
        self.task = task
        self.module = module
        self.frame_source = frame_source
        self.settings = settings or RunnerSettings()
        self.session_uuid = session_uuid or uuid4()
        self.identity = module_identity(self.module_id, self.session_uuid)
        self.metrics = metrics or RuntimeMetrics(monotonic=monotonic)
        self.metrics.set_metadata("state", ComponentState.STARTING.value)
        self.logger = logger
        self.state_machine = ComponentStateMachine(observer=self._observe_state)
        self.shutdown = ShutdownCoordinator(state_machine=self.state_machine, monotonic=monotonic)
        self.cache = CommandStatusCache(monotonic=monotonic)
        self.frame_queue = FrameInputQueue[Frame](metrics=self.metrics, copy_item=Frame.private_copy)
        self.result_queue = CvResultQueue[PublicationItem](metrics=self.metrics)
        self.command_queue = ControlCommandQueue[control_pb2.CommandRequest](metrics=self.metrics)
        self.result_control_queue = ControlResultQueue[control_pb2.CommandResponse](
            cache_result=self._cache_control_result,
            event=self._queue_event,
            escalate=self._request_escalation,
            metrics=self.metrics,
        )
        self._monotonic = monotonic
        self._input_exists = Event()
        self._first_frame = Event()
        self._publisher_ready = Event()
        self._initialization_complete = Event()
        self._initialization_error: Exception | None = None
        self._registration_succeeded = False
        self._initialized = False
        self._active = False
        self._escalation: EscalationRequest | None = None
        self._escalation_lock = Lock()
        self._settings_lock = Lock()
        self._active_diagnostics = config.diagnostics
        self._active_debug_snapshots = config.debug_snapshots
        self._threads: list[Thread] = []
        self._install_signals = install_signals
        self.execution_counts: dict[str, int] = {}
        self.watchdog = WorkerWatchdog(
            task.processing_deadline_ms,
            monotonic_ns=monotonic_ns,
            minimum_seconds=self.settings.watchdog_minimum_seconds,
        )
        self.processing = ProcessingSupervisor(
            task.processing_deadline_ms,
            metrics=self.metrics,
            state_machine=self.state_machine,
            escalate=self._request_escalation,
        )
        self._monotonic_ns = monotonic_ns
        self.publisher_sequence = publisher_sequence or PublisherSequence()
        self.publisher_sequence_id = self.publisher_sequence.session_id

    @property
    def registration_succeeded(self) -> bool:
        return self._registration_succeeded

    @property
    def escalation(self) -> EscalationRequest | None:
        with self._escalation_lock:
            return self._escalation

    def _observe_state(self, result: object) -> None:
        del result
        self.metrics.set_metadata("state", self.state_machine.state.value)

    def _log(self, level: str, event_code: str, message: str, **fields: object) -> None:
        if self.logger is not None:
            self.logger.log(level, event_code, message, context=fields)

    def _record_error(self, error_code: ErrorCode, message: str) -> None:
        self.metrics.set_metadata("last_error_code", error_code.value)
        self.metrics.set_metadata("last_error_message", message)

    def _current_max_input_fps(self) -> int:
        with self._settings_lock:
            return self.task.max_input_fps

    def _current_health_interval_ms(self) -> int:
        with self._settings_lock:
            return self._active_diagnostics.publish_interval_ms

    def _queue_event(self, event: QueueEvent) -> None:
        self._log(event.level, event.event_code, event.message, **event.context)

    def _cache_control_result(self, response: control_pb2.CommandResponse) -> None:
        self.cache.put(response)

    def _request_escalation(self, escalation: EscalationRequest) -> None:
        with self._escalation_lock:
            if self._escalation is None:
                self._escalation = escalation
                first = True
            else:
                first = False
        if first:
            try:
                self._record_error(ErrorCode(escalation.event_code), escalation.reason)
            except ValueError:
                pass
            self._log("ERROR", escalation.event_code, escalation.reason)
            self.shutdown.request(escalation.reason)

    def request_shutdown(self, reason: str = "requested") -> None:
        self.shutdown.request(reason)

    def _thread_entry(self, name: str, target: Callable[[], None]) -> None:
        try:
            target()
        except Exception as error:
            self._log("ERROR", "RUNNER_THREAD_FAILED", f"{name} thread failed", exception=repr(error))
            self._request_escalation(
                EscalationRequest(ExitCode.TEMPORARY_FAILURE, f"{name} thread failed", "RUNNER_THREAD_FAILED")
            )

    def _start_thread(self, name: str, target: Callable[[], None]) -> None:
        thread = Thread(
            target=self._thread_entry, args=(name, target), name=f"module:{self.module_id}:{name}", daemon=True
        )
        self._threads.append(thread)
        thread.start()

    def _module_context(self) -> ModuleContext:
        return ModuleContext(
            self.module_id,
            self.task_id,
            self.config.device.device_id,
            self.task.input_camera,
            self.task,
        )

    def _frame_ingress(self) -> None:
        next_allowed = 0.0
        try:
            while not self.shutdown.token.is_requested:
                if self.state_machine.state is ComponentState.ERROR:
                    self.shutdown.token.wait(0.050)
                    continue
                if not self.frame_source.attached:
                    if not self.frame_source.attach():
                        self.shutdown.token.wait(self.settings.frame_attach_retry_seconds)
                        continue
                    self._input_exists.set()
                frame = self.frame_source.read(0.250)
                if frame is None:
                    continue
                now = self._monotonic()
                if now < next_allowed:
                    continue
                next_allowed = now + 1.0 / self._current_max_input_fps()
                self.metrics.increment("frames_read")
                self.frame_queue.offer_owned(frame)
                self._first_frame.set()
        except FrameSourceInvalid as error:
            if self.state_machine.state not in {ComponentState.ERROR, ComponentState.STOPPING, ComponentState.STOPPED}:
                self.state_machine.transition_to(ComponentState.ERROR)
            self._record_error(ErrorCode.SHARED_MEMORY_INVALID, str(error))
            self._log("ERROR", "SHARED_MEMORY_INVALID", str(error))
            self._request_escalation(EscalationRequest(ExitCode.TEMPORARY_FAILURE, str(error), "SHARED_MEMORY_INVALID"))
        finally:
            self.frame_source.close()

    @staticmethod
    def _integral_struct_number(value: NativeValue, path: str) -> NativeValue:
        if isinstance(value, float) and value.is_integer():
            return int(value)
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError(f"{path} must be an integer")
        return value

    def _dynamic_update(self, request: control_pb2.CommandRequest) -> _DynamicUpdate:
        converted = MessageToDict(request.set_dynamic_config.fields)
        plain = cast(dict[str, NativeValue], converted)
        flattened: dict[str, NativeValue] = {}
        for path, value in plain.items():
            if path in {"dynamic", "diagnostics", "debug_snapshots"} and isinstance(value, dict):
                for child, child_value in value.items():
                    flattened[f"{path}.{child}"] = child_value
            else:
                flattened[path] = value

        with self._settings_lock:
            current_task = self.task
            current_diagnostics = self._active_diagnostics
            current_debug = self._active_debug_snapshots
        task_values = current_task.model_dump(mode="python")
        diagnostics_values = current_diagnostics.model_dump(mode="python")
        debug_values = current_debug.model_dump(mode="python")
        task_dynamic_changed = False
        debug_changed = False

        for received_path, received_value in flattened.items():
            path = received_path
            task_prefix = f"tasks.{self.task_id}."
            if path.startswith(task_prefix):
                path = path.removeprefix(task_prefix)
            elif path.startswith("tasks."):
                raise ValueError(f"runtime update targets another task: {received_path}")
            if path in DynamicTaskConfig.model_fields:
                path = f"dynamic.{path}"

            if path == "max_input_fps" or path.startswith("dynamic."):
                canonical_path = f"tasks.{self.task_id}.{path}"
            elif path.startswith("diagnostics.") or path.startswith("debug_snapshots."):
                canonical_path = path
            else:
                canonical_path = f"tasks.{self.task_id}.{path}"
            classification = classify_field_path(canonical_path)
            if classification is ChangeClass.STATIC:
                raise PermissionError(f"runtime update is static: {received_path}")
            if classification is not ChangeClass.DYNAMIC:
                raise ValueError(f"runtime update is unsupported: {received_path}")

            if path == "max_input_fps":
                task_values["max_input_fps"] = self._integral_struct_number(received_value, path)
            elif path.startswith("dynamic."):
                field = path.removeprefix("dynamic.")
                task_values["dynamic"][field] = received_value
                task_dynamic_changed = True
            elif path == "diagnostics.publish_interval_ms":
                diagnostics_values["publish_interval_ms"] = self._integral_struct_number(received_value, path)
            elif path.startswith("debug_snapshots."):
                field = path.removeprefix("debug_snapshots.")
                if field == "jpeg_quality":
                    received_value = self._integral_struct_number(received_value, path)
                debug_values[field] = received_value
                debug_changed = True
            else:  # guarded by the canonical policy above
                raise ValueError(f"runtime update has no Phase 5 owner: {received_path}")

        proposed_task = TaskConfig.model_validate(task_values)
        proposed_diagnostics = DiagnosticsConfig.model_validate(diagnostics_values)
        proposed_debug = DebugSnapshotsConfig.model_validate(debug_values)
        module_values: dict[str, NativeValue] = {}
        rollback_values: dict[str, NativeValue] = {}
        if task_dynamic_changed:
            module_values.update(cast(DynamicConfig, proposed_task.dynamic.model_dump(mode="python")))
            rollback_values.update(cast(DynamicConfig, current_task.dynamic.model_dump(mode="python")))
        if debug_changed:
            dynamic_debug_fields = ("enabled", "maximum_rate_hz", "jpeg_quality")
            module_values["debug_snapshots"] = {field: getattr(proposed_debug, field) for field in dynamic_debug_fields}
            rollback_values["debug_snapshots"] = {
                field: getattr(current_debug, field) for field in dynamic_debug_fields
            }
        return _DynamicUpdate(
            proposed_task,
            proposed_diagnostics,
            proposed_debug,
            module_values if module_values else None,
            rollback_values if rollback_values else None,
        )

    def _apply_dynamic_update(self, update: _DynamicUpdate) -> None:
        if update.module_values is not None:
            try:
                self.module.apply_dynamic_config(update.module_values)
            except Exception as apply_error:
                try:
                    assert update.rollback_values is not None
                    self.module.apply_dynamic_config(update.rollback_values)
                except Exception as rollback_error:
                    raise RuntimeError(
                        f"dynamic configuration apply failed and rollback failed: "
                        f"{type(rollback_error).__name__}: {rollback_error}"
                    ) from apply_error
                raise
        with self._settings_lock:
            self.task = update.task
            self._active_diagnostics = update.diagnostics
            self._active_debug_snapshots = update.debug_snapshots

    def _execute_command(self, request: control_pb2.CommandRequest) -> control_pb2.CommandResponse:
        command_type = request.WhichOneof("command")
        assert command_type is not None
        self.execution_counts[command_type] = self.execution_counts.get(command_type, 0) + 1
        try:
            if command_type == "start":
                if self.processing.requires_restart:
                    return _response(
                        request,
                        control_pb2.COMMAND_STATUS_REJECTED,
                        state=self.state_machine.state,
                        error_code=ErrorCode.RESTART_REQUIRED,
                        message="a static configuration update requires process restart",
                    )
                if self.state_machine.state is not ComponentState.READY:
                    return _response(
                        request,
                        control_pb2.COMMAND_STATUS_REJECTED,
                        state=self.state_machine.state,
                        error_code=ErrorCode.INVALID_STATE_TRANSITION,
                        message="start is valid only from READY",
                    )
                transition = self.state_machine.transition_to(ComponentState.RUNNING)
                if not transition.accepted:
                    return _response(
                        request,
                        control_pb2.COMMAND_STATUS_REJECTED,
                        state=self.state_machine.state,
                        error_code=ErrorCode.INVALID_STATE_TRANSITION,
                        message=transition.detail,
                    )
                try:
                    self.module.on_start()
                except Exception as error:
                    raise RuntimeError(f"on_start failed: {type(error).__name__}: {error}") from error
                self._active = True
            elif command_type == "stop":
                transition = self.state_machine.transition_to(ComponentState.READY)
                if not transition.accepted:
                    return _response(
                        request,
                        control_pb2.COMMAND_STATUS_REJECTED,
                        state=self.state_machine.state,
                        error_code=ErrorCode.INVALID_STATE_TRANSITION,
                        message=transition.detail,
                    )
                try:
                    self.module.on_stop()
                except Exception as error:
                    raise RuntimeError(f"on_stop failed: {type(error).__name__}: {error}") from error
                self._active = False
            elif command_type == "reset":
                transition = self.state_machine.reset_from_error()
                if not transition.accepted:
                    return _response(
                        request,
                        control_pb2.COMMAND_STATUS_REJECTED,
                        state=self.state_machine.state,
                        error_code=ErrorCode.INVALID_STATE_TRANSITION,
                        message=transition.detail,
                    )
                if self._active:
                    try:
                        self.module.on_stop()
                    except Exception as error:
                        raise RuntimeError(f"on_stop during reset failed: {type(error).__name__}: {error}") from error
                self.processing.reset()
                self._active = False
                self.metrics.set_metadata("last_error_code", None)
                self.metrics.set_metadata("last_error_message", None)
            elif command_type == "set_dynamic_config":
                self._apply_dynamic_update(self._dynamic_update(request))
            else:
                return _response(
                    request,
                    control_pb2.COMMAND_STATUS_REJECTED,
                    state=self.state_machine.state,
                    error_code=ErrorCode.INVALID_COMMAND,
                    message=f"{command_type} is not implemented by this task",
                )
        except PermissionError as error:
            self.processing.record_external_degradation()
            self._record_error(ErrorCode.RESTART_REQUIRED, str(error))
            return _response(
                request,
                control_pb2.COMMAND_STATUS_REJECTED,
                state=self.state_machine.state,
                error_code=ErrorCode.RESTART_REQUIRED,
                message=str(error),
            )
        except (ValidationError, ValueError, TypeError) as error:
            return _response(
                request,
                control_pb2.COMMAND_STATUS_REJECTED,
                state=self.state_machine.state,
                error_code=ErrorCode.INVALID_COMMAND,
                message=str(error),
            )
        except Exception as error:
            if self.state_machine.state not in {ComponentState.ERROR, ComponentState.STOPPING, ComponentState.STOPPED}:
                self.state_machine.transition_to(ComponentState.ERROR)
            self._record_error(
                ErrorCode.INTERNAL_ERROR, f"module lifecycle hook failed: {type(error).__name__}: {error}"
            )
            return _response(
                request,
                control_pb2.COMMAND_STATUS_FAILED,
                state=self.state_machine.state,
                error_code=ErrorCode.INTERNAL_ERROR,
                message=f"module lifecycle hook failed: {type(error).__name__}: {error}",
            )
        return _response(
            request,
            control_pb2.COMMAND_STATUS_COMPLETED,
            state=self.state_machine.state,
            message=f"executed {command_type} count={self.execution_counts[command_type]}",
        )

    def _process_frame(self, frame: Frame) -> None:
        started_ns = self._monotonic_ns()
        try:
            outputs = self.module.process(frame)
            if not isinstance(outputs, list) or any(not isinstance(payload, Message) for payload in outputs):
                raise TypeError("CVModule.process() must return list[Message]")
            payload_spec = PAYLOAD_REGISTRY[self.task.payload_type]
            if any(not isinstance(payload, payload_spec.message_class) for payload in outputs):
                raise TypeError(f"CVModule output does not match payload_type={self.task.payload_type}")
        except Exception as error:
            self.processing.record_exception()
            self._record_error(ErrorCode.PROCESSING_FAILURE, f"{type(error).__name__}: {error}")
            if self.logger is not None:
                self.logger.log(
                    "ERROR",
                    "MODULE_PROCESSING_EXCEPTION",
                    "module failed to process a frame; result dropped",
                    camera_id=frame.camera_id,
                    camera_session_id=frame.camera_session_id,
                    frame_number=frame.frame_number,
                    exception=error,
                    context={"task_id": self.task_id, "module_id": self.module_id},
                )
            return
        duration_ns = self._monotonic_ns() - started_ns
        self.processing.record_success(duration_ns)
        for payload in outputs:
            self.result_queue.offer(PublicationItem(payload, frame))

    def _initialize_module(self) -> None:
        try:
            self.module.initialize(self._module_context())
        except Exception as error:
            self._initialization_error = error
            if self.state_machine.state not in {ComponentState.STOPPING, ComponentState.STOPPED}:
                self.state_machine.transition_to(ComponentState.ERROR)
            error_code = ErrorCode.MODEL_LOAD_FAILED if self.module.requires_artifact else ErrorCode.INTERNAL_ERROR
            self._record_error(error_code, f"{type(error).__name__}: {error}")
            self._log("ERROR", error_code.value, f"module initialization failed: {type(error).__name__}: {error}")
        else:
            self._initialized = True
        finally:
            self._initialization_complete.set()

    def _worker(self) -> None:
        try:
            self._initialize_module()
            if not self._initialized:
                return
            while not self.shutdown.token.is_requested:
                self.watchdog.progress()
                command = self.command_queue.receive(timeout_seconds=0.0, shutdown=self.shutdown.token)
                if command.status is ReceiveStatus.ITEM:
                    assert command.item is not None
                    response = self._execute_command(command.item)
                    if self.logger is not None:
                        self.logger.log(
                            "INFO" if response.status == control_pb2.COMMAND_STATUS_COMPLETED else "WARNING",
                            "MODULE_COMMAND_RESULT",
                            response.message or "module command completed",
                            command_id=command.item.command_id,
                            command_type=command.item.WhichOneof("command"),
                            target_id=command.item.target_id,
                            context={
                                "status": control_pb2.CommandStatus.Name(response.status),
                                "error_code": response.error_code,
                            },
                        )
                    self.cache.put(response)
                    self.result_control_queue.offer(response)
                    self.watchdog.progress()
                    continue
                if self.state_machine.state not in {ComponentState.RUNNING, ComponentState.DEGRADED}:
                    self.shutdown.token.wait(0.050)
                    self.watchdog.progress()
                    continue
                received = self.frame_queue.receive(timeout_seconds=0.250, shutdown=self.shutdown.token)
                if received.status is ReceiveStatus.ITEM:
                    assert received.item is not None
                    self._process_frame(received.item)
                self.watchdog.progress()
        finally:
            try:
                if self._active:
                    self.module.on_stop()
                    self._active = False
            finally:
                try:
                    self.module.shutdown()
                except Exception as error:
                    message = f"module shutdown failed: {type(error).__name__}: {error}"
                    self._record_error(ErrorCode.INTERNAL_ERROR, message)
                    self._request_escalation(
                        EscalationRequest(ExitCode.INTERNAL_SOFTWARE_FAILURE, message, ErrorCode.INTERNAL_ERROR.value)
                    )

    def _registration(self) -> registration_pb2.ModuleRegistration:
        return registration_pb2.ModuleRegistration(
            module_id=self.module_id,
            task_id=self.task_id,
            module_session_id=self.session_uuid.bytes,
            supported_command_types=sorted(RUNNER_SUPPORTED_COMMANDS),
            current_state=to_wire_component_state(self.state_machine.state),
            process_id=os.getpid(),
            host_device_id=self.config.device.device_id,
        )

    def _status_lookup(self, request: control_pb2.CommandRequest) -> control_pb2.CommandResponse:
        cached = self.cache.get(bytes(request.get_command_status.target_command_id))
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

    def _send_control_response(self, socket: zmq.Socket[bytes], response: control_pb2.CommandResponse) -> None:
        try:
            socket.send_multipart([COMMAND_RESPONSE, response.SerializeToString()])
        except zmq.Again as error:
            self._log(
                "WARNING",
                "CONTROL_RESPONSE_SEND_TIMEOUT",
                "command outcome retained in cache after response timeout",
                command_id=response.command_id.hex(),
                exception=repr(error),
            )
            return
        self.metrics.increment("messages_sent")

    def _handle_command(self, socket: zmq.Socket[bytes], payload: bytes) -> None:
        request = control_pb2.CommandRequest()
        try:
            request.ParseFromString(payload)
        except (DecodeError, TypeError, ValueError):
            self.metrics.increment("invalid_messages")
            return
        validation = validate_command_request(request)
        if not validation.valid or request.target_id != self.module_id:
            self.metrics.increment("invalid_messages")
            self._send_control_response(
                socket,
                _response(
                    request,
                    control_pb2.COMMAND_STATUS_REJECTED,
                    state=self.state_machine.state,
                    error_code=ErrorCode.INVALID_COMMAND,
                    message="invalid command",
                ),
            )
            return
        self.metrics.increment("messages_received")
        command_type = request.WhichOneof("command")
        assert command_type is not None
        if command_type == "get_status":
            self._send_control_response(
                socket,
                _response(
                    request,
                    control_pb2.COMMAND_STATUS_COMPLETED,
                    state=self.state_machine.state,
                    message="module status",
                ),
            )
            return
        if command_type == "get_command_status":
            self._send_control_response(socket, self._status_lookup(request))
            return
        if command_type in STATE_CHANGING_COMMANDS:
            received = _response(
                request,
                control_pb2.COMMAND_STATUS_RECEIVED,
                state=self.state_machine.state,
                message="command reserved for execution",
            )
            reservation = self.cache.try_reserve(received)
            if reservation is CommandReservationStatus.DUPLICATE:
                self._send_control_response(
                    socket,
                    _response(
                        request,
                        control_pb2.COMMAND_STATUS_REJECTED,
                        state=self.state_machine.state,
                        error_code=ErrorCode.DUPLICATE_COMMAND_ID,
                        message="command ID already known; use get_command_status",
                    ),
                )
                return
            if reservation is CommandReservationStatus.CAPACITY_FULL:
                self._send_control_response(
                    socket,
                    _response(
                        request,
                        control_pb2.COMMAND_STATUS_REJECTED,
                        state=self.state_machine.state,
                        error_code=ErrorCode.MODULE_BUSY,
                        message="command-status cache is full of active commands",
                    ),
                )
                return
        offered = self.command_queue.offer(request)
        if not offered.accepted:
            busy = _response(
                request,
                control_pb2.COMMAND_STATUS_REJECTED,
                state=self.state_machine.state,
                error_code=ErrorCode.MODULE_BUSY,
                message="module command queue is full",
            )
            if len(request.command_id) == 16:
                self.cache.put(busy)
            self._send_control_response(socket, busy)
            return
        if command_type in STATE_CHANGING_COMMANDS:
            pending = self.cache.get(bytes(request.command_id))
            assert pending is not None
            self._send_control_response(socket, pending)

    def _handle_registration(self, payload: bytes, retry: RegistrationRetryController) -> None:
        response = registration_pb2.ModuleRegistrationResponse()
        try:
            response.ParseFromString(payload)
        except (DecodeError, TypeError, ValueError):
            self.metrics.increment("invalid_messages")
            return
        if not validate_module_registration_response(response).valid or not response.accepted:
            self.metrics.increment("invalid_messages")
            return
        retry.acknowledged()
        self._registration_succeeded = True
        self.metrics.increment("messages_received")

    def _update_readiness(self) -> None:
        if (
            self.state_machine.state is ComponentState.STARTING
            and self._initialized
            and self._registration_succeeded
            and self._input_exists.is_set()
            and self._first_frame.is_set()
        ):
            self.state_machine.transition_to(ComponentState.READY)

    def run(self) -> ExitCode:
        if self._install_signals:
            install_signal_handlers(self.shutdown)
        self._start_thread("worker", self._worker)
        while not self._initialization_complete.wait(0.050) and not self.shutdown.token.is_requested:
            pass
        if self._initialization_error is not None:
            self._threads[0].join(1.0)
            raise ModuleInitializationError(
                self._initialization_error,
                artifact_related=self.module.requires_artifact,
            ) from self._initialization_error
        if not self._initialized:
            self._threads[0].join(1.0)
            if self._threads[0].is_alive():
                self._request_escalation(
                    EscalationRequest(
                        ExitCode.TEMPORARY_FAILURE,
                        "module initialization did not stop after shutdown",
                        "SHUTDOWN_TIMEOUT",
                    )
                )
            self.shutdown.run(timeout_seconds=0.0)
            escalation = self.escalation
            return ExitCode.CLEAN_SHUTDOWN if escalation is None else escalation.exit_code
        context = zmq.Context()
        socket: zmq.Socket[bytes] | None = None
        publisher = ResultPublisher(
            self.config.messaging.broker.publisher_endpoint,
            topic=self.task.publish_topic,
            payload_type=self.task.payload_type,
            task_id=self.task_id,
            module_id=self.module_id,
            device_id=self.config.device.device_id,
            health_interval_ms=self.config.diagnostics.publish_interval_ms,
            queue=self.result_queue,
            metrics=self.metrics,
            state_machine=self.state_machine,
            shutdown=self.shutdown.token,
            context=context,
            sequence=self.publisher_sequence,
            ready=self._publisher_ready,
            health_interval_ms_getter=self._current_health_interval_ms,
            logger=self.logger,
        )
        try:
            self._start_thread("frame-ingress", self._frame_ingress)
            self._start_thread("publisher", publisher.run)
            socket = context.socket(zmq.DEALER)
            configure_dealer(socket, self.identity)
            socket.connect(self.config.messaging.control.module_endpoint)
            poller = zmq.Poller()
            poller.register(socket, zmq.POLLIN)
            retry = RegistrationRetryController(
                retry_seconds=self.settings.registration_retry_seconds,
                acknowledgement_timeout_seconds=self.settings.registration_ack_timeout_seconds,
                maximum_attempts=self.settings.registration_max_attempts,
                monotonic=self._monotonic,
            )
            next_heartbeat = self._monotonic()
            while not self.shutdown.token.is_requested:
                if not self._registration_succeeded and retry.should_attempt():
                    try:
                        socket.send_multipart([REGISTER_MODULE, self._registration().SerializeToString()])
                        self.metrics.increment("messages_sent")
                    except zmq.Again:
                        pass
                    retry.attempted()
                registration_escalation = retry.update() if not self._registration_succeeded else None
                if registration_escalation is not None:
                    self._request_escalation(registration_escalation)
                    break
                now = self._monotonic()
                if self._registration_succeeded and now >= next_heartbeat:
                    try:
                        socket.send_multipart([MODULE_HEARTBEAT, self.session_uuid.bytes])
                        self.metrics.increment("messages_sent")
                    except zmq.Again:
                        pass
                    next_heartbeat = now + self.settings.heartbeat_interval_seconds
                while True:
                    completed = self.result_control_queue.receive(timeout_seconds=0.0)
                    if completed.status is not ReceiveStatus.ITEM:
                        break
                    assert completed.item is not None
                    self._send_control_response(socket, completed.item)
                self._update_readiness()
                if self.watchdog.exceeded():
                    self._request_escalation(self.watchdog.escalation())
                    break
                events = dict(poller.poll(self.settings.control_poll_ms))
                if socket not in events:
                    continue
                frames = socket.recv_multipart()
                message = parse_dealer_message(frames)
                if message is None:
                    self.metrics.increment("invalid_messages")
                    continue
                kind, payload = message
                if kind == REGISTER_MODULE_RESPONSE and not self._registration_succeeded:
                    self._handle_registration(payload, retry)
                elif kind == COMMAND_REQUEST and self._registration_succeeded:
                    self._handle_command(socket, payload)
                else:
                    self.metrics.increment("invalid_messages")
        finally:
            self.shutdown.request("module runner loop stopped")
            deadline = self._monotonic() + 4.5
            for thread in self._threads:
                thread.join(max(0.0, min(1.0, deadline - self._monotonic())))
            if any(thread.is_alive() for thread in self._threads):
                self._request_escalation(
                    EscalationRequest(ExitCode.TEMPORARY_FAILURE, "runner thread did not stop", "SHUTDOWN_TIMEOUT")
                )
            if socket is not None:
                socket.close(linger=0)
            context.term()
            self.shutdown.run(timeout_seconds=max(0.0, deadline - self._monotonic()))
        escalation = self.escalation
        return ExitCode.CLEAN_SHUTDOWN if escalation is None else escalation.exit_code


__all__ = ["ModuleInitializationError", "ModuleRunnerService", "RunnerSettings"]
