"""Focused Phase 5 module API, runner, publication, and supervision tests."""

from __future__ import annotations

import struct
import threading
import time
from collections.abc import Iterator
from multiprocessing import shared_memory
from pathlib import Path
from typing import cast
from uuid import UUID

import numpy as np
import pytest
import zmq
from google.protobuf import struct_pb2
from google.protobuf.message import Message
from purdue_rov.cv.v1 import bounding_box_pb2, control_pb2

from purdue_rov_cv.config.issues import ConfigIssue
from purdue_rov_cv.config.loader import load_config
from purdue_rov_cv.config.models import AppConfig
from purdue_rov_cv.frame_buffer import (
    HEADER_SIZE,
    FrameWrite,
    PixelFormat,
    SharedMemoryFrameReader,
    SharedMemoryFrameWriter,
    shared_memory_name,
)
from purdue_rov_cv.messaging.cache import CommandReservationStatus, CommandStatusCache
from purdue_rov_cv.module_runner.artifacts import ArtifactValidationError, ArtifactValidator
from purdue_rov_cv.module_runner.entrypoints import load_module, module_runner_entrypoint
from purdue_rov_cv.module_runner.frame_source import FrameSourceInvalid, SharedMemoryFrameSource
from purdue_rov_cv.module_runner.publisher import PublicationItem, ResultPublisher, configure_result_publisher
from purdue_rov_cv.module_runner.service import (
    RUNNER_SUPPORTED_COMMANDS,
    ModuleInitializationError,
    ModuleRunnerService,
)
from purdue_rov_cv.module_runner.supervision import ProcessingSupervisor, WorkerWatchdog
from purdue_rov_cv.modules.base import CVModule, Frame, ModuleContext
from purdue_rov_cv.modules.echo import EchoModule
from purdue_rov_cv.runtime.exit_codes import EscalationRequest, ExitCode
from purdue_rov_cv.runtime.metrics import RuntimeMetrics
from purdue_rov_cv.runtime.publisher import PublisherSequence
from purdue_rov_cv.runtime.queues import CvResultQueue
from purdue_rov_cv.runtime.shutdown import ShutdownToken
from purdue_rov_cv.runtime.state import ComponentState, ComponentStateMachine
from purdue_rov_cv.wire.errors import ErrorCode

FIXTURE = Path(__file__).parents[1] / "fixtures" / "config" / "valid" / "single_camera.yaml"


def _config(*, module_class: str = "purdue_rov_cv.modules.echo.EchoModule") -> AppConfig:
    config = load_config(FIXTURE)
    task = config.tasks["gate_detection"].model_copy(update={"module_class": module_class})
    return config.model_copy(update={"tasks": {"gate_detection": task}})


def _frame(number: int = 1) -> Frame:
    return Frame(np.full((2, 3, 3), number, dtype=np.uint8), "front_camera", UUID(int=3).bytes, number, 10, 11)


class _MinimalModule(CVModule):
    requires_artifact = False

    def __init__(self) -> None:
        self.context: ModuleContext | None = None
        self.dynamic: dict[str, object] = {}
        self.thread_ids: list[int] = []
        self.initialize_thread_id: int | None = None
        self.processed = threading.Event()
        self.shutdowns = 0

    def initialize(self, context: ModuleContext) -> None:
        self.context = context
        self.initialize_thread_id = threading.get_ident()

    def process(self, frame: Frame) -> list[Message]:
        self.thread_ids.append(threading.get_ident())
        self.processed.set()
        return [bounding_box_pb2.BoundingBoxResult(frame_number=frame.frame_number)]

    def apply_dynamic_config(self, config: dict) -> None:
        self.dynamic = config

    def shutdown(self) -> None:
        self.shutdowns += 1


class _InitializationFailure(_MinimalModule):
    def initialize(self, context: ModuleContext) -> None:
        del context
        raise RuntimeError("load failed")


class _FakeSource:
    def __init__(self, frames: list[Frame] | None = None) -> None:
        self.frames = list(frames or [])
        self.attached = False
        self.closed = False

    def attach(self) -> bool:
        self.attached = True
        return True

    def read(self, timeout_seconds: float = 0.250) -> Frame | None:
        del timeout_seconds
        return self.frames.pop(0) if self.frames else None

    def close(self) -> None:
        self.closed = True


class _Probe:
    def __init__(self, issues: tuple[ConfigIssue, ...] = ()) -> None:
        self.issues = issues

    def probe_camera(self, camera_id, camera):
        raise AssertionError((camera_id, camera))

    def validate_runtime_and_artifact(self, config: AppConfig) -> tuple[ConfigIssue, ...]:
        assert tuple(config.tasks) == ("gate_detection",)
        return self.issues

    def validate_port_availability(self, config: AppConfig) -> tuple[ConfigIssue, ...]:
        return ()


def _request(command: str, *, command_id: bytes = UUID(int=20).bytes) -> control_pb2.CommandRequest:
    request = control_pb2.CommandRequest(command_id=command_id, target_id="gate_detection", issued_time_unix_ns=1)
    getattr(request, command).SetInParent()
    return request


def _cached_response(
    command_id: bytes, status: int = control_pb2.COMMAND_STATUS_RECEIVED
) -> control_pb2.CommandResponse:
    return control_pb2.CommandResponse(command_id=command_id, target_id="gate_detection", status=status)


def test_cvmodule_requires_initialize_and_process_only() -> None:
    assert CVModule.__abstractmethods__ == {"initialize", "process"}
    with pytest.raises(TypeError):
        CVModule()
    module = _MinimalModule()
    module.on_start()
    module.on_stop()
    module.apply_dynamic_config({})
    module.shutdown()
    assert module.shutdowns == 1


def test_frame_private_copy_and_validation() -> None:
    original = _frame()
    copied = original.private_copy()
    original.pixels[:] = 99
    assert np.all(copied.pixels == 1)
    assert not np.shares_memory(original.pixels, copied.pixels)
    with pytest.raises(ValueError, match="16-byte"):
        Frame(np.ones((1,), dtype=np.uint8), "front_camera", b"short", 0, 0, 0)


def test_echo_uses_normal_payload_and_dynamic_configuration() -> None:
    config = _config()
    module = EchoModule()
    module.initialize(
        ModuleContext("gate_detection", "gate_detection", "rov_pi5", "front_camera", config.tasks["gate_detection"])
    )
    module.on_start()
    result = module.process(_frame())[0]
    assert isinstance(result, bounding_box_pb2.BoundingBoxResult)
    assert result.frame_number == 1
    assert result.detections[0].confidence == pytest.approx(0.6)
    module.apply_dynamic_config({"confidence_threshold": 0.25})
    assert module.process(_frame())[0].detections[0].confidence == pytest.approx(0.25)
    module.on_stop()
    module.shutdown()
    assert module.stopped and module.shutdown_called


@pytest.mark.parametrize("value", [True, "0.5", -0.1, 1.1])
def test_echo_rejects_invalid_dynamic_values(value: object) -> None:
    with pytest.raises(ValueError):
        EchoModule().apply_dynamic_config({"confidence_threshold": value})


def test_module_loader_and_entrypoint_argument_boundary() -> None:
    assert isinstance(load_module("purdue_rov_cv.modules.echo.EchoModule"), EchoModule)
    with pytest.raises((TypeError, ValueError)):
        load_module("purdue_rov_cv.modules.echo.Frame")
    with pytest.raises(SystemExit) as caught:
        module_runner_entrypoint([])
    assert caught.value.code == ExitCode.INVALID_ARGUMENTS


def test_runner_initialization_failure_enters_error_and_cleans_module() -> None:
    module = _InitializationFailure()
    service = ModuleRunnerService(_config(), "gate_detection", module, _FakeSource())
    with pytest.raises(ModuleInitializationError) as caught:
        service.run()
    assert not caught.value.artifact_related
    assert service.state_machine.state is ComponentState.ERROR
    assert module.shutdowns == 1


def test_cache_reservation_is_atomic_for_concurrent_duplicates() -> None:
    cache = CommandStatusCache()
    response = _cached_response(UUID(int=40).bytes)
    barrier = threading.Barrier(12)
    outcomes: list[bool] = []

    def reserve() -> None:
        barrier.wait()
        outcomes.append(cache.reserve(response))

    threads = [threading.Thread(target=reserve) for _ in range(12)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert outcomes.count(True) == 1
    assert outcomes.count(False) == 11
    assert cache.get(response.command_id).status == control_pb2.COMMAND_STATUS_RECEIVED


def test_cache_reservation_retains_ttl_capacity_and_final_status() -> None:
    now = [0.0]
    cache = CommandStatusCache(ttl_seconds=2.0, capacity=2, monotonic=lambda: now[0])
    ids = [UUID(int=index).bytes for index in (1, 2, 3)]
    assert cache.try_reserve(_cached_response(ids[0])) is CommandReservationStatus.RESERVED
    assert cache.try_reserve(_cached_response(ids[0])) is CommandReservationStatus.DUPLICATE
    assert cache.try_reserve(_cached_response(ids[1])) is CommandReservationStatus.RESERVED
    assert cache.try_reserve(_cached_response(ids[2])) is CommandReservationStatus.CAPACITY_FULL
    now[0] = 3.0
    assert len(cache) == 2
    assert cache.get(ids[0]).status == control_pb2.COMMAND_STATUS_RECEIVED
    cache.put(_cached_response(ids[0], control_pb2.COMMAND_STATUS_COMPLETED))
    assert cache.get(ids[0]).status == control_pb2.COMMAND_STATUS_COMPLETED
    assert cache.try_reserve(_cached_response(ids[2])) is CommandReservationStatus.RESERVED
    assert cache.get(ids[0]) is None
    assert cache.get(ids[1]).status == control_pb2.COMMAND_STATUS_RECEIVED
    now[0] = 6.0
    assert len(cache) == 2


def test_processing_deadline_streak_degrades_resets_and_escalates() -> None:
    metrics = RuntimeMetrics()
    state = ComponentStateMachine(ComponentState.RUNNING)
    escalations: list[EscalationRequest] = []
    supervisor = ProcessingSupervisor(10, metrics=metrics, state_machine=state, escalate=escalations.append)
    supervisor.record_success(11_000_000)
    assert supervisor.deadline_miss_streak == 1
    supervisor.record_success(1_000_000)
    assert supervisor.deadline_miss_streak == 0
    for _ in range(5):
        supervisor.record_success(11_000_000)
    assert state.state is ComponentState.DEGRADED
    for _ in range(15):
        supervisor.record_success(11_000_000)
    assert escalations[-1].exit_code is ExitCode.TEMPORARY_FAILURE
    snapshot = metrics.snapshot().values
    assert snapshot["processing_deadline_misses"] == 21
    assert snapshot["frames_processed"] == 22


def test_processing_exception_streak_enters_error_and_success_resets() -> None:
    metrics = RuntimeMetrics()
    state = ComponentStateMachine(ComponentState.RUNNING)
    supervisor = ProcessingSupervisor(10, metrics=metrics, state_machine=state, escalate=lambda escalation: None)
    supervisor.record_exception()
    assert state.state is ComponentState.DEGRADED
    supervisor.record_success(1)
    assert supervisor.exception_streak == 0
    assert state.state is ComponentState.RUNNING
    for _ in range(3):
        supervisor.record_exception()
    assert state.state is ComponentState.ERROR
    assert metrics.snapshot().values["processing_exceptions"] == 4


def test_worker_watchdog_threshold_progress_and_stall() -> None:
    now = [0]
    watchdog = WorkerWatchdog(100, monotonic_ns=lambda: now[0])
    assert watchdog.threshold_ns == 10_000_000_000
    now[0] = 9_000_000_000
    assert not watchdog.exceeded()
    watchdog.progress()
    now[0] = 20_000_000_001
    assert watchdog.exceeded()
    assert watchdog.escalation().exit_code is ExitCode.TEMPORARY_FAILURE
    assert watchdog.escalation().event_code == ErrorCode.PROCESSING_WATCHDOG_EXCEEDED


def test_shared_memory_source_attaches_copies_and_never_unlinks() -> None:
    name = shared_memory_name("front_camera")
    writer = SharedMemoryFrameWriter("front_camera", 64, UUID(int=50).bytes)
    reader = SharedMemoryFrameReader("front_camera", unregister_from_resource_tracker=False)
    source = SharedMemoryFrameSource(name, camera_id="front_camera", reader=reader)
    pixels = np.arange(18, dtype=np.uint8).reshape((2, 3, 3))
    try:
        writer.open()
        writer.write(FrameWrite(pixels.tobytes(), 3, 2, 9, PixelFormat.BGR8, 7, 8, 9))
        assert source.attach()
        frame = source.read(0.0)
        assert frame is not None and frame.frame_number == 7
        writer.write(FrameWrite(bytes([99]) * 18, 3, 2, 9, PixelFormat.BGR8, 8, 10, 11))
        assert np.array_equal(frame.pixels, pixels)
        source.close()
        verifier = shared_memory.SharedMemory(name=name, create=False)
        verifier.close()
    finally:
        source.close()
        writer.close()


def test_shared_memory_source_missing_and_invalid_header() -> None:
    name = shared_memory_name("front_camera")
    source = SharedMemoryFrameSource(name, camera_id="front_camera")
    assert not source.attach()
    invalid = shared_memory.SharedMemory(name=name, create=True, size=HEADER_SIZE + 3 * 64)
    try:
        invalid.buf[:] = bytes(len(invalid.buf))
        invalid.buf[:8] = b"INVALID!"
        struct.pack_into("<I", invalid.buf, 76, 123)
        with pytest.raises(FrameSourceInvalid):
            source.attach()
    finally:
        source.close()
        invalid.close()
        invalid.unlink()


def test_shared_memory_source_reports_disconnect_and_reattachment() -> None:
    camera_id = "front_camera"
    name = shared_memory_name(camera_id)
    metrics = RuntimeMetrics()
    first = SharedMemoryFrameWriter(camera_id, 64, UUID(int=51).bytes)
    reader = SharedMemoryFrameReader(camera_id, unregister_from_resource_tracker=False)
    source = SharedMemoryFrameSource(name, camera_id=camera_id, metrics=metrics, reader=reader)
    replacement: SharedMemoryFrameWriter | None = None
    try:
        first.open()
        first.write(FrameWrite(bytes(18), 3, 2, 9, PixelFormat.BGR8, 0, 1, 2))
        assert source.attach()
        assert source.read(0.0) is not None
        assert metrics.snapshot().values["input_source_present"] is True

        first.close()
        assert source.read(0.0) is None
        lost = metrics.snapshot().values
        assert lost["input_source_present"] is False
        assert lost["shared_memory_disconnects"] == 1

        replacement = SharedMemoryFrameWriter(camera_id, 64, UUID(int=52).bytes)
        replacement.open()
        replacement.write(FrameWrite(bytes([9]) * 18, 3, 2, 9, PixelFormat.BGR8, 0, 3, 4))
        assert source.attach()
        recovered = source.read(0.0)
        assert recovered is not None and recovered.camera_session_id == UUID(int=52).bytes
        values = metrics.snapshot().values
        assert values["input_source_present"] is True
        assert values["shared_memory_reattach_count"] == 1
    finally:
        source.close()
        first.close()
        if replacement is not None:
            replacement.close()


def test_artifact_adapter_reports_phase2_issues_and_load_failure() -> None:
    config = _config()
    issue = ConfigIssue("MODEL_HASH_MISMATCH", "tasks.gate_detection.artifact.sha256", "mismatch")
    with pytest.raises(ArtifactValidationError) as caught:
        ArtifactValidator(probe=_Probe((issue,))).validate(config, "gate_detection")
    assert caught.value.issues == (issue,)

    def fail_load(task) -> None:
        raise RuntimeError(task.artifact.path)

    with pytest.raises(ArtifactValidationError) as load_caught:
        ArtifactValidator(probe=_Probe(), load_probe=fail_load).validate(config, "gate_detection")
    assert load_caught.value.issues[0].code == "MODEL_LOAD_FAILED"
    ArtifactValidator(probe=_Probe()).validate(config, "gate_detection")


def test_result_publisher_socket_contract() -> None:
    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    try:
        configure_result_publisher(socket)
        assert socket.getsockopt(zmq.SNDHWM) == 5
        assert socket.getsockopt(zmq.SNDTIMEO) == 0
        assert socket.getsockopt(zmq.LINGER) == 0
        assert socket.getsockopt(zmq.IMMEDIATE) == 1
        assert socket.getsockopt(zmq.RECONNECT_IVL) == 250
        assert socket.getsockopt(zmq.RECONNECT_IVL_MAX) == 2_000
        assert socket.getsockopt(zmq.MAXMSGSIZE) == 4 * 1024 * 1024
    finally:
        socket.close(linger=0)
        context.term()


class _SendSocket:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.parts: list[bytes] | None = None

    def send_multipart(self, parts: list[bytes], flags: int) -> None:
        assert flags == zmq.DONTWAIT
        if self.fail:
            raise zmq.Again()
        self.parts = parts


def _publisher(metrics: RuntimeMetrics, sequence: PublisherSequence) -> ResultPublisher:
    return ResultPublisher(
        "inproc://unused",
        topic="cv.result.gate_detection.front_camera",
        payload_type="bounding_boxes_v1",
        task_id="gate_detection",
        module_id="gate_detection",
        device_id="rov_pi5",
        health_interval_ms=1_000,
        queue=CvResultQueue(metrics=metrics),
        metrics=metrics,
        state_machine=ComponentStateMachine(ComponentState.READY),
        shutdown=ShutdownToken(),
        context=zmq.Context(),
        sequence=sequence,
    )


def test_publisher_envelope_sequence_success_and_nonretry_drop() -> None:
    metrics = RuntimeMetrics()
    sequence = PublisherSequence(uuid_factory=lambda: UUID(int=60))
    publisher = _publisher(metrics, sequence)
    from purdue_rov_cv.runtime.envelope import EnvelopeBuilder

    builder = EnvelopeBuilder(sequence, unix_time_ns=lambda: 1, monotonic_ns=lambda: 2)
    payload = bounding_box_pb2.BoundingBoxResult(
        camera_id="front_camera", camera_session_id=UUID(int=3).bytes, frame_number=1, capture_time_unix_ns=10
    )
    sent = _SendSocket()
    publisher._send(sent, builder, PublicationItem(payload, _frame()))
    assert sent.parts is not None
    assert metrics.snapshot().values["results_published"] == 1
    dropped = _SendSocket(fail=True)
    payload_2 = bounding_box_pb2.BoundingBoxResult(
        camera_id="front_camera",
        camera_session_id=UUID(int=3).bytes,
        frame_number=2,
        capture_time_unix_ns=10,
    )
    publisher._send(dropped, builder, PublicationItem(payload_2, _frame(2)))
    assert metrics.snapshot().values["zmq_send_dropped"] == 1
    assert sequence.next_attempt().sequence_number == 2
    publisher.context.term()


def test_publisher_error_health_includes_canonical_last_error() -> None:
    metrics = RuntimeMetrics()
    metrics.set_metadata("last_error_code", ErrorCode.PROCESSING_FAILURE)
    metrics.set_metadata("last_error_message", "RuntimeError: bad frame")
    publisher = _publisher(metrics, PublisherSequence(uuid_factory=lambda: UUID(int=61)))
    health = publisher._health()
    assert health.last_error_code == ErrorCode.PROCESSING_FAILURE
    assert health.last_error_message == "RuntimeError: bad frame"
    publisher.context.term()


def test_runner_dynamic_struct_validation_readiness_and_lifecycle() -> None:
    module = _MinimalModule()
    service = ModuleRunnerService(_config(), "gate_detection", module, _FakeSource())
    module.initialize(service._module_context())
    service._initialized = True
    service._registration_succeeded = True
    service._input_exists.set()
    service._first_frame.set()
    service._update_readiness()
    assert service.state_machine.state is ComponentState.READY
    assert service._execute_command(_request("start")).status == control_pb2.COMMAND_STATUS_COMPLETED
    duplicate_start = service._execute_command(_request("start", command_id=UUID(int=21).bytes))
    assert duplicate_start.error_code == ErrorCode.INVALID_STATE_TRANSITION
    dynamic = _request("set_dynamic_config", command_id=UUID(int=22).bytes)
    dynamic.set_dynamic_config.fields.CopyFrom(
        struct_pb2.Struct(fields={"dynamic.confidence_threshold": struct_pb2.Value(number_value=0.4)})
    )
    assert service._execute_command(dynamic).status == control_pb2.COMMAND_STATUS_COMPLETED
    assert module.dynamic == {"confidence_threshold": 0.4}
    empty = _request("set_dynamic_config", command_id=UUID(int=24).bytes)
    assert service._execute_command(empty).status == control_pb2.COMMAND_STATUS_COMPLETED
    assert module.dynamic == {"confidence_threshold": 0.4}

    runner_owned = _request("set_dynamic_config", command_id=UUID(int=25).bytes)
    runner_owned.set_dynamic_config.fields.CopyFrom(
        struct_pb2.Struct(
            fields={
                "max_input_fps": struct_pb2.Value(number_value=10),
                "diagnostics.publish_interval_ms": struct_pb2.Value(number_value=750),
            }
        )
    )
    assert service._execute_command(runner_owned).status == control_pb2.COMMAND_STATUS_COMPLETED
    assert service.task.max_input_fps == 10
    assert service._current_health_interval_ms() == 750
    assert module.dynamic == {"confidence_threshold": 0.4}

    debug = _request("set_dynamic_config", command_id=UUID(int=26).bytes)
    debug.set_dynamic_config.fields["debug_snapshots.jpeg_quality"] = 65
    assert service._execute_command(debug).status == control_pb2.COMMAND_STATUS_COMPLETED
    assert module.dynamic == {"debug_snapshots": {"enabled": True, "maximum_rate_hz": 1.0, "jpeg_quality": 65}}
    assert (
        service._execute_command(_request("stop", command_id=UUID(int=23).bytes)).resulting_state
        == ComponentState.READY
    )


def test_runner_static_dynamic_update_is_rejected() -> None:
    service = ModuleRunnerService(_config(), "gate_detection", _MinimalModule(), _FakeSource())
    service.state_machine.transition_to(ComponentState.READY)
    service.state_machine.transition_to(ComponentState.RUNNING)
    request = _request("set_dynamic_config")
    request.set_dynamic_config.fields["artifact.path"] = "/tmp/model"
    response = service._execute_command(request)
    assert response.status == control_pb2.COMMAND_STATUS_REJECTED
    assert response.error_code == ErrorCode.RESTART_REQUIRED
    assert service.state_machine.state is ComponentState.DEGRADED
    restart_blocked = service._execute_command(_request("start", command_id=UUID(int=28).bytes))
    assert restart_blocked.error_code == ErrorCode.RESTART_REQUIRED
    assert service.state_machine.state is ComponentState.DEGRADED

    unsupported = _request("set_dynamic_config", command_id=UUID(int=27).bytes)
    unsupported.set_dynamic_config.fields["diagnostics.log_level"] = "DEBUG"
    unsupported_response = service._execute_command(unsupported)
    assert unsupported_response.status == control_pb2.COMMAND_STATUS_REJECTED
    assert unsupported_response.error_code == ErrorCode.INVALID_COMMAND


def test_runner_dynamic_callback_failure_rolls_back_without_committing() -> None:
    class PartiallyFailingDynamic(_MinimalModule):
        def __init__(self) -> None:
            super().__init__()
            self.calls: list[dict[str, object]] = []

        def apply_dynamic_config(self, config: dict) -> None:
            self.dynamic = config
            self.calls.append(config)
            if len(self.calls) == 1:
                raise ValueError("partial apply")

    module = PartiallyFailingDynamic()
    service = ModuleRunnerService(_config(), "gate_detection", module, _FakeSource())
    request = _request("set_dynamic_config")
    request.set_dynamic_config.fields["dynamic.confidence_threshold"] = 0.2

    response = service._execute_command(request)

    assert response.status == control_pb2.COMMAND_STATUS_REJECTED
    assert response.error_code == ErrorCode.INVALID_COMMAND
    assert module.calls == [{"confidence_threshold": 0.2}, {"confidence_threshold": 0.6}]
    assert module.dynamic == {"confidence_threshold": 0.6}
    assert service.task.dynamic.confidence_threshold == pytest.approx(0.6)


def test_runner_lifecycle_value_error_is_internal_failure_and_enters_error() -> None:
    class StartFailure(_MinimalModule):
        def on_start(self) -> None:
            raise ValueError("module bug")

    service = ModuleRunnerService(_config(), "gate_detection", StartFailure(), _FakeSource())
    service.state_machine.transition_to(ComponentState.READY)

    response = service._execute_command(_request("start"))

    assert response.status == control_pb2.COMMAND_STATUS_FAILED
    assert response.error_code == ErrorCode.INTERNAL_ERROR
    assert service.state_machine.state is ComponentState.ERROR


def test_runner_registration_advertises_only_implemented_canonical_commands() -> None:
    service = ModuleRunnerService(_config(), "gate_detection", _MinimalModule(), _FakeSource())
    registration = service._registration()
    assert set(registration.supported_command_types) == RUNNER_SUPPORTED_COMMANDS


def test_runner_duplicate_during_execution_has_exactly_one_side_effect() -> None:
    class BlockingStart(_MinimalModule):
        def __init__(self) -> None:
            super().__init__()
            self.entered = threading.Event()
            self.release = threading.Event()
            self.start_count = 0

        def on_start(self) -> None:
            self.start_count += 1
            self.entered.set()
            assert self.release.wait(1.0)

    module = BlockingStart()
    service = ModuleRunnerService(_config(), "gate_detection", module, _FakeSource())
    service.state_machine.transition_to(ComponentState.READY)
    worker = threading.Thread(target=service._worker)
    worker.start()
    assert service._initialization_complete.wait(1.0)
    context = zmq.Context()
    server: zmq.Socket[bytes] = context.socket(zmq.PAIR)
    client: zmq.Socket[bytes] = context.socket(zmq.PAIR)
    endpoint = "inproc://phase5-concurrent-duplicate"
    server.bind(endpoint)
    client.connect(endpoint)
    request = _request("start", command_id=UUID(int=333).bytes)
    try:
        service._handle_command(server, request.SerializeToString())
        initial = control_pb2.CommandResponse.FromString(client.recv_multipart()[1])
        assert initial.status == control_pb2.COMMAND_STATUS_RECEIVED
        assert module.entered.wait(1.0)

        service._handle_command(server, request.SerializeToString())
        duplicate = control_pb2.CommandResponse.FromString(client.recv_multipart()[1])
        assert duplicate.error_code == ErrorCode.DUPLICATE_COMMAND_ID

        module.release.set()
        final = service.result_control_queue.receive(timeout_seconds=0.250)
        assert final.item is not None and final.item.status == control_pb2.COMMAND_STATUS_COMPLETED
        assert service.cache.get(request.command_id).status == control_pb2.COMMAND_STATUS_COMPLETED
        assert module.start_count == 1
    finally:
        module.release.set()
        service.request_shutdown()
        worker.join(1.0)
        server.close(linger=0)
        client.close(linger=0)
        context.term()


def test_runner_worker_processes_zero_multiple_and_malformed_outputs_on_one_thread() -> None:
    class Outputs(_MinimalModule):
        def __init__(self) -> None:
            super().__init__()
            self.outputs: Iterator[object] = iter(
                [[], [bounding_box_pb2.BoundingBoxResult(), bounding_box_pb2.BoundingBoxResult()], [object()]]
            )

        def process(self, frame: Frame) -> list[Message]:
            self.thread_ids.append(threading.get_ident())
            self.processed.set()
            return cast(list[Message], next(self.outputs))

    module = Outputs()
    source = _FakeSource()
    service = ModuleRunnerService(_config(), "gate_detection", module, source)
    service.state_machine.transition_to(ComponentState.READY)
    service.state_machine.transition_to(ComponentState.RUNNING)
    worker = threading.Thread(target=service._worker)
    worker.start()
    assert service._initialization_complete.wait(1.0)
    for number in range(1, 4):
        service.frame_queue.offer(_frame(number))
        deadline = time.monotonic() + 1.0
        while len(module.thread_ids) < number and time.monotonic() < deadline:
            assert module.processed.wait(0.050)
            module.processed.clear()
    service.request_shutdown()
    worker.join(1.0)
    assert not worker.is_alive()
    assert len(set(module.thread_ids)) == 1
    assert module.initialize_thread_id == module.thread_ids[0]
    assert service.result_queue.qsize() == 2
    assert service.metrics.snapshot().values["processing_exceptions"] == 1
    assert module.shutdowns == 1


def test_runner_control_result_full_caches_and_escalates() -> None:
    service = ModuleRunnerService(_config(), "gate_detection", _MinimalModule(), _FakeSource())
    responses = [_cached_response(UUID(int=index).bytes) for index in range(100, 117)]
    for response in responses[:16]:
        assert service.result_control_queue.offer(response).accepted
    dropped = service.result_control_queue.offer(responses[16])
    assert not dropped.accepted
    assert service.cache.get(responses[16].command_id) is not None
    assert service.escalation is not None
    assert service.escalation.exit_code is ExitCode.TEMPORARY_FAILURE


def test_runner_command_queue_full_returns_module_busy_and_caches_outcome() -> None:
    service = ModuleRunnerService(_config(), "gate_detection", _MinimalModule(), _FakeSource())
    for index in range(16):
        assert service.command_queue.offer(_request("start", command_id=UUID(int=200 + index).bytes)).accepted
    context = zmq.Context()
    server: zmq.Socket[bytes] = context.socket(zmq.PAIR)
    client: zmq.Socket[bytes] = context.socket(zmq.PAIR)
    endpoint = "inproc://phase5-module-busy"
    server.bind(endpoint)
    client.connect(endpoint)
    command = _request("start", command_id=UUID(int=300).bytes)
    try:
        service._handle_command(server, command.SerializeToString())
        frames = client.recv_multipart()
        assert frames[0] == b"COMMAND_RESPONSE"
        response = control_pb2.CommandResponse.FromString(frames[1])
        assert response.status == control_pb2.COMMAND_STATUS_REJECTED
        assert response.error_code == ErrorCode.MODULE_BUSY
        assert service.cache.get(command.command_id).error_code == ErrorCode.MODULE_BUSY
    finally:
        server.close(linger=0)
        client.close(linger=0)
        context.term()


def test_runner_error_reset_and_shutdown_attachment_cleanup() -> None:
    module = _MinimalModule()
    source = _FakeSource([_frame()])
    service = ModuleRunnerService(_config(), "gate_detection", module, source)
    module.initialize(service._module_context())
    service.state_machine.transition_to(ComponentState.ERROR)
    reset = service._execute_command(_request("reset"))
    assert reset.status == control_pb2.COMMAND_STATUS_COMPLETED
    assert service.state_machine.state is ComponentState.STARTING
    service.request_shutdown()
    source.close()
    assert source.closed


def test_runner_shutdown_hook_failure_requests_internal_software_exit() -> None:
    class ShutdownFailure(_MinimalModule):
        def shutdown(self) -> None:
            raise RuntimeError("cleanup bug")

    service = ModuleRunnerService(_config(), "gate_detection", ShutdownFailure(), _FakeSource())
    worker = threading.Thread(target=service._worker)
    worker.start()
    assert service._initialization_complete.wait(1.0)
    service.request_shutdown()
    worker.join(1.0)
    assert not worker.is_alive()
    assert service.escalation is not None
    assert service.escalation.exit_code is ExitCode.INTERNAL_SOFTWARE_FAILURE
    assert service.escalation.event_code == ErrorCode.INTERNAL_ERROR
