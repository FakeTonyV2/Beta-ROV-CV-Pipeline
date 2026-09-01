"""Real-process Phase 5 control, publication, failure, and isolation tests."""

from __future__ import annotations

import mmap
import multiprocessing
import socket
import struct
import subprocess
import sys
import time
from pathlib import Path
from uuid import UUID, uuid4

import numpy as np
import pytest
import zmq
from google.protobuf import struct_pb2
from purdue_rov.cv.v1 import (
    bounding_box_pb2,
    control_pb2,
    diagnostics_pb2,
    envelope_pb2,
    module_state_pb2,
    registration_pb2,
)

from purdue_rov_cv.camera import CameraService, CapturedFrame
from purdue_rov_cv.config.loader import load_config
from purdue_rov_cv.config.models import AppConfig
from purdue_rov_cv.frame_buffer import (
    FrameWrite,
    PixelFormat,
    SharedMemoryFrameReader,
    SharedMemoryFrameWriter,
    shared_memory_name,
)
from purdue_rov_cv.messaging.broker import DataBrokerService
from purdue_rov_cv.messaging.client import ControlClient
from purdue_rov_cv.messaging.protocol import MODULE_HEARTBEAT, REGISTER_MODULE, REGISTER_MODULE_RESPONSE
from purdue_rov_cv.messaging.router import ControlRouterService
from purdue_rov_cv.messaging.sockets import configure_dealer
from purdue_rov_cv.module_runner.frame_source import SharedMemoryFrameSource
from purdue_rov_cv.module_runner.service import ModuleRunnerService, RunnerSettings
from purdue_rov_cv.modules.base import Frame
from purdue_rov_cv.modules.echo import EchoModule
from purdue_rov_cv.runtime.envelope import ReceivedMultipartValidator
from purdue_rov_cv.runtime.exit_codes import ExitCode
from purdue_rov_cv.runtime.metrics import RuntimeMetrics
from purdue_rov_cv.runtime.publisher import PublisherSequence
from purdue_rov_cv.runtime.shutdown import install_signal_handlers
from purdue_rov_cv.runtime.state import ComponentState
from purdue_rov_cv.wire.errors import ErrorCode

FIXTURE = Path(__file__).parents[1] / "fixtures" / "config" / "valid" / "single_camera.yaml"
SHARED_FRAME_MAGIC = b"PROVCV1\0"
SHARED_FRAME_VERSION = 1
SHARED_FRAME_DTYPE_UINT8 = 1
SHARED_FRAME_HEADER = struct.Struct("<8sIQIIII16sQqq")
_SEQUENCE_OFFSET = struct.calcsize("<8sI")


def _free_tcp_endpoint() -> str:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as candidate:
        candidate.bind(("127.0.0.1", 0))
        return f"tcp://127.0.0.1:{candidate.getsockname()[1]}"


def _config(
    publisher_endpoint: str,
    subscriber_endpoint: str,
    client_endpoint: str,
    module_endpoint: str,
    *,
    task_ids: tuple[str, ...] = ("task_a",),
) -> AppConfig:
    base = load_config(FIXTURE)
    messaging = base.messaging.model_copy(
        update={
            "broker": base.messaging.broker.model_copy(
                update={"publisher_endpoint": publisher_endpoint, "subscriber_endpoint": subscriber_endpoint}
            ),
            "control": base.messaging.control.model_copy(
                update={"client_endpoint": client_endpoint, "module_endpoint": module_endpoint}
            ),
        }
    )
    original = base.tasks["gate_detection"]
    tasks = {
        task_id: original.model_copy(
            update={
                "module_class": "purdue_rov_cv.modules.echo.EchoModule",
                "publish_topic": f"cv.result.{task_id}.front_camera",
            }
        )
        for task_id in task_ids
    }
    return base.model_copy(update={"messaging": messaging, "tasks": tasks})


class _FrameWriter:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.pixels = np.arange(18, dtype=np.uint8).reshape((2, 3, 3))
        self.file = path.open("w+b")
        self.file.truncate(SHARED_FRAME_HEADER.size + self.pixels.nbytes)
        self.mapping = mmap.mmap(self.file.fileno(), length=0, access=mmap.ACCESS_WRITE)
        self.sequence = 0
        self.frame_number = 0

    def publish(self) -> int:
        self.sequence += 2
        self.frame_number += 1
        values = (
            SHARED_FRAME_MAGIC,
            SHARED_FRAME_VERSION,
            self.sequence - 1,
            3,
            2,
            3,
            SHARED_FRAME_DTYPE_UINT8,
            UUID(int=700).bytes,
            self.frame_number,
            time.time_ns(),
            time.monotonic_ns(),
        )
        self.mapping[: SHARED_FRAME_HEADER.size] = SHARED_FRAME_HEADER.pack(*values)
        self.mapping[SHARED_FRAME_HEADER.size :] = self.pixels.tobytes()
        stable = list(values)
        stable[2] = self.sequence
        self.mapping[: SHARED_FRAME_HEADER.size] = SHARED_FRAME_HEADER.pack(*stable)
        self.mapping.flush()
        return self.frame_number

    def close(self) -> None:
        self.mapping.close()
        self.file.close()


class _FileFrameSource:
    """Injectable Phase 5 seam retained for non-Phase-6 process scenarios."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.file = None
        self.mapping = None
        self.last_sequence = 0

    @property
    def attached(self) -> bool:
        return self.mapping is not None

    def attach(self) -> bool:
        if self.attached:
            return True
        try:
            self.file = self.path.open("rb", buffering=0)
        except FileNotFoundError:
            return False
        self.mapping = mmap.mmap(self.file.fileno(), length=0, access=mmap.ACCESS_READ)
        return True

    def read(self, timeout_seconds: float = 0.250) -> Frame | None:
        deadline = time.monotonic() + timeout_seconds
        while True:
            assert self.mapping is not None
            values = SHARED_FRAME_HEADER.unpack_from(self.mapping, 0)
            magic, version, sequence, width, height, channels, dtype_code, session, number, unix_ns, mono_ns = values
            if (
                magic == SHARED_FRAME_MAGIC
                and version == SHARED_FRAME_VERSION
                and dtype_code == SHARED_FRAME_DTYPE_UINT8
                and sequence > 0
                and sequence % 2 == 0
                and sequence != self.last_sequence
            ):
                size = width * height * channels
                pixels = np.frombuffer(
                    self.mapping,
                    dtype=np.uint8,
                    count=size,
                    offset=SHARED_FRAME_HEADER.size,
                ).reshape((height, width, channels))
                copied = np.array(pixels, copy=True)
                stable = struct.unpack_from("<Q", self.mapping, _SEQUENCE_OFFSET)[0]
                if stable == sequence and stable % 2 == 0:
                    self.last_sequence = stable
                    return Frame(copied, "front_camera", session, number, unix_ns, mono_ns)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            time.sleep(min(0.005, remaining))

    def close(self) -> None:
        mapping, file = self.mapping, self.file
        self.mapping = None
        self.file = None
        if mapping is not None:
            mapping.close()
        if file is not None:
            file.close()


class _FailingEcho(EchoModule):
    def __init__(self, counter_path: Path) -> None:
        super().__init__()
        self.counter_path = counter_path

    def process(self, frame: Frame):
        with self.counter_path.open("a", encoding="utf-8") as counter:
            counter.write(f"{frame.frame_number}\n")
        raise RuntimeError(f"failed frame {frame.frame_number}")


class _HungEcho(EchoModule):
    def process(self, frame: Frame):
        del frame
        while True:
            time.sleep(1.0)


class _SlowEcho(EchoModule):
    def process(self, frame: Frame):
        time.sleep(0.03)
        return super().process(frame)


class _BlockingStartEcho(EchoModule):
    def __init__(self, counter_path: Path) -> None:
        super().__init__()
        self.counter_path = counter_path

    def on_start(self) -> None:
        with self.counter_path.open("a", encoding="utf-8") as counter:
            counter.write("start\n")
        time.sleep(0.5)
        super().on_start()


class _ShutdownBlockingStartEcho(EchoModule):
    def __init__(self, entered_path: Path) -> None:
        super().__init__()
        self.entered_path = entered_path

    def on_start(self) -> None:
        self.entered_path.write_text("entered\n", encoding="utf-8")
        time.sleep(10.0)


def _run_broker(publisher_endpoint: str, subscriber_endpoint: str, signal_ready) -> None:
    service = DataBrokerService(publisher_endpoint, subscriber_endpoint)
    install_signal_handlers(service.shutdown)
    signal_ready.set()
    service.run()


def _await_process_ready(process: multiprocessing.Process, ready, *, timeout_seconds: float = 15.0) -> None:
    if ready.wait(timeout_seconds):
        return
    exit_code = process.exitcode
    if process.is_alive():
        process.terminate()
    process.join(5.0)
    raise AssertionError(f"{process.name} did not signal readiness; initial exit code={exit_code}")


def _run_router(client_endpoint: str, module_endpoint: str, allowed: set[str]) -> None:
    ControlRouterService(
        client_endpoint,
        module_endpoint,
        device_id="rov_pi5",
        allowed_module_ids=allowed,
        heartbeat_expiry_seconds=0.4,
        install_signals=True,
    ).run()


def _run_runner(
    config: AppConfig,
    task_id: str,
    directory: Path,
    name: str,
    identities,
    module_kind: str = "echo",
    watchdog_minimum_seconds: float = 10.0,
) -> None:
    if module_kind == "failing":
        module = _FailingEcho(directory / f"{name}.failures")
    elif module_kind == "hung":
        module = _HungEcho()
    elif module_kind == "slow":
        module = _SlowEcho()
    elif module_kind == "blocking_start":
        module = _BlockingStartEcho(directory / f"{name}.starts")
    elif module_kind == "shutdown_blocking_start":
        module = _ShutdownBlockingStartEcho(directory / f"{name}.entered")
    else:
        module = EchoModule()
    sequence = PublisherSequence()
    service = ModuleRunnerService(
        config,
        task_id,
        module,
        _FileFrameSource(directory / name),
        settings=RunnerSettings(
            registration_retry_seconds=0.05,
            registration_ack_timeout_seconds=0.05,
            heartbeat_interval_seconds=0.1,
            watchdog_minimum_seconds=watchdog_minimum_seconds,
            control_poll_ms=10,
        ),
        publisher_sequence=sequence,
        install_signals=True,
    )
    identities.put((service.session_uuid.bytes, sequence.session_id))
    raise SystemExit(int(service.run()))


def _run_unregistered(config: AppConfig, directory: Path, name: str, result) -> None:
    service = ModuleRunnerService(
        config,
        "task_a",
        EchoModule(),
        _FileFrameSource(directory / name),
        settings=RunnerSettings(
            registration_retry_seconds=0.02,
            registration_ack_timeout_seconds=0.02,
            registration_max_attempts=10,
            heartbeat_interval_seconds=0.1,
            watchdog_minimum_seconds=10.0,
            control_poll_ms=5,
        ),
    )
    started = time.monotonic()
    exit_code = service.run()
    result.put((exit_code, time.monotonic() - started))
    raise SystemExit(int(exit_code))


def _run_shared_memory_runner(config: AppConfig, identities, camera_id: str = "front_camera") -> None:
    sequence = PublisherSequence()
    reader = SharedMemoryFrameReader(camera_id, unregister_from_resource_tracker=False)
    source = SharedMemoryFrameSource(
        shared_memory_name(camera_id),
        camera_id=camera_id,
        expected_slot_capacity_bytes=config.cameras[camera_id].slot_capacity_bytes,
        reader=reader,
    )
    service = ModuleRunnerService(
        config,
        "task_a",
        EchoModule(),
        source,
        settings=RunnerSettings(
            registration_retry_seconds=0.05,
            registration_ack_timeout_seconds=0.05,
            heartbeat_interval_seconds=0.1,
            control_poll_ms=10,
        ),
        publisher_sequence=sequence,
        install_signals=True,
    )
    identities.put((service.session_uuid.bytes, sequence.session_id))
    raise SystemExit(int(service.run()))


class _CameraLoopBackend:
    def __init__(self) -> None:
        self.number = 0

    def start(self) -> None:
        return None

    def poll(self, timeout_seconds: float) -> CapturedFrame:
        time.sleep(min(timeout_seconds, 0.01))
        value = self.number % 251
        self.number += 1
        return CapturedFrame(
            bytes([value]) * 18,
            3,
            2,
            9,
            PixelFormat.BGR8,
            time.time_ns(),
            time.monotonic_ns(),
        )

    def stop(self) -> None:
        return None


def _run_camera_service(camera_id: str, camera_config, session: bytes) -> None:
    service = CameraService(
        camera_id,
        camera_config,
        _CameraLoopBackend,
        session_uuid=UUID(bytes=session),
        install_signals=True,
    )
    service.run()


def _request(target: str, command: str, *, command_id: bytes | None = None) -> control_pb2.CommandRequest:
    request = control_pb2.CommandRequest(
        command_id=command_id or uuid4().bytes,
        target_id=target,
        issued_time_unix_ns=time.time_ns(),
    )
    getattr(request, command).SetInParent()
    return request


def _wait_state(client: ControlClient, target: str, state: ComponentState, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = client.send_command(_request(target, "get_status"))
        if last.status == control_pb2.COMMAND_STATUS_COMPLETED and last.resulting_state == state:
            return
        time.sleep(0.02)
    raise AssertionError(f"{target} did not reach {state}: {last}")


def _stop(process: multiprocessing.Process, *, clean: bool = True) -> float:
    started = time.monotonic()
    if process.is_alive():
        process.terminate()
    process.join(5.0)
    elapsed = time.monotonic() - started
    assert not process.is_alive()
    assert elapsed < 5.0
    if clean:
        assert process.exitcode == ExitCode.CLEAN_SHUTDOWN
    print(f"{process.name}_shutdown_seconds={elapsed:.3f}")
    return elapsed


def test_real_runner_control_echo_publication_and_processing_error(tmp_path: Path) -> None:
    pub, sub, client_endpoint = _free_tcp_endpoint(), _free_tcp_endpoint(), _free_tcp_endpoint()
    module_endpoint = f"ipc://{tmp_path / 'control.sock'}"
    config = _config(pub, sub, client_endpoint, module_endpoint)
    writer = _FrameWriter(tmp_path / "task_a")
    writer.publish()
    process_context = multiprocessing.get_context("spawn")
    identities = process_context.Queue()
    broker_signal_ready = process_context.Event()
    broker = process_context.Process(target=_run_broker, args=(pub, sub, broker_signal_ready), name="phase5-broker")
    router = process_context.Process(
        target=_run_router, args=(client_endpoint, module_endpoint, {"task_a"}), name="phase5-router"
    )
    runner = process_context.Process(
        target=_run_runner, args=(config, "task_a", tmp_path, "task_a", identities), name="phase5-runner"
    )
    broker.start()
    _await_process_ready(broker, broker_signal_ready)
    router.start()
    runner.start()
    module_session, publisher_session = identities.get(timeout=5.0)
    assert module_session != publisher_session
    context = zmq.Context()
    subscriber: zmq.Socket[bytes] = context.socket(zmq.SUB)
    subscriber.setsockopt(zmq.LINGER, 0)
    subscriber.setsockopt(zmq.SUBSCRIBE, b"cv.result.task_a.front_camera")
    subscriber.connect(sub)
    client = ControlClient(client_endpoint, acknowledgement_timeout_seconds=0.3)
    try:
        _wait_state(client, "task_a", ComponentState.READY)
        start_id = UUID(int=710).bytes
        started = client.execute_command(_request("task_a", "start", command_id=start_id))
        assert started.status == control_pb2.COMMAND_STATUS_COMPLETED
        assert started.resulting_state == ComponentState.RUNNING
        duplicate = client.send_command(_request("task_a", "start", command_id=start_id))
        assert duplicate.error_code == ErrorCode.DUPLICATE_COMMAND_ID
        cached = client.get_command_status("task_a", start_id)
        assert cached.status == control_pb2.COMMAND_STATUS_COMPLETED
        assert "count=1" in cached.message

        dynamic = _request("task_a", "set_dynamic_config")
        dynamic.set_dynamic_config.fields.CopyFrom(
            struct_pb2.Struct(fields={"dynamic.confidence_threshold": struct_pb2.Value(number_value=0.2)})
        )
        assert client.execute_command(dynamic).status == control_pb2.COMMAND_STATUS_COMPLETED

        received = None
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline and received is None:
            writer.publish()
            if subscriber.poll(100, zmq.POLLIN):
                candidate = subscriber.recv_multipart()
                candidate_envelope = envelope_pb2.MessageEnvelope.FromString(candidate[1])
                candidate_result = bounding_box_pb2.BoundingBoxResult.FromString(candidate_envelope.payload)
                if candidate_result.detections[0].confidence == pytest.approx(0.2):
                    received = candidate
        assert received is not None
        metrics = RuntimeMetrics()
        validation = ReceivedMultipartValidator(metrics).validate(received)
        assert validation.valid
        envelope = envelope_pb2.MessageEnvelope.FromString(received[1])
        assert envelope.publisher_session_id == publisher_session
        assert envelope.payload_type == "bounding_boxes_v1"
        echo_result = bounding_box_pb2.BoundingBoxResult.FromString(envelope.payload)
        assert echo_result.detections[0].confidence == pytest.approx(0.2)

        stopped = client.execute_command(_request("task_a", "stop"))
        assert stopped.resulting_state == ComponentState.READY
        invalid = client.execute_command(_request("task_a", "stop"))
        assert invalid.error_code == ErrorCode.INVALID_STATE_TRANSITION
        restarted = client.execute_command(_request("task_a", "start"))
        assert restarted.resulting_state == ComponentState.RUNNING
        writer.publish()
    finally:
        client.close()
        subscriber.close(linger=0)
        context.term()
        _stop(runner)
        _stop(router)
        _stop(broker)
        writer.close()


def test_real_shared_memory_reader_reaches_phase5_echo_process(tmp_path: Path) -> None:
    pub, sub, client_endpoint = _free_tcp_endpoint(), _free_tcp_endpoint(), _free_tcp_endpoint()
    module_endpoint = f"ipc://{tmp_path / 'phase6-control.sock'}"
    config = _config(pub, sub, client_endpoint, module_endpoint)
    camera = config.cameras["front_camera"]
    writer = SharedMemoryFrameWriter(
        "front_camera",
        camera.slot_capacity_bytes,
        UUID(int=750).bytes,
    )
    process_context = multiprocessing.get_context("spawn")
    identities = process_context.Queue()
    broker_signal_ready = process_context.Event()
    broker = process_context.Process(target=_run_broker, args=(pub, sub, broker_signal_ready), name="phase6-broker")
    router = process_context.Process(
        target=_run_router,
        args=(client_endpoint, module_endpoint, {"task_a"}),
        name="phase6-router",
    )
    runner = process_context.Process(
        target=_run_shared_memory_runner,
        args=(config, identities),
        name="phase6-module-runner",
    )
    context = zmq.Context()
    subscriber: zmq.Socket[bytes] = context.socket(zmq.SUB)
    subscriber.setsockopt(zmq.LINGER, 0)
    subscriber.setsockopt(zmq.SUBSCRIBE, b"cv.result.task_a.front_camera")
    subscriber.connect(sub)
    client = ControlClient(client_endpoint, acknowledgement_timeout_seconds=0.3)
    replacement: SharedMemoryFrameWriter | None = None
    try:
        writer.open()
        pixels = np.arange(18, dtype=np.uint8).reshape((2, 3, 3))
        writer.write(
            FrameWrite(
                pixels.tobytes(),
                3,
                2,
                9,
                PixelFormat.BGR8,
                0,
                time.time_ns(),
                time.monotonic_ns(),
            )
        )
        broker.start()
        _await_process_ready(broker, broker_signal_ready)
        router.start()
        runner.start()
        identities.get(timeout=5.0)
        _wait_state(client, "task_a", ComponentState.READY)
        started = client.execute_command(_request("task_a", "start"))
        assert started.resulting_state == ComponentState.RUNNING
        received = None
        deadline = time.monotonic() + 5.0
        frame_number = 1
        while received is None and time.monotonic() < deadline:
            writer.write(
                FrameWrite(
                    pixels.tobytes(),
                    3,
                    2,
                    9,
                    PixelFormat.BGR8,
                    frame_number,
                    time.time_ns(),
                    time.monotonic_ns(),
                )
            )
            frame_number += 1
            if subscriber.poll(100, zmq.POLLIN):
                received = subscriber.recv_multipart()
        assert received is not None
        envelope = envelope_pb2.MessageEnvelope.FromString(received[1])
        assert envelope.camera_session_id == UUID(int=750).bytes
        assert envelope.frame_number >= 0

        writer.close()
        time.sleep(0.1)
        _wait_state(client, "task_a", ComponentState.RUNNING)
        replacement = SharedMemoryFrameWriter(
            "front_camera",
            camera.slot_capacity_bytes,
            UUID(int=751).bytes,
        )
        replacement.open()
        replacement.write(
            FrameWrite(
                bytes([33]) * 18,
                3,
                2,
                9,
                PixelFormat.BGR8,
                0,
                time.time_ns(),
                time.monotonic_ns(),
            )
        )
        replacement_received = None
        deadline = time.monotonic() + 5.0
        replacement_number = 1
        while replacement_received is None and time.monotonic() < deadline:
            replacement.write(
                FrameWrite(
                    bytes([33]) * 18,
                    3,
                    2,
                    9,
                    PixelFormat.BGR8,
                    replacement_number,
                    time.time_ns(),
                    time.monotonic_ns(),
                )
            )
            replacement_number += 1
            if subscriber.poll(100, zmq.POLLIN):
                candidate = subscriber.recv_multipart()
                candidate_envelope = envelope_pb2.MessageEnvelope.FromString(candidate[1])
                if candidate_envelope.camera_session_id == UUID(int=751).bytes:
                    replacement_received = candidate_envelope
        assert replacement_received is not None
        assert replacement_received.frame_number >= 0
        _wait_state(client, "task_a", ComponentState.RUNNING)
    finally:
        client.close()
        subscriber.close(linger=0)
        context.term()
        _stop(runner)
        _stop(router)
        _stop(broker)
        writer.close()
        if replacement is not None:
            replacement.close()


def test_camera_a_crash_does_not_stop_camera_b_broker_router_or_module(tmp_path: Path) -> None:
    pub, sub, client_endpoint = _free_tcp_endpoint(), _free_tcp_endpoint(), _free_tcp_endpoint()
    module_endpoint = f"ipc://{tmp_path / 'isolation-control.sock'}"
    config = _config(pub, sub, client_endpoint, module_endpoint)
    base_camera = config.cameras["front_camera"].model_copy(update={"width": 3, "height": 2, "slot_capacity_bytes": 64})
    task = config.tasks["task_a"].model_copy(
        update={"input_camera": "camera_b", "publish_topic": "cv.result.task_a.camera_b"}
    )
    config = config.model_copy(
        update={"cameras": {"camera_a": base_camera, "camera_b": base_camera}, "tasks": {"task_a": task}}
    )
    process_context = multiprocessing.get_context("spawn")
    identities = process_context.Queue()
    broker_signal_ready = process_context.Event()
    broker = process_context.Process(target=_run_broker, args=(pub, sub, broker_signal_ready), name="isolation-broker")
    router = process_context.Process(
        target=_run_router,
        args=(client_endpoint, module_endpoint, {"task_a"}),
        name="isolation-router",
    )
    runner = process_context.Process(
        target=_run_shared_memory_runner,
        args=(config, identities, "camera_b"),
        name="isolation-module",
    )
    camera_a = process_context.Process(
        target=_run_camera_service,
        args=("camera_a", base_camera, uuid4().bytes),
        name="isolation-camera-a",
    )
    camera_b = process_context.Process(
        target=_run_camera_service,
        args=("camera_b", base_camera, uuid4().bytes),
        name="isolation-camera-b",
    )
    client = ControlClient(client_endpoint, acknowledgement_timeout_seconds=0.3)
    cleanup_a: SharedMemoryFrameWriter | None = None
    try:
        broker.start()
        _await_process_ready(broker, broker_signal_ready)
        router.start()
        runner.start()
        identities.get(timeout=5.0)
        deadline = time.monotonic() + 5.0
        starting = None
        while time.monotonic() < deadline:
            starting = client.send_command(_request("task_a", "get_status"))
            if starting.status == control_pb2.COMMAND_STATUS_COMPLETED:
                break
            time.sleep(0.02)
        assert starting is not None and starting.resulting_state == ComponentState.STARTING
        camera_a.start()
        camera_b.start()
        _wait_state(client, "task_a", ComponentState.READY)
        assert client.execute_command(_request("task_a", "start")).resulting_state == ComponentState.RUNNING
        camera_a.kill()
        camera_a.join(5.0)
        assert camera_a.exitcode is not None and camera_a.exitcode != 0
        assert camera_b.is_alive()
        assert broker.is_alive()
        assert router.is_alive()
        assert runner.is_alive()
        _wait_state(client, "task_a", ComponentState.RUNNING)
        cleanup_a = SharedMemoryFrameWriter("camera_a", 64, uuid4().bytes)
        cleanup_a.open()
    finally:
        client.close()
        if cleanup_a is not None:
            cleanup_a.close()
        if camera_a.is_alive():
            camera_a.kill()
            camera_a.join(2.0)
        _stop(camera_b)
        _stop(runner)
        _stop(router)
        _stop(broker)


def test_real_process_registration_failure_and_hung_worker_exit_75(tmp_path: Path) -> None:
    config = _config(
        _free_tcp_endpoint(),
        _free_tcp_endpoint(),
        _free_tcp_endpoint(),
        _free_tcp_endpoint(),
    )
    process_context = multiprocessing.get_context("spawn")
    registration_result = process_context.Queue()
    unavailable = process_context.Process(
        target=_run_unregistered, args=(config, tmp_path, "missing", registration_result)
    )
    unavailable.start()
    returned_code, registration_seconds = registration_result.get(timeout=8.0)
    unavailable.join(2.0)
    assert returned_code == ExitCode.TEMPORARY_FAILURE
    assert registration_seconds < 5.0
    assert unavailable.exitcode == ExitCode.TEMPORARY_FAILURE

    pub, sub, client_endpoint = _free_tcp_endpoint(), _free_tcp_endpoint(), _free_tcp_endpoint()
    module_endpoint = f"ipc://{tmp_path / 'hung-control.sock'}"
    hung_config = _config(pub, sub, client_endpoint, module_endpoint)
    writer = _FrameWriter(tmp_path / "hung")
    writer.publish()
    identities = process_context.Queue()
    broker_signal_ready = process_context.Event()
    broker = process_context.Process(target=_run_broker, args=(pub, sub, broker_signal_ready))
    router = process_context.Process(target=_run_router, args=(client_endpoint, module_endpoint, {"task_a"}))
    runner = process_context.Process(
        target=_run_runner,
        args=(hung_config, "task_a", tmp_path, "hung", identities, "hung", 0.2),
    )
    broker.start()
    _await_process_ready(broker, broker_signal_ready)
    router.start()
    runner.start()
    identities.get(timeout=5.0)
    client = ControlClient(client_endpoint, acknowledgement_timeout_seconds=0.3)
    try:
        _wait_state(client, "task_a", ComponentState.READY)
        assert client.execute_command(_request("task_a", "start")).status == control_pb2.COMMAND_STATUS_COMPLETED
        writer.publish()
        runner.join(5.0)
        assert runner.exitcode == ExitCode.TEMPORARY_FAILURE
        assert broker.is_alive() and router.is_alive()
    finally:
        client.close()
        _stop(runner, clean=False)
        _stop(router)
        _stop(broker)
        writer.close()


def test_real_process_twenty_consecutive_deadline_misses_exit_75(tmp_path: Path) -> None:
    pub, sub, client_endpoint = _free_tcp_endpoint(), _free_tcp_endpoint(), _free_tcp_endpoint()
    module_endpoint = f"ipc://{tmp_path / 'deadline-control.sock'}"
    config = _config(pub, sub, client_endpoint, module_endpoint)
    task = config.tasks["task_a"].model_copy(update={"processing_deadline_ms": 1, "max_input_fps": 240})
    config = config.model_copy(update={"tasks": {"task_a": task}})
    writer = _FrameWriter(tmp_path / "slow")
    writer.publish()
    process_context = multiprocessing.get_context("spawn")
    identities = process_context.Queue()
    broker_signal_ready = process_context.Event()
    broker = process_context.Process(target=_run_broker, args=(pub, sub, broker_signal_ready), name="deadline-broker")
    router = process_context.Process(
        target=_run_router, args=(client_endpoint, module_endpoint, {"task_a"}), name="deadline-router"
    )
    runner = process_context.Process(
        target=_run_runner,
        args=(config, "task_a", tmp_path, "slow", identities, "slow"),
        name="deadline-runner",
    )
    broker.start()
    _await_process_ready(broker, broker_signal_ready)
    router.start()
    runner.start()
    identities.get(timeout=5.0)
    client = ControlClient(client_endpoint, acknowledgement_timeout_seconds=0.3)
    try:
        _wait_state(client, "task_a", ComponentState.READY)
        assert client.execute_command(_request("task_a", "start")).status == control_pb2.COMMAND_STATUS_COMPLETED
        deadline = time.monotonic() + 8.0
        while runner.is_alive() and time.monotonic() < deadline:
            writer.publish()
            time.sleep(0.01)
        runner.join(2.0)
        assert runner.exitcode == ExitCode.TEMPORARY_FAILURE
        assert broker.is_alive() and router.is_alive()
    finally:
        client.close()
        _stop(runner, clean=False)
        _stop(router)
        _stop(broker)
        writer.close()


def test_real_concurrent_duplicate_executes_start_side_effect_once(tmp_path: Path) -> None:
    pub, sub, client_endpoint = _free_tcp_endpoint(), _free_tcp_endpoint(), _free_tcp_endpoint()
    module_endpoint = f"ipc://{tmp_path / 'duplicate-control.sock'}"
    config = _config(pub, sub, client_endpoint, module_endpoint)
    writer = _FrameWriter(tmp_path / "blocking")
    writer.publish()
    process_context = multiprocessing.get_context("spawn")
    identities = process_context.Queue()
    broker_signal_ready = process_context.Event()
    broker = process_context.Process(target=_run_broker, args=(pub, sub, broker_signal_ready), name="duplicate-broker")
    router = process_context.Process(
        target=_run_router, args=(client_endpoint, module_endpoint, {"task_a"}), name="duplicate-router"
    )
    runner = process_context.Process(
        target=_run_runner,
        args=(config, "task_a", tmp_path, "blocking", identities, "blocking_start"),
        name="duplicate-runner",
    )
    broker.start()
    _await_process_ready(broker, broker_signal_ready)
    router.start()
    runner.start()
    identities.get(timeout=5.0)
    first_client = ControlClient(client_endpoint, acknowledgement_timeout_seconds=0.3)
    second_client = ControlClient(client_endpoint, acknowledgement_timeout_seconds=0.3)
    command_id = UUID(int=711).bytes
    try:
        _wait_state(first_client, "task_a", ComponentState.READY)
        first = first_client.send_command(_request("task_a", "start", command_id=command_id))
        assert first.status == control_pb2.COMMAND_STATUS_RECEIVED
        duplicate = second_client.send_command(_request("task_a", "start", command_id=command_id))
        assert duplicate.error_code == ErrorCode.DUPLICATE_COMMAND_ID

        completion_deadline = time.monotonic() + 3.0
        final = None
        while time.monotonic() < completion_deadline:
            final = first_client.get_command_status("task_a", command_id)
            if final.status == control_pb2.COMMAND_STATUS_COMPLETED:
                break
            time.sleep(0.05)
        assert final is not None and final.status == control_pb2.COMMAND_STATUS_COMPLETED
        assert (tmp_path / "blocking.starts").read_text(encoding="utf-8").splitlines() == ["start"]
        static = _request("task_a", "set_dynamic_config")
        static.set_dynamic_config.fields["artifact.path"] = "/tmp/replacement.onnx"
        restart_required = first_client.execute_command(static)
        assert restart_required.error_code == ErrorCode.RESTART_REQUIRED
        assert restart_required.resulting_state == ComponentState.DEGRADED
    finally:
        first_client.close()
        second_client.close()
        _stop(runner)
        _stop(router)
        _stop(broker)
        writer.close()


def test_three_real_processing_failures_enter_error_but_control_survives(tmp_path: Path) -> None:
    pub, sub, client_endpoint = _free_tcp_endpoint(), _free_tcp_endpoint(), _free_tcp_endpoint()
    module_endpoint = f"ipc://{tmp_path / 'error-control.sock'}"
    config = _config(pub, sub, client_endpoint, module_endpoint)
    writer = _FrameWriter(tmp_path / "failing")
    writer.publish()
    process_context = multiprocessing.get_context("spawn")
    identities = process_context.Queue()
    broker_signal_ready = process_context.Event()
    broker = process_context.Process(target=_run_broker, args=(pub, sub, broker_signal_ready))
    router = process_context.Process(target=_run_router, args=(client_endpoint, module_endpoint, {"task_a"}))
    runner = process_context.Process(
        target=_run_runner,
        args=(config, "task_a", tmp_path, "failing", identities, "failing"),
    )
    broker.start()
    _await_process_ready(broker, broker_signal_ready)
    router.start()
    runner.start()
    identities.get(timeout=5.0)
    client = ControlClient(client_endpoint, acknowledgement_timeout_seconds=0.3)
    health_context = zmq.Context()
    health_subscriber: zmq.Socket[bytes] = health_context.socket(zmq.SUB)
    health_subscriber.setsockopt(zmq.LINGER, 0)
    health_subscriber.setsockopt(zmq.SUBSCRIBE, b"cv.health.task_a")
    health_subscriber.connect(sub)
    counter_path = tmp_path / "failing.failures"
    try:
        _wait_state(client, "task_a", ComponentState.READY)
        assert client.execute_command(_request("task_a", "start")).status == control_pb2.COMMAND_STATUS_COMPLETED
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            writer.publish()
            status = client.send_command(_request("task_a", "get_status"))
            if status.resulting_state == ComponentState.ERROR:
                break
            time.sleep(0.08)
        else:
            raise AssertionError("three processing failures did not enter ERROR")
        assert runner.is_alive()
        assert len(counter_path.read_text(encoding="utf-8").splitlines()) == 3
        for _ in range(4):
            writer.publish()
            time.sleep(0.08)
        assert len(counter_path.read_text(encoding="utf-8").splitlines()) == 3
        assert client.send_command(_request("task_a", "get_status")).resulting_state == ComponentState.ERROR
        error_health = None
        health_deadline = time.monotonic() + 3.0
        while time.monotonic() < health_deadline:
            if not health_subscriber.poll(250, zmq.POLLIN):
                continue
            frames = health_subscriber.recv_multipart()
            envelope = envelope_pb2.MessageEnvelope.FromString(frames[1])
            candidate = diagnostics_pb2.DiagnosticStatus.FromString(envelope.payload)
            if candidate.state == module_state_pb2.ERROR:
                error_health = candidate
                break
        assert error_health is not None
        assert error_health.last_error_code == ErrorCode.PROCESSING_FAILURE
        assert "failed frame" in error_health.last_error_message
        assert error_health.module.processing_exceptions == 3
        reset = client.execute_command(_request("task_a", "reset"))
        assert reset.status == control_pb2.COMMAND_STATUS_COMPLETED
        _wait_state(client, "task_a", ComponentState.READY)
        assert client.execute_command(_request("task_a", "start")).status == control_pb2.COMMAND_STATUS_COMPLETED
        second_error_deadline = time.monotonic() + 5.0
        while time.monotonic() < second_error_deadline:
            writer.publish()
            status = client.send_command(_request("task_a", "get_status"))
            if status.resulting_state == ComponentState.ERROR:
                break
            time.sleep(0.08)
        else:
            raise AssertionError("reset module did not re-enter ERROR after three new failures")
    finally:
        client.close()
        health_subscriber.close(linger=0)
        health_context.term()
        _stop(runner)
        _stop(router)
        _stop(broker)
        writer.close()


def test_sigterm_during_blocked_command_exits_75_within_five_seconds(tmp_path: Path) -> None:
    pub, sub, client_endpoint = _free_tcp_endpoint(), _free_tcp_endpoint(), _free_tcp_endpoint()
    module_endpoint = f"ipc://{tmp_path / 'blocked-command-control.sock'}"
    config = _config(pub, sub, client_endpoint, module_endpoint)
    writer = _FrameWriter(tmp_path / "blocked-command")
    writer.publish()
    process_context = multiprocessing.get_context("spawn")
    identities = process_context.Queue()
    router = process_context.Process(
        target=_run_router,
        args=(client_endpoint, module_endpoint, {"task_a"}),
        name="blocked-command-router",
    )
    runner = process_context.Process(
        target=_run_runner,
        args=(config, "task_a", tmp_path, "blocked-command", identities, "shutdown_blocking_start"),
        name="blocked-command-runner",
    )
    router.start()
    runner.start()
    identities.get(timeout=15.0)
    client = ControlClient(client_endpoint, acknowledgement_timeout_seconds=0.3)
    entered = tmp_path / "blocked-command.entered"
    try:
        _wait_state(client, "task_a", ComponentState.READY)
        received = client.send_command(_request("task_a", "start"))
        assert received.status == control_pb2.COMMAND_STATUS_RECEIVED
        entered_deadline = time.monotonic() + 3.0
        while not entered.exists() and time.monotonic() < entered_deadline:
            time.sleep(0.02)
        assert entered.exists()
        assert entered.read_text(encoding="utf-8") == "entered\n"

        shutdown_started = time.monotonic()
        runner.terminate()
        runner.join(5.0)
        shutdown_seconds = time.monotonic() - shutdown_started
        assert not runner.is_alive()
        assert runner.exitcode == ExitCode.TEMPORARY_FAILURE
        assert shutdown_seconds < 5.0
        assert router.is_alive()
    finally:
        client.close()
        _stop(runner, clean=False)
        _stop(router)
        writer.close()


def test_kill_restart_isolates_modules_and_retires_old_session(tmp_path: Path) -> None:
    pub, sub, client_endpoint = _free_tcp_endpoint(), _free_tcp_endpoint(), _free_tcp_endpoint()
    module_endpoint = f"ipc://{tmp_path / 'isolation-control.sock'}"
    config = _config(pub, sub, client_endpoint, module_endpoint, task_ids=("task_a", "task_b"))
    writer_a, writer_b = _FrameWriter(tmp_path / "task_a"), _FrameWriter(tmp_path / "task_b")
    writer_a.publish()
    writer_b.publish()
    process_context = multiprocessing.get_context("spawn")
    identities_a, identities_b = process_context.Queue(), process_context.Queue()
    broker_signal_ready = process_context.Event()
    broker = process_context.Process(target=_run_broker, args=(pub, sub, broker_signal_ready), name="isolation-broker")
    router = process_context.Process(
        target=_run_router, args=(client_endpoint, module_endpoint, {"task_a", "task_b"}), name="isolation-router"
    )
    runner_a = process_context.Process(
        target=_run_runner, args=(config, "task_a", tmp_path, "task_a", identities_a), name="module-a"
    )
    runner_b = process_context.Process(
        target=_run_runner, args=(config, "task_b", tmp_path, "task_b", identities_b), name="module-b"
    )
    broker.start()
    _await_process_ready(broker, broker_signal_ready)
    router.start()
    runner_a.start()
    runner_b.start()
    old_module_session, old_publisher_session = identities_a.get(timeout=5.0)
    identities_b.get(timeout=5.0)
    client = ControlClient(client_endpoint, acknowledgement_timeout_seconds=0.3)
    restarted = None
    raw_context = zmq.Context()
    stale: zmq.Socket[bytes] | None = None
    try:
        _wait_state(client, "task_a", ComponentState.READY)
        _wait_state(client, "task_b", ComponentState.READY)
        assert client.execute_command(_request("task_b", "start")).status == control_pb2.COMMAND_STATUS_COMPLETED
        runner_a.kill()
        runner_a.join(5.0)
        assert runner_a.exitcode != 0
        assert runner_b.is_alive() and broker.is_alive() and router.is_alive()
        _wait_state(client, "task_b", ComponentState.RUNNING)
        deadline = time.monotonic() + 3.0
        while time.monotonic() < deadline:
            unavailable = client.send_command(_request("task_a", "get_status"))
            if unavailable.error_code == ErrorCode.TARGET_UNAVAILABLE:
                break
            time.sleep(0.05)
        else:
            raise AssertionError("killed task_a did not become unavailable")

        identities_restart = process_context.Queue()
        restarted = process_context.Process(
            target=_run_runner,
            args=(config, "task_a", tmp_path, "task_a", identities_restart),
            name="module-a-restarted",
        )
        restarted.start()
        new_module_session, new_publisher_session = identities_restart.get(timeout=5.0)
        assert new_module_session != old_module_session
        assert new_publisher_session != old_publisher_session
        _wait_state(client, "task_a", ComponentState.READY)

        stale = raw_context.socket(zmq.DEALER)
        configure_dealer(stale, f"module:task_a:{UUID(bytes=old_module_session)}")
        stale.connect(module_endpoint)
        registration = registration_pb2.ModuleRegistration(
            module_id="task_a",
            task_id="task_a",
            module_session_id=old_module_session,
            supported_command_types=["get_status"],
            current_state=1,
            process_id=1,
            host_device_id="rov_pi5",
        )
        assert stale.poll(5_000, zmq.POLLOUT)
        stale.send_multipart([REGISTER_MODULE, registration.SerializeToString()])
        assert stale.poll(1_000, zmq.POLLIN)
        frames = stale.recv_multipart()
        assert frames[0] == REGISTER_MODULE_RESPONSE
        assert not registration_pb2.ModuleRegistrationResponse.FromString(frames[1]).accepted

        restarted.kill()
        restarted.join(5.0)
        assert restarted.exitcode != 0
        restarted = None
        unavailable_deadline = time.monotonic() + 3.0
        while time.monotonic() < unavailable_deadline:
            stale.send_multipart([MODULE_HEARTBEAT, old_module_session])
            unavailable = client.send_command(_request("task_a", "get_status"))
            if unavailable.error_code == ErrorCode.TARGET_UNAVAILABLE:
                break
            time.sleep(0.05)
        else:
            raise AssertionError("retired heartbeat kept the replacement session available")
        _wait_state(client, "task_b", ComponentState.RUNNING)
    finally:
        client.close()
        if stale is not None:
            stale.close(linger=0)
        raw_context.term()
        if restarted is not None:
            _stop(restarted)
        _stop(runner_b)
        _stop(router)
        _stop(broker)
        writer_a.close()
        writer_b.close()


def test_installed_module_runner_uses_exit_64_for_bad_arguments() -> None:
    installed_script = Path(sys.executable).with_name("purdue-cv-module-runner")
    assert installed_script.is_file()
    completed = subprocess.run(
        [str(installed_script)],
        check=False,
        capture_output=True,
        text=True,
        timeout=5.0,
    )
    assert completed.returncode == ExitCode.INVALID_ARGUMENTS
    assert "--task" in completed.stderr
