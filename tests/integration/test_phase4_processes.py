"""Real-process ZeroMQ integration for the realizable Phase 4 topology."""

from __future__ import annotations

import multiprocessing
import socket
import time
from pathlib import Path
from uuid import UUID, uuid4

import pytest
import zmq
from purdue_rov.cv.v1 import control_pb2, diagnostics_pb2, module_state_pb2, registration_pb2

from purdue_rov_cv.messaging.broker import DataBrokerService
from purdue_rov_cv.messaging.client import ControlClient
from purdue_rov_cv.messaging.fake_module import FakeModuleService
from purdue_rov_cv.messaging.protocol import (
    COMMAND_REQUEST,
    COMMAND_RESPONSE,
    REGISTER_MODULE,
    REGISTER_MODULE_RESPONSE,
)
from purdue_rov_cv.messaging.router import ControlRouterService
from purdue_rov_cv.messaging.sockets import configure_dealer
from purdue_rov_cv.runtime import EnvelopeBuilder, PublisherSequence, ReceivedMultipartValidator, RuntimeMetrics
from purdue_rov_cv.runtime.exit_codes import ExitCode
from purdue_rov_cv.runtime.state import ComponentState
from purdue_rov_cv.wire.errors import ErrorCode


def _free_tcp_endpoint() -> str:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as candidate:
        candidate.bind(("127.0.0.1", 0))
        port = candidate.getsockname()[1]
    return f"tcp://127.0.0.1:{port}"


def _run_broker(publisher_endpoint: str, subscriber_endpoint: str) -> None:
    DataBrokerService(publisher_endpoint, subscriber_endpoint, install_signals=True).run()


_SUPPRESSED_RESPONSE_COMMAND_ID = UUID(int=701).bytes


class _SuppressingFakeModule(FakeModuleService):
    """Cache one known result but simulate loss before it reaches the router."""

    def _send_response(
        self,
        socket: zmq.Socket[bytes],
        response: control_pb2.CommandResponse,
        *,
        cache_result: bool,
    ) -> None:
        if cache_result and response.command_id == _SUPPRESSED_RESPONSE_COMMAND_ID:
            self.cache.put(response)
            return
        super()._send_response(socket, response, cache_result=cache_result)


def _run_router(client_endpoint: str, module_endpoint: str, heartbeat_expiry_seconds: float, ready) -> None:
    ControlRouterService(
        client_endpoint,
        module_endpoint,
        device_id="rov_pi5",
        allowed_module_ids={"gate_detection"},
        heartbeat_expiry_seconds=heartbeat_expiry_seconds,
        ready_signal=ready,
        install_signals=True,
    ).run()


def _run_module(module_endpoint: str) -> None:
    service = FakeModuleService(
        module_endpoint,
        module_id="gate_detection",
        task_id="gate_detection",
        host_device_id="rov_pi5",
        heartbeat_interval_seconds=0.1,
        registration_retry_seconds=0.1,
        registration_ack_timeout_seconds=0.05,
        install_signals=True,
    )
    raise SystemExit(int(service.run()))


def _run_module_with_suppressed_response(module_endpoint: str) -> None:
    service = _SuppressingFakeModule(
        module_endpoint,
        module_id="gate_detection",
        task_id="gate_detection",
        host_device_id="rov_pi5",
        heartbeat_interval_seconds=0.1,
        registration_retry_seconds=0.1,
        registration_ack_timeout_seconds=0.05,
        install_signals=True,
    )
    raise SystemExit(int(service.run()))


def _stop_process(process: multiprocessing.Process) -> float:
    started = time.monotonic()
    process.terminate()
    process.join(5.0)
    elapsed = time.monotonic() - started
    assert not process.is_alive()
    assert process.exitcode == ExitCode.CLEAN_SHUTDOWN
    assert elapsed < 5.0
    print(f"{process.name}_shutdown_seconds={elapsed:.3f}")
    return elapsed


def _request(command_type: str, *, command_id: bytes | None = None) -> control_pb2.CommandRequest:
    request = control_pb2.CommandRequest(
        command_id=command_id or uuid4().bytes,
        target_id="gate_detection",
        issued_time_unix_ns=time.time_ns(),
    )
    getattr(request, command_type).SetInParent()
    return request


def _wait_for_registered(client: ControlClient) -> control_pb2.CommandResponse:
    deadline = time.monotonic() + 5.0
    last: control_pb2.CommandResponse | None = None
    while time.monotonic() < deadline:
        last = client.send_command(_request("get_status"))
        if last.status == control_pb2.COMMAND_STATUS_COMPLETED:
            return last
        assert last.error_code in {
            ErrorCode.TARGET_UNAVAILABLE,
            ErrorCode.COMMAND_OUTCOME_UNKNOWN,
        }
    raise AssertionError(f"module did not register: {last}")


def test_real_process_broker_forwards_valid_multipart_and_shuts_down() -> None:
    publisher_endpoint = _free_tcp_endpoint()
    subscriber_endpoint = _free_tcp_endpoint()
    process = multiprocessing.get_context("spawn").Process(
        target=_run_broker,
        args=(publisher_endpoint, subscriber_endpoint),
        name="phase4-broker",
    )
    process.start()
    context = zmq.Context()
    publisher: zmq.Socket[bytes] = context.socket(zmq.PUB)
    subscriber: zmq.Socket[bytes] = context.socket(zmq.SUB)
    publisher.setsockopt(zmq.LINGER, 0)
    subscriber.setsockopt(zmq.LINGER, 0)
    subscriber.setsockopt(zmq.SUBSCRIBE, b"cv.health.camera")
    publisher.connect(publisher_endpoint)
    subscriber.connect(subscriber_endpoint)
    metrics = RuntimeMetrics()
    receiver = ReceivedMultipartValidator(metrics)
    builder = EnvelopeBuilder(PublisherSequence(), unix_time_ns=lambda: 1, monotonic_ns=lambda: 1)
    received: list[bytes] | None = None
    try:
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline and received is None:
            payload = diagnostics_pb2.DiagnosticStatus(source_id="camera")
            built = builder.build(
                topic="cv.health.camera",
                payload_type="diagnostic_status_v1",
                payload=payload,
                task_id="",
                source_id="camera",
            )
            publisher.send_multipart(built.frames)
            if subscriber.poll(100, zmq.POLLIN):
                received = subscriber.recv_multipart()
        assert received is not None
        validation = receiver.validate(received)
        assert validation.valid
        assert received == list(built.frames)
        assert metrics.snapshot().values["messages_received"] == 1
    finally:
        publisher.close(linger=0)
        subscriber.close(linger=0)
        context.term()
        _stop_process(process)


def test_real_process_router_module_client_commands_heartbeat_and_shutdown(tmp_path: Path) -> None:
    client_endpoint = _free_tcp_endpoint()
    module_endpoint = f"ipc://{tmp_path / 'module-control.sock'}"
    process_context = multiprocessing.get_context("spawn")
    ready = process_context.Event()
    router = process_context.Process(
        target=_run_router,
        args=(client_endpoint, module_endpoint, 0.35, ready),
        name="phase4-router",
    )
    module = process_context.Process(
        target=_run_module,
        args=(module_endpoint,),
        name="phase4-module",
    )
    router.start()
    assert ready.wait(10.0)
    module.start()
    client = ControlClient(client_endpoint, acknowledgement_timeout_seconds=0.2)
    raw_context = zmq.Context()
    malformed: zmq.Socket[bytes] = raw_context.socket(zmq.DEALER)
    malformed.setsockopt(zmq.LINGER, 0)
    malformed.setsockopt(zmq.IDENTITY, f"client:{uuid4()}".encode())
    malformed.connect(client_endpoint)
    rogue_module: zmq.Socket[bytes] = raw_context.socket(zmq.DEALER)
    rogue_session = uuid4()
    configure_dealer(rogue_module, f"module:other:{rogue_session}")
    rogue_module.connect(module_endpoint)
    try:
        registered = _wait_for_registered(client)
        assert registered.resulting_state == ComponentState.READY

        inconsistent = registration_pb2.ModuleRegistration(
            module_id="gate_detection",
            task_id="gate_detection",
            module_session_id=rogue_session.bytes,
            supported_command_types=["get_status"],
            current_state=module_state_pb2.READY,
            process_id=99,
            host_device_id="rov_pi5",
        )
        assert rogue_module.poll(5_000, zmq.POLLOUT)
        rogue_module.send_multipart([REGISTER_MODULE, inconsistent.SerializeToString()])
        assert rogue_module.poll(500, zmq.POLLIN)
        registration_frames = rogue_module.recv_multipart()
        assert registration_frames[0] == REGISTER_MODULE_RESPONSE
        registration_reply = registration_pb2.ModuleRegistrationResponse.FromString(registration_frames[1])
        assert not registration_reply.accepted
        assert registration_reply.error_code == ErrorCode.INVALID_ENVELOPE

        malformed.send_multipart([b"unsupported-frame"])
        malformed.send_multipart([COMMAND_REQUEST, b"\xff"])
        assert _wait_for_registered(client).status == control_pb2.COMMAND_STATUS_COMPLETED

        start = _request("start", command_id=UUID(int=101).bytes)
        first = client.send_command(start)
        assert first.status == control_pb2.COMMAND_STATUS_COMPLETED
        assert first.resulting_state == ComponentState.RUNNING
        assert "count=1" in first.message

        duplicate = client.send_command(start)
        assert duplicate.status == control_pb2.COMMAND_STATUS_REJECTED
        assert duplicate.error_code == ErrorCode.DUPLICATE_COMMAND_ID

        cached = client.get_command_status("gate_detection", start.command_id)
        assert cached.status == control_pb2.COMMAND_STATUS_COMPLETED
        assert "count=1" in cached.message

        invalid_state = client.send_command(_request("start"))
        assert invalid_state.status == control_pb2.COMMAND_STATUS_REJECTED
        assert invalid_state.error_code == ErrorCode.INVALID_STATE_TRANSITION

        missing = client.get_command_status("gate_detection", UUID(int=999).bytes)
        assert missing.status == control_pb2.COMMAND_STATUS_REJECTED
        assert missing.error_code == ErrorCode.INVALID_COMMAND

        heartbeat_proof_deadline = time.monotonic() + 0.5
        while time.monotonic() < heartbeat_proof_deadline:
            response = client.send_command(_request("get_status"))
            assert response.error_code != ErrorCode.TARGET_UNAVAILABLE

        _stop_process(module)
        unavailable_deadline = time.monotonic() + 2.0
        while True:
            unavailable = client.send_command(_request("get_status"))
            if unavailable.error_code == ErrorCode.TARGET_UNAVAILABLE:
                break
            assert time.monotonic() < unavailable_deadline
    finally:
        client.close()
        malformed.close(linger=0)
        rogue_module.close(linger=0)
        raw_context.term()
        if module.is_alive():
            _stop_process(module)
        _stop_process(router)


def test_real_socket_router_preserves_origin_when_two_clients_reuse_an_inflight_id(tmp_path: Path) -> None:
    client_endpoint = _free_tcp_endpoint()
    module_endpoint = f"ipc://{tmp_path / 'multi-client-module-control.sock'}"
    process_context = multiprocessing.get_context("spawn")
    ready = process_context.Event()
    router = process_context.Process(
        target=_run_router,
        args=(client_endpoint, module_endpoint, 3.5, ready),
        name="phase4-multi-client-router",
    )
    router.start()
    assert ready.wait(10.0)
    context = zmq.Context()
    module: zmq.Socket[bytes] = context.socket(zmq.DEALER)
    client_a: zmq.Socket[bytes] = context.socket(zmq.DEALER)
    client_b: zmq.Socket[bytes] = context.socket(zmq.DEALER)
    session = UUID(int=801)
    configure_dealer(module, f"module:gate_detection:{session}")
    configure_dealer(client_a, f"client:{uuid4()}")
    configure_dealer(client_b, f"client:{uuid4()}")
    module.connect(module_endpoint)
    client_a.connect(client_endpoint)
    client_b.connect(client_endpoint)
    try:
        registration = registration_pb2.ModuleRegistration(
            module_id="gate_detection",
            task_id="gate_detection",
            module_session_id=session.bytes,
            supported_command_types=["start"],
            current_state=module_state_pb2.READY,
            process_id=801,
            host_device_id="rov_pi5",
        )
        assert module.poll(5_000, zmq.POLLOUT)
        module.send_multipart([REGISTER_MODULE, registration.SerializeToString()])
        assert module.poll(500, zmq.POLLIN)
        registration_frames = module.recv_multipart()
        assert registration_frames[0] == REGISTER_MODULE_RESPONSE
        assert registration_pb2.ModuleRegistrationResponse.FromString(registration_frames[1]).accepted

        request = _request("start", command_id=UUID(int=802).bytes)
        assert client_a.poll(5_000, zmq.POLLOUT)
        client_a.send_multipart([COMMAND_REQUEST, request.SerializeToString()])
        assert module.poll(500, zmq.POLLIN)
        forwarded = module.recv_multipart()
        assert forwarded == [COMMAND_REQUEST, request.SerializeToString()]

        assert client_b.poll(5_000, zmq.POLLOUT)
        client_b.send_multipart([COMMAND_REQUEST, request.SerializeToString()])
        assert client_b.poll(500, zmq.POLLIN)
        duplicate_frames = client_b.recv_multipart()
        assert duplicate_frames[0] == COMMAND_RESPONSE
        duplicate = control_pb2.CommandResponse.FromString(duplicate_frames[1])
        assert duplicate.error_code == ErrorCode.DUPLICATE_COMMAND_ID
        assert not module.poll(100, zmq.POLLIN)

        completed = control_pb2.CommandResponse(
            command_id=request.command_id,
            target_id=request.target_id,
            status=control_pb2.COMMAND_STATUS_COMPLETED,
            resulting_state=ComponentState.RUNNING,
            response_time_unix_ns=time.time_ns(),
        )
        module.send_multipart([COMMAND_RESPONSE, completed.SerializeToString()])
        assert client_a.poll(500, zmq.POLLIN)
        completed_frames = client_a.recv_multipart()
        assert completed_frames[0] == COMMAND_RESPONSE
        assert control_pb2.CommandResponse.FromString(completed_frames[1]) == completed
    finally:
        module.close(linger=0)
        client_a.close(linger=0)
        client_b.close(linger=0)
        context.term()
        _stop_process(router)


def test_real_process_uncertain_response_recovers_cached_outcome_once(tmp_path: Path) -> None:
    client_endpoint = _free_tcp_endpoint()
    module_endpoint = f"ipc://{tmp_path / 'status-recovery-module-control.sock'}"
    process_context = multiprocessing.get_context("spawn")
    ready = process_context.Event()
    router = process_context.Process(
        target=_run_router,
        args=(client_endpoint, module_endpoint, 0.35, ready),
        name="phase4-status-router",
    )
    module = process_context.Process(
        target=_run_module_with_suppressed_response,
        args=(module_endpoint,),
        name="phase4-status-module",
    )
    router.start()
    assert ready.wait(10.0)
    module.start()
    client = ControlClient(client_endpoint, acknowledgement_timeout_seconds=0.2)
    try:
        _wait_for_registered(client)
        command = _request("start", command_id=_SUPPRESSED_RESPONSE_COMMAND_ID)
        unknown = client.send_command(command)
        assert unknown.status == control_pb2.COMMAND_STATUS_OUTCOME_UNKNOWN
        assert unknown.error_code == ErrorCode.COMMAND_OUTCOME_UNKNOWN

        recovered = client.get_command_status(command.target_id, command.command_id)
        assert recovered.status == control_pb2.COMMAND_STATUS_COMPLETED
        assert recovered.resulting_state == ComponentState.RUNNING
        assert "count=1" in recovered.message
        with pytest.raises(RuntimeError, match="only one status query"):
            client.get_command_status(command.target_id, command.command_id)
    finally:
        client.close()
        _stop_process(module)
        _stop_process(router)
