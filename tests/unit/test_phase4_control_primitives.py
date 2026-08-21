"""Focused Phase 4 socket, registry, cache, retry, and client behavior tests."""

from __future__ import annotations

import errno
import io
from dataclasses import dataclass
from pathlib import Path
from uuid import UUID, uuid4

import pytest
import zmq
from purdue_rov.cv.v1 import control_pb2, diagnostics_pb2, module_state_pb2, registration_pb2

from purdue_rov_cv.config.loader import load_config
from purdue_rov_cv.messaging import client as client_module
from purdue_rov_cv.messaging import entrypoints as service_entrypoints
from purdue_rov_cv.messaging.broker import DataBrokerService
from purdue_rov_cv.messaging.cache import CommandStatusCache
from purdue_rov_cv.messaging.client import ControlClient
from purdue_rov_cv.messaging.deadlines import COMMAND_COMPLETION_SECONDS, completion_timeout_seconds
from purdue_rov_cv.messaging.entrypoints import broker_entrypoint
from purdue_rov_cv.messaging.fake_module import FakeModuleService, RegistrationRetryController
from purdue_rov_cv.messaging.protocol import COMMAND_REQUEST, COMMAND_RESPONSE, RouterMessage
from purdue_rov_cv.messaging.registry import ModuleRegistrationRegistry
from purdue_rov_cv.messaging.router import ControlRouterService, PendingRoute
from purdue_rov_cv.messaging.sockets import (
    BROKER_HWM,
    CONTROL_RECEIVE_TIMEOUT_MS,
    CONTROL_ROUTER_HWM,
    CONTROL_SEND_TIMEOUT_MS,
    DEALER_HWM,
    TRANSPORT_MAX_MESSAGE_BYTES,
    configure_dealer,
    configure_router,
    configure_xpub,
    configure_xsub,
)
from purdue_rov_cv.runtime import EnvelopeBuilder, PublisherSequence, ReceivedMultipartValidator, RuntimeMetrics
from purdue_rov_cv.runtime.exit_codes import ExitCode
from purdue_rov_cv.runtime.json_logging import configure_json_logger
from purdue_rov_cv.wire.errors import ErrorCode
from purdue_rov_cv.wire.validators import COMMAND_ONEOF_NAMES


@dataclass
class FakeClock:
    now: float = 0.0

    def __call__(self) -> float:
        return self.now


def _registration(session_id: bytes = b"s" * 16) -> registration_pb2.ModuleRegistration:
    return registration_pb2.ModuleRegistration(
        module_id="gate_detection",
        task_id="gate_detection",
        module_session_id=session_id,
        supported_command_types=sorted(COMMAND_ONEOF_NAMES),
        current_state=module_state_pb2.READY,
        process_id=123,
        host_device_id="rov_pi5",
    )


def _command(command_id: bytes | None = None, *, command_type: str = "start") -> control_pb2.CommandRequest:
    request = control_pb2.CommandRequest(
        command_id=command_id or uuid4().bytes,
        target_id="gate_detection",
        issued_time_unix_ns=1,
    )
    getattr(request, command_type).SetInParent()
    return request


def test_effective_broker_router_and_dealer_socket_options() -> None:
    context = zmq.Context()
    sockets: list[zmq.Socket[bytes]] = []
    try:
        xsub = context.socket(zmq.XSUB)
        xpub = context.socket(zmq.XPUB)
        router = context.socket(zmq.ROUTER)
        dealer = context.socket(zmq.DEALER)
        sockets.extend((xsub, xpub, router, dealer))
        configure_xsub(xsub)
        configure_xpub(xpub)
        configure_router(router, "tcp://127.0.0.1:5560")
        configure_dealer(dealer, f"client:{uuid4()}")

        for socket in (xsub, xpub):
            assert socket.getsockopt(zmq.RCVHWM) == BROKER_HWM
            assert socket.getsockopt(zmq.SNDHWM) == BROKER_HWM
            assert socket.getsockopt(zmq.LINGER) == 0
            assert socket.getsockopt(zmq.MAXMSGSIZE) == TRANSPORT_MAX_MESSAGE_BYTES
            assert socket.getsockopt(zmq.TCP_KEEPALIVE) == 1
        # libzmq exposes XPUB_VERBOSE as set-only on supported versions; the
        # successful configure_xpub call above is the reliable check here.
        assert router.getsockopt(zmq.RCVHWM) == CONTROL_ROUTER_HWM
        assert router.getsockopt(zmq.SNDHWM) == CONTROL_ROUTER_HWM
        assert router.getsockopt(zmq.RCVTIMEO) == CONTROL_RECEIVE_TIMEOUT_MS
        assert router.getsockopt(zmq.SNDTIMEO) == CONTROL_SEND_TIMEOUT_MS
        assert router.getsockopt(zmq.LINGER) == 0
        # ROUTER_MANDATORY is likewise set-only in this libzmq build.
        assert router.getsockopt(zmq.TCP_KEEPALIVE) == 1
        assert dealer.getsockopt(zmq.RCVHWM) == DEALER_HWM
        assert dealer.getsockopt(zmq.SNDHWM) == DEALER_HWM
        assert dealer.getsockopt(zmq.RCVTIMEO) == CONTROL_RECEIVE_TIMEOUT_MS
        assert dealer.getsockopt(zmq.SNDTIMEO) == CONTROL_SEND_TIMEOUT_MS
        assert dealer.getsockopt(zmq.LINGER) == 0
        assert dealer.getsockopt(zmq.IMMEDIATE) == 1
        assert dealer.getsockopt(zmq.RECONNECT_IVL) == 250
        assert dealer.getsockopt(zmq.RECONNECT_IVL_MAX) == 2_000
    finally:
        for socket in sockets:
            socket.close(linger=0)
        context.term()


def test_services_take_all_endpoints_and_identities_from_authoritative_config() -> None:
    config = load_config(Path(__file__).parents[2] / "config" / "mission.yaml", environ={})
    broker = DataBrokerService.from_config(config)
    router = ControlRouterService.from_config(config)
    assert broker.publisher_endpoint == config.messaging.broker.publisher_endpoint
    assert broker.subscriber_endpoint == config.messaging.broker.subscriber_endpoint
    assert router.client_endpoint == config.messaging.control.client_endpoint
    assert router.module_endpoint == config.messaging.control.module_endpoint
    assert router.device_id == config.device.device_id
    assert router.allowed_module_ids == frozenset(config.tasks)


def test_broker_forward_path_preserves_multipart_and_updates_success_metrics() -> None:
    context = zmq.Context()
    source_peer: zmq.Socket[bytes] = context.socket(zmq.PAIR)
    source: zmq.Socket[bytes] = context.socket(zmq.PAIR)
    destination: zmq.Socket[bytes] = context.socket(zmq.PAIR)
    destination_peer: zmq.Socket[bytes] = context.socket(zmq.PAIR)
    source_peer.bind("inproc://phase4-forward-source")
    source.connect("inproc://phase4-forward-source")
    destination.bind("inproc://phase4-forward-destination")
    destination_peer.connect("inproc://phase4-forward-destination")
    metrics = RuntimeMetrics()
    service = DataBrokerService("unused", "unused", metrics=metrics)
    try:
        expected = [b"topic", b"serialized-envelope"]
        source_peer.send_multipart(expected)
        service._forward(source, destination)
        assert destination_peer.recv_multipart() == expected
        snapshot = metrics.snapshot().values
        assert snapshot["messages_received"] == 1
        assert snapshot["messages_sent"] == 1
    finally:
        for socket in (source_peer, source, destination, destination_peer):
            socket.close(linger=0)
        context.term()


@pytest.mark.parametrize(
    "identity",
    ["client:not-a-uuid", "module::123e4567-e89b-12d3-a456-426614174000", "client:" + "x" * 129],
)
def test_dealer_configuration_rejects_invalid_and_overlong_identity(identity: str) -> None:
    context = zmq.Context()
    socket: zmq.Socket[bytes] = context.socket(zmq.DEALER)
    try:
        with pytest.raises(ValueError):
            configure_dealer(socket, identity)
    finally:
        socket.close(linger=0)
        context.term()


def test_registry_replaces_new_session_and_rejects_retired_session() -> None:
    clock = FakeClock()
    registry = ModuleRegistrationRegistry(monotonic=clock)
    first = _registration(b"a" * 16)
    second = _registration(b"b" * 16)
    assert registry.register(first, b"route-a").accepted
    clock.now = 1.0
    decision = registry.register(second, b"route-b")
    assert decision.accepted and decision.replaced_session
    assert registry.resolve("gate_detection").session_id == b"b" * 16
    assert not registry.register(first, b"route-a").accepted
    assert not registry.heartbeat("gate_detection", b"a" * 16, b"route-a")


def test_registry_never_forgets_a_retired_session_after_many_restarts() -> None:
    registry = ModuleRegistrationRegistry()
    first_session = UUID(int=1).bytes
    assert registry.register(_registration(first_session), b"route-1").accepted
    for value in range(2, 70):
        session_id = UUID(int=value).bytes
        assert registry.register(_registration(session_id), f"route-{value}".encode()).accepted
    assert not registry.register(_registration(first_session), b"route-1").accepted
    assert not registry.heartbeat("gate_detection", first_session, b"route-1")


def test_heartbeat_expiry_exact_boundary_and_stale_refresh() -> None:
    clock = FakeClock()
    registry = ModuleRegistrationRegistry(monotonic=clock)
    registry.register(_registration(), b"route")
    clock.now = 3.499
    assert registry.resolve("gate_detection").available
    clock.now = 3.5
    assert not registry.resolve("gate_detection").available
    assert not registry.heartbeat("gate_detection", b"x" * 16, b"stale")
    clock.now = 3.501
    assert registry.heartbeat("gate_detection", b"s" * 16, b"route")
    assert registry.resolve("gate_detection").available


def test_command_cache_ttl_capacity_copy_and_fifo_eviction() -> None:
    clock = FakeClock()
    cache = CommandStatusCache(ttl_seconds=10.0, capacity=2, monotonic=clock)
    ids = [UUID(int=value).bytes for value in (1, 2, 3)]
    for command_id in ids:
        cache.put(
            control_pb2.CommandResponse(
                command_id=command_id,
                target_id="gate_detection",
                status=control_pb2.COMMAND_STATUS_COMPLETED,
            )
        )
        clock.now += 1.0
    assert cache.get(ids[0]) is None
    result = cache.get(ids[1])
    assert result is not None
    result.message = "mutated"
    assert cache.get(ids[1]).message == ""
    clock.now = 11.0
    assert cache.get(ids[1]) is None
    assert cache.get(ids[2]) is not None
    clock.now = 12.0
    assert len(cache) == 0


def test_command_cache_default_capacity_is_1024() -> None:
    cache = CommandStatusCache()
    for value in range(1, 1_026):
        cache.put(
            control_pb2.CommandResponse(
                command_id=UUID(int=value).bytes,
                target_id="gate_detection",
                status=control_pb2.COMMAND_STATUS_COMPLETED,
            )
        )
    assert len(cache) == 1_024
    assert cache.get(UUID(int=1).bytes) is None
    assert cache.get(UUID(int=2).bytes) is not None


def test_registration_retry_timing_and_ten_attempt_escalation_without_sleep() -> None:
    clock = FakeClock()
    retry = RegistrationRetryController(monotonic=clock)
    for attempt in range(10):
        assert retry.should_attempt()
        retry.attempted()
        clock.now += 0.499
        assert retry.update() is None
        clock.now += 0.001
        escalation = retry.update()
        if attempt < 9:
            assert escalation is None
            clock.now += 0.5
        else:
            assert escalation is not None
            assert escalation.exit_code is ExitCode.TEMPORARY_FAILURE
    assert retry.state.attempts == 10


def test_all_canonical_command_deadlines_and_requested_timeout_clamping() -> None:
    assert set(COMMAND_COMPLETION_SECONDS) == COMMAND_ONEOF_NAMES
    assert COMMAND_COMPLETION_SECONDS == {
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
    request = _command(command_type="start")
    assert completion_timeout_seconds(request) == 5.0
    request.requested_timeout_ms = 250
    assert completion_timeout_seconds(request) == 0.25
    request.requested_timeout_ms = 60_000
    assert completion_timeout_seconds(request) == 5.0


def test_sequence_gap_metrics_count_missing_values_and_reset_by_source_and_session() -> None:
    metrics = RuntimeMetrics()
    receiver = ReceivedMultipartValidator(metrics)
    publisher = PublisherSequence(uuid_factory=lambda: UUID(int=1))
    builder = EnvelopeBuilder(publisher, unix_time_ns=lambda: 1, monotonic_ns=lambda: 1)
    payload = diagnostics_pb2.DiagnosticStatus(source_id="camera")
    first = builder.build(
        topic="cv.health.camera",
        payload_type="diagnostic_status_v1",
        payload=payload,
        task_id="",
        source_id="camera",
    )
    publisher.next_attempt()
    publisher.next_attempt()
    fourth = builder.build(
        topic="cv.health.camera",
        payload_type="diagnostic_status_v1",
        payload=payload,
        task_id="",
        source_id="camera",
    )
    assert receiver.validate(first.frames).valid
    assert receiver.validate(fourth.frames).valid
    assert metrics.snapshot().values["observed_sequence_gaps"] == 2
    assert not receiver.validate(fourth.frames).valid
    assert metrics.snapshot().values["observed_sequence_gaps"] == 2
    other_source = builder.build(
        topic="cv.health.other",
        payload_type="diagnostic_status_v1",
        payload=diagnostics_pb2.DiagnosticStatus(source_id="other"),
        task_id="",
        source_id="other",
    )
    assert receiver.validate(other_source.frames).valid
    restarted = EnvelopeBuilder(
        PublisherSequence(uuid_factory=lambda: UUID(int=2)),
        unix_time_ns=lambda: 1,
        monotonic_ns=lambda: 1,
    ).build(
        topic="cv.health.camera",
        payload_type="diagnostic_status_v1",
        payload=payload,
        task_id="",
        source_id="camera",
    )
    assert receiver.validate(restarted.frames).valid
    assert metrics.snapshot().values["observed_sequence_gaps"] == 2


def test_router_maps_only_again_to_target_send_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    service = ControlRouterService(
        "tcp://127.0.0.1:1",
        "ipc:///tmp/unused-phase4.sock",
        device_id="rov_pi5",
        allowed_module_ids={"gate_detection"},
    )
    session = UUID(int=1)
    registration = _registration(session.bytes)
    route = f"module:gate_detection:{session}".encode()
    service.registry.register(registration, route)
    client_socket = object()
    module_socket = object()
    sent: list[list[bytes]] = []

    def fake_send(socket: object, frames: list[bytes]) -> None:
        if socket is module_socket:
            raise zmq.Again()
        sent.append(frames)

    monkeypatch.setattr(service, "_send", fake_send)
    request = _command()
    service._handle_client_command(
        client_socket,
        module_socket,
        RouterMessage(f"client:{uuid4()}".encode(), COMMAND_REQUEST, request.SerializeToString()),
    )
    assert sent[0][1] == COMMAND_RESPONSE
    response = control_pb2.CommandResponse.FromString(sent[0][2])
    assert response.status == control_pb2.COMMAND_STATUS_REJECTED
    assert response.error_code == ErrorCode.TARGET_SEND_TIMEOUT


def test_router_rejects_inflight_duplicate_without_overwriting_originating_route(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = ControlRouterService(
        "tcp://127.0.0.1:1",
        "ipc:///tmp/unused-phase4.sock",
        device_id="rov_pi5",
        allowed_module_ids={"gate_detection"},
    )
    session = UUID(int=1)
    route = f"module:gate_detection:{session}".encode()
    service.registry.register(_registration(session.bytes), route)
    command_id = UUID(int=77).bytes
    original_client = f"client:{uuid4()}".encode()
    second_client = f"client:{uuid4()}".encode()
    service._pending[command_id] = PendingRoute(
        client_identity=original_client,
        module_id="gate_detection",
        module_session_id=session.bytes,
        module_routing_identity=route,
        created_monotonic=0.0,
    )
    client_socket = object()
    module_socket = object()
    sent: list[tuple[object, list[bytes]]] = []

    def capture(socket: object, frames: list[bytes]) -> None:
        sent.append((socket, frames))

    monkeypatch.setattr(service, "_send", capture)
    request = _command(command_id=command_id)
    service._handle_client_command(
        client_socket,
        module_socket,
        RouterMessage(second_client, COMMAND_REQUEST, request.SerializeToString()),
    )
    assert all(socket is not module_socket for socket, _ in sent)
    assert sent[0][1][0] == second_client
    response = control_pb2.CommandResponse.FromString(sent[0][1][2])
    assert response.error_code == ErrorCode.DUPLICATE_COMMAND_ID
    assert service._pending[command_id].client_identity == original_client


@pytest.mark.parametrize("failure", [zmq.Again(), zmq.ZMQError(zmq.EHOSTUNREACH)])
def test_router_peer_send_failure_is_local_and_not_counted_as_success(
    monkeypatch: pytest.MonkeyPatch,
    failure: zmq.ZMQError,
) -> None:
    service = ControlRouterService(
        "tcp://127.0.0.1:1",
        "ipc:///tmp/unused-phase4.sock",
        device_id="rov_pi5",
        allowed_module_ids={"gate_detection"},
    )

    def fail(socket: object, frames: list[bytes]) -> None:
        del socket, frames
        raise failure

    monkeypatch.setattr(service, "_send", fail)
    response = control_pb2.CommandResponse(
        command_id=UUID(int=12).bytes,
        target_id="gate_detection",
        status=control_pb2.COMMAND_STATUS_COMPLETED,
    )
    assert not service._reply_client(object(), f"client:{uuid4()}".encode(), response)
    assert not service._reply_registration(object(), b"module-route", accepted=True)
    assert service.metrics.snapshot().values["messages_sent"] == 0


def test_router_peer_send_propagates_unexpected_context_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    service = ControlRouterService(
        "tcp://127.0.0.1:1",
        "ipc:///tmp/unused-phase4.sock",
        device_id="rov_pi5",
        allowed_module_ids={"gate_detection"},
    )

    def fail(socket: object, frames: list[bytes]) -> None:
        del socket, frames
        raise zmq.ZMQError(zmq.ETERM)

    monkeypatch.setattr(service, "_send", fail)
    with pytest.raises(zmq.ZMQError) as error:
        service._reply_registration(object(), b"module-route", accepted=True)
    assert error.value.errno == zmq.ETERM


def test_stale_module_response_cannot_reach_pending_client() -> None:
    service = ControlRouterService(
        "tcp://127.0.0.1:1",
        "ipc:///tmp/unused-phase4.sock",
        device_id="rov_pi5",
        allowed_module_ids={"gate_detection"},
    )
    old_session = UUID(int=1)
    new_session = UUID(int=2)
    old_route = f"module:gate_detection:{old_session}".encode()
    new_route = f"module:gate_detection:{new_session}".encode()
    service.registry.register(_registration(old_session.bytes), old_route)
    command_id = UUID(int=55).bytes
    service._pending[command_id] = PendingRoute(
        client_identity=f"client:{uuid4()}".encode(),
        module_id="gate_detection",
        module_session_id=old_session.bytes,
        module_routing_identity=old_route,
        created_monotonic=0.0,
    )
    service.registry.register(_registration(new_session.bytes), new_route)
    response = control_pb2.CommandResponse(
        command_id=command_id,
        target_id="gate_detection",
        status=control_pb2.COMMAND_STATUS_COMPLETED,
    )
    service._handle_module_response(
        object(),
        RouterMessage(old_route, COMMAND_RESPONSE, response.SerializeToString()),
    )
    assert service.metrics.snapshot().values["invalid_messages"] == 1
    assert command_id in service._pending


def test_router_retires_status_lookup_route_after_one_pending_status_reply(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = ControlRouterService(
        "tcp://127.0.0.1:1",
        "ipc:///tmp/unused-phase4.sock",
        device_id="rov_pi5",
        allowed_module_ids={"gate_detection"},
    )
    session = UUID(int=3)
    route = f"module:gate_detection:{session}".encode()
    service.registry.register(_registration(session.bytes), route)
    query_id = UUID(int=56).bytes
    service._pending[query_id] = PendingRoute(
        client_identity=f"client:{uuid4()}".encode(),
        module_id="gate_detection",
        module_session_id=session.bytes,
        module_routing_identity=route,
        created_monotonic=0.0,
        command_type="get_command_status",
    )
    monkeypatch.setattr(service, "_reply_client", lambda *args: True)
    response = control_pb2.CommandResponse(
        command_id=query_id,
        target_id="gate_detection",
        status=control_pb2.COMMAND_STATUS_RECEIVED,
    )

    service._handle_module_response(
        object(),
        RouterMessage(route, COMMAND_RESPONSE, response.SerializeToString()),
    )

    assert query_id not in service._pending


def test_fake_module_response_timeout_caches_once_and_logs() -> None:
    stream = io.StringIO()
    logger = configure_json_logger(
        device_id="rov_pi5",
        process_name="fake-module",
        source_id="gate_detection",
        publisher_session_id=None,
        stream=stream,
    )
    module = FakeModuleService(
        "ipc:///tmp/unused-phase4.sock",
        module_id="gate_detection",
        task_id="gate_detection",
        host_device_id="rov_pi5",
        logger=logger,
    )

    class TimeoutSocket:
        def send_multipart(self, frames: list[bytes]) -> None:
            del frames
            raise zmq.Again()

    response = control_pb2.CommandResponse(
        command_id=UUID(int=9).bytes,
        target_id="gate_detection",
        status=control_pb2.COMMAND_STATUS_COMPLETED,
    )
    module._send_response(TimeoutSocket(), response, cache_result=True)
    assert module.cache.get(response.command_id) is not None
    assert "CONTROL_RESPONSE_SEND_TIMEOUT" in stream.getvalue()


def test_fake_module_rejects_contract_invalid_registration_ack() -> None:
    module = FakeModuleService(
        "ipc:///tmp/unused-phase4.sock",
        module_id="gate_detection",
        task_id="gate_detection",
        host_device_id="rov_pi5",
    )
    retry = RegistrationRetryController()
    invalid = registration_pb2.ModuleRegistrationResponse(accepted=True, error_code="not-a-code")
    assert not module._handle_registration_response(invalid.SerializeToString(), retry)
    assert not module.registration_succeeded
    assert module.state_machine.state.value == "STARTING"
    assert module.metrics.snapshot().values["invalid_messages"] == 1

    valid = registration_pb2.ModuleRegistrationResponse(accepted=True)
    assert module._handle_registration_response(valid.SerializeToString(), retry)
    assert module.registration_succeeded
    assert module.state_machine.state.value == "READY"


def test_control_client_rejects_inherited_context_and_socket_after_fork(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = ControlClient("inproc://phase4-process-ownership", acknowledgement_timeout_seconds=0.01)
    try:
        monkeypatch.setattr(client_module.os, "getpid", lambda: client._owner_process_id + 1)
        with pytest.raises(RuntimeError, match="after fork"):
            client.send_command(_command())
    finally:
        monkeypatch.undo()
        client.close()


def test_control_client_ack_timeout_never_resends_and_allows_one_status_query() -> None:
    context = zmq.Context()
    router: zmq.Socket[bytes] = context.socket(zmq.ROUTER)
    endpoint = "inproc://phase4-client-timeout"
    router.bind(endpoint)
    client = ControlClient(endpoint, acknowledgement_timeout_seconds=0.01)
    try:
        command = _command()
        result = client.send_command(command)
        assert result.status == control_pb2.COMMAND_STATUS_OUTCOME_UNKNOWN
        assert result.error_code == ErrorCode.COMMAND_OUTCOME_UNKNOWN
        assert client.send_attempts == 1
        client.get_command_status("gate_detection", command.command_id)
        with pytest.raises(RuntimeError):
            client.get_command_status("gate_detection", command.command_id)
    finally:
        client.close()
        router.close(linger=0)
        context.term()


def test_accepted_command_completion_polls_status_without_resending(monkeypatch: pytest.MonkeyPatch) -> None:
    clock = FakeClock()

    def advance(seconds: float) -> None:
        clock.now += seconds

    client = ControlClient(
        "inproc://phase4-completion-policy",
        acknowledgement_timeout_seconds=0.01,
        monotonic=clock,
        sleep=advance,
    )
    command = _command(command_id=UUID(int=42).bytes)
    acknowledgements = 0
    status_lookups = 0

    def accepted(request: control_pb2.CommandRequest) -> control_pb2.CommandResponse:
        nonlocal acknowledgements
        acknowledgements += 1
        return control_pb2.CommandResponse(
            command_id=request.command_id,
            target_id=request.target_id,
            status=control_pb2.COMMAND_STATUS_ACCEPTED,
        )

    def no_completion(command_id: bytes, deadline: float) -> None:
        del command_id
        clock.now = deadline
        return None

    def status(target_id: str, target_command_id: bytes) -> control_pb2.CommandResponse:
        nonlocal status_lookups
        status_lookups += 1
        if status_lookups == 1:
            return control_pb2.CommandResponse(
                command_id=uuid4().bytes,
                target_id=target_id,
                status=control_pb2.COMMAND_STATUS_REJECTED,
                error_code=ErrorCode.INVALID_COMMAND,
            )
        return control_pb2.CommandResponse(
            command_id=uuid4().bytes,
            target_id=target_id,
            status=control_pb2.COMMAND_STATUS_COMPLETED,
            message=target_command_id.hex(),
        )

    monkeypatch.setattr(client, "send_command", accepted)
    monkeypatch.setattr(client, "_receive_correlated", no_completion)
    monkeypatch.setattr(client, "get_command_status", status)
    try:
        result = client.execute_command(command)
        assert result.status == control_pb2.COMMAND_STATUS_COMPLETED
        assert result.command_id == command.command_id
        assert acknowledgements == 1
        assert status_lookups == 2
    finally:
        client.close()


def test_accepted_command_status_polling_stops_after_ten_seconds_with_original_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = FakeClock()

    def advance(seconds: float) -> None:
        clock.now += seconds

    client = ControlClient(
        "inproc://phase4-completion-unknown-policy",
        acknowledgement_timeout_seconds=0.01,
        monotonic=clock,
        sleep=advance,
    )
    command = _command(command_id=UUID(int=43).bytes)
    acknowledgements = 0
    status_lookups = 0

    def accepted(request: control_pb2.CommandRequest) -> control_pb2.CommandResponse:
        nonlocal acknowledgements
        acknowledgements += 1
        return control_pb2.CommandResponse(
            command_id=request.command_id,
            target_id=request.target_id,
            status=control_pb2.COMMAND_STATUS_ACCEPTED,
        )

    def no_completion(command_id: bytes, deadline: float) -> None:
        del command_id
        clock.now = deadline
        return None

    def status(target_id: str, target_command_id: bytes) -> control_pb2.CommandResponse:
        nonlocal status_lookups
        assert target_command_id == command.command_id
        status_lookups += 1
        return control_pb2.CommandResponse(
            command_id=uuid4().bytes,
            target_id=target_id,
            status=control_pb2.COMMAND_STATUS_REJECTED,
            error_code=ErrorCode.INVALID_COMMAND,
        )

    monkeypatch.setattr(client, "send_command", accepted)
    monkeypatch.setattr(client, "_receive_correlated", no_completion)
    monkeypatch.setattr(client, "get_command_status", status)
    try:
        result = client.execute_command(command)
        assert result.status == control_pb2.COMMAND_STATUS_OUTCOME_UNKNOWN
        assert result.error_code == ErrorCode.COMMAND_OUTCOME_UNKNOWN
        assert result.command_id == command.command_id
        assert acknowledgements == 1
        assert status_lookups == 10
        assert clock.now == 15.0
    finally:
        client.close()


def test_service_entrypoint_translates_invalid_configuration_and_cli(tmp_path: Path) -> None:
    invalid = tmp_path / "invalid.yaml"
    invalid.write_text("schema_version: nope\n", encoding="utf-8")
    assert broker_entrypoint(["--config", str(invalid)]) == ExitCode.INVALID_CONFIGURATION
    with pytest.raises(SystemExit) as exit_info:
        broker_entrypoint(["--unknown"])
    assert exit_info.value.code == ExitCode.INVALID_ARGUMENTS


@pytest.mark.parametrize(
    ("failure", "expected"),
    [
        (zmq.ZMQError(zmq.EADDRINUSE), ExitCode.TEMPORARY_FAILURE),
        (zmq.ZMQError(errno.EADDRNOTAVAIL), ExitCode.INVALID_CONFIGURATION),
        (zmq.ZMQError(errno.EACCES), ExitCode.IO_FAILURE),
        (zmq.ZMQError(errno.EINVAL), ExitCode.INTERNAL_SOFTWARE_FAILURE),
        (OSError("device I/O failed"), ExitCode.IO_FAILURE),
        (RuntimeError("bug"), ExitCode.INTERNAL_SOFTWARE_FAILURE),
    ],
)
def test_service_entrypoint_translates_runtime_failures(
    monkeypatch: pytest.MonkeyPatch,
    failure: Exception,
    expected: ExitCode,
) -> None:
    def fail(argv: list[str] | None = None) -> ExitCode:
        del argv
        raise failure

    monkeypatch.setattr(service_entrypoints, "broker_main", fail)
    assert service_entrypoints.broker_entrypoint([]) == expected
