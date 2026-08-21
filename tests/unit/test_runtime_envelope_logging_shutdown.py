"""Envelope, JSON logging, and bounded shutdown integration coverage."""

from __future__ import annotations

import io
import json
import signal
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from threading import Event
from uuid import UUID

import pytest
from purdue_rov.cv.v1 import bounding_box_pb2, debug_snapshot_pb2

from purdue_rov_cv.config.models import LogLevel as ConfigLogLevel
from purdue_rov_cv.runtime.envelope import (
    EnvelopeBuilder,
    EnvelopeBuildError,
    validate_received_multipart,
)
from purdue_rov_cv.runtime.exit_codes import ExitCode
from purdue_rov_cv.runtime.json_logging import LogContext, LogLevel, StructuredJsonLogger
from purdue_rov_cv.runtime.metrics import RuntimeMetrics
from purdue_rov_cv.runtime.publisher import PublisherSequence
from purdue_rov_cv.runtime.queues import CvResultQueue, ReceiveStatus
from purdue_rov_cv.runtime.shutdown import ShutdownCoordinator, ShutdownToken, install_signal_handlers
from purdue_rov_cv.runtime.state import ComponentState, ComponentStateMachine

SESSION_UUID = UUID("123e4567-e89b-12d3-a456-426614174000")
CAMERA_SESSION = b"c" * 16


def _payload():
    return bounding_box_pb2.BoundingBoxResult(
        camera_id="front",
        camera_session_id=CAMERA_SESSION,
        frame_number=7,
        capture_time_unix_ns=12,
        detections=[bounding_box_pb2.Detection(confidence=0.8, x=0.1, y=0.1, width=0.2, height=0.2)],
    )


def _build(builder: EnvelopeBuilder):
    return builder.build(
        topic="cv.result.gate_detection.front",
        payload_type="bounding_boxes_v1",
        payload=_payload(),
        task_id="gate_detection",
        source_id="gate_module",
        camera_id="front",
        camera_session_id=CAMERA_SESSION,
        frame_number=7,
        capture_time_unix_ns=12,
    )


def test_envelope_builder_populates_validated_contract_and_attempt_sequence():
    publisher = PublisherSequence(uuid_factory=lambda: SESSION_UUID)
    builder = EnvelopeBuilder(publisher, unix_time_ns=lambda: 100, monotonic_ns=lambda: 200)
    first = _build(builder)
    assert first.publication.sequence_number == 0
    assert first.envelope.publisher_session_id == SESSION_UUID.bytes
    assert first.envelope.publish_time_unix_ns == 100
    assert first.envelope.source_monotonic_ns == 200
    assert first.envelope.payload_size_bytes == len(first.envelope.payload)
    assert first.frames == (first.topic, first.serialized_envelope)
    # A transport failure happens after building, so its number is already consumed.
    send_succeeded = False
    assert not send_succeeded
    assert _build(builder).publication.sequence_number == 1


def test_envelope_builder_unknown_type_and_oversize_reject_before_transmission_but_consume_attempt():
    publisher = PublisherSequence(uuid_factory=lambda: SESSION_UUID)
    builder = EnvelopeBuilder(publisher)
    with pytest.raises(EnvelopeBuildError) as unknown:
        builder.build(
            topic="cv.result.gate_detection.front",
            payload_type="unknown_v1",
            payload=_payload(),
            task_id="gate_detection",
            source_id="gate_module",
        )
    assert unknown.value.publication.sequence_number == 0
    huge = debug_snapshot_pb2.DebugSnapshot(
        camera_id="front",
        camera_session_id=CAMERA_SESSION,
        frame_number=7,
        capture_time_unix_ns=12,
        width=640,
        height=360,
        jpeg_quality=70,
        jpeg_data=b"\xff\xd8" + (b"x" * (4 * 1024 * 1024)) + b"\xff\xd9",
    )
    with pytest.raises(EnvelopeBuildError, match="MESSAGE_TOO_LARGE") as oversized:
        builder.build(
            topic="cv.debug_snapshot.front",
            payload_type="debug_snapshot_v1",
            payload=huge,
            task_id="debug",
            source_id="camera",
            camera_id="front",
            camera_session_id=CAMERA_SESSION,
            frame_number=7,
            capture_time_unix_ns=12,
        )
    assert oversized.value.publication.sequence_number == 1
    assert _build(builder).publication.sequence_number == 2


def test_receiver_multipart_validation_updates_exact_metrics():
    metrics = RuntimeMetrics()
    invalid = validate_received_multipart([b"only-one"], metrics=metrics)
    assert not invalid.valid
    snapshot = metrics.snapshot().values
    assert snapshot["invalid_messages"] == 1
    assert snapshot["invalid_multipart_message"] == 1
    built = _build(EnvelopeBuilder(PublisherSequence(uuid_factory=lambda: SESSION_UUID)))
    assert validate_received_multipart(built.frames, metrics=metrics).valid
    assert metrics.snapshot().values["messages_received"] == 1

    built.envelope.payload_type = "unknown_v1"
    unknown_frames = (built.topic, built.envelope.SerializeToString())
    assert not validate_received_multipart(unknown_frames, metrics=metrics).valid
    snapshot = metrics.snapshot().values
    assert snapshot["invalid_messages"] == 2
    assert snapshot["unknown_payload_types"] == 1


def test_structured_logger_emits_valid_deterministic_json_with_context_and_exception():
    stream = io.StringIO()
    logger = StructuredJsonLogger(
        LogContext("rov_pi5", "gate", "gate_module", SESSION_UUID),
        stream=stream,
        utc_now=lambda: datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC),
    )
    record = logger.log(
        LogLevel.WARNING,
        "FRAME_DELAYED",
        "frame was delayed",
        camera_id="front",
        camera_session_id=CAMERA_SESSION,
        frame_number=7,
        command_id=SESSION_UUID,
        command_type="start",
        target_id="gate",
        exception=ValueError("bad frame"),
        context={"path": Path("/tmp/model"), "state": ComponentState.RUNNING},
    )
    decoded = json.loads(stream.getvalue())
    assert decoded == record
    assert decoded["timestamp_utc"] == "2026-01-02T03:04:05Z"
    assert decoded["level"] == "WARNING"
    assert decoded["publisher_session_id"] == str(SESSION_UUID)
    assert decoded["camera_session_id"] == str(UUID(bytes=CAMERA_SESSION))
    assert decoded["command_id"] == str(SESSION_UUID)
    for field in (
        "device_id",
        "process_name",
        "source_id",
        "event_code",
        "message",
        "publisher_session_id",
        "camera_id",
        "camera_session_id",
        "frame_number",
        "command_id",
        "command_type",
        "target_id",
    ):
        assert field in decoded
    assert decoded["context"] == {"path": "/tmp/model", "state": "RUNNING"}
    assert decoded["exception"] == {"type": "ValueError", "message": "bad frame"}


def test_structured_logger_reuses_config_level_and_keeps_nonfinite_context_valid_json():
    assert LogLevel is ConfigLogLevel
    stream = io.StringIO()
    logger = StructuredJsonLogger(LogContext("pi", "worker", "source", SESSION_UUID.bytes), stream=stream)

    logger.log(
        LogLevel.INFO,
        "NUMERIC_CONTEXT",
        "nonfinite values",
        context={"nan": float("nan"), "positive": float("inf"), "negative": float("-inf")},
    )

    decoded = json.loads(stream.getvalue())
    assert decoded["publisher_session_id"] == str(SESSION_UUID)
    assert decoded["context"] == {"nan": "NaN", "negative": "-Infinity", "positive": "Infinity"}


def test_missing_log_context_is_null_and_receive_timeout_is_debug():
    stream = io.StringIO()
    logger = StructuredJsonLogger(LogContext("pi", "worker", "source", None), stream=stream)
    logger.receive_timeout()
    decoded = json.loads(stream.getvalue())
    assert decoded["level"] == "DEBUG"
    assert decoded["camera_id"] is None
    assert decoded["command_id"] is None


def test_structured_logger_escapes_newlines_unicode_and_serializes_concurrent_records():
    stream = io.StringIO()
    logger = StructuredJsonLogger(LogContext("pi", "worker", "source", SESSION_UUID), stream=stream)

    with ThreadPoolExecutor(max_workers=8) as executor:
        list(
            executor.map(
                lambda index: logger.log(LogLevel.INFO, "UNICODE", f"frame {index}\nready λ"),
                range(100),
            )
        )

    records = [json.loads(line) for line in stream.getvalue().splitlines()]
    assert len(records) == 100
    assert {record["message"] for record in records} == {f"frame {index}\nready λ" for index in range(100)}


def test_shutdown_request_is_idempotent_stops_work_and_worker_observes_token():
    state = ComponentStateMachine(ComponentState.RUNNING)
    coordinator = ShutdownCoordinator(state_machine=state)
    first = coordinator.request("SIGTERM")
    second = coordinator.request("again")
    assert first.first_request and not second.first_request
    assert coordinator.token.reason == "SIGTERM"
    assert not coordinator.accepting_work
    assert state.state is ComponentState.STOPPING
    queue = CvResultQueue[int]()
    assert queue.receive(timeout_seconds=0.250, shutdown=coordinator.token).status is ReceiveStatus.SHUTDOWN


def test_shutdown_callbacks_are_ordered_idempotent_and_failures_do_not_stop_cleanup():
    state = ComponentStateMachine(ComponentState.RUNNING)
    coordinator = ShutdownCoordinator(state_machine=state)
    order: list[str] = []
    coordinator.register("sockets", lambda: order.append("sockets"), order=20)

    def failing() -> None:
        order.append("pipeline")
        raise OSError("close failed")

    coordinator.register("pipeline", failing, order=10)
    coordinator.register("logs", lambda: order.append("logs"), order=30)
    coordinator.request("test")
    result = coordinator.run(timeout_seconds=1.0)
    assert order == ["pipeline", "sockets", "logs"]
    assert [failure.hook_name for failure in result.failures] == ["pipeline"]
    assert result.exit_code is ExitCode.INTERNAL_SOFTWARE_FAILURE
    assert state.state is ComponentState.STOPPED
    assert coordinator.wait_complete(0)
    assert coordinator.run(timeout_seconds=1.0) is result
    assert order == ["pipeline", "sockets", "logs"]


def test_shutdown_timeout_is_bounded_and_requests_exit75():
    blocker = Event()
    coordinator = ShutdownCoordinator()
    coordinator.register("blocked", blocker.wait)
    coordinator.request("test timeout")
    result = coordinator.run(timeout_seconds=0.01)
    assert result.timed_out
    assert not result.completed
    assert result.exit_code is ExitCode.TEMPORARY_FAILURE
    assert result.completed_hooks == ()


def test_concurrent_shutdown_runners_execute_hooks_once():
    coordinator = ShutdownCoordinator()
    calls: list[str] = []
    coordinator.register("once", lambda: calls.append("once"))
    coordinator.request("concurrent")
    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _: coordinator.run(timeout_seconds=1.0), range(2)))
    assert calls == ["once"]
    assert results[0] is results[1]


def test_shutdown_request_serializes_against_hook_registration():
    request_entered = Event()
    allow_request = Event()

    class BlockingToken(ShutdownToken):
        def request(self, reason):
            request_entered.set()
            assert allow_request.wait(1.0)
            return super().request(reason)

    coordinator = ShutdownCoordinator(token=BlockingToken())
    with ThreadPoolExecutor(max_workers=2) as executor:
        request = executor.submit(coordinator.request, "race test")
        assert request_entered.wait(1.0)
        registration = executor.submit(coordinator.register, "late", lambda: None)
        assert not registration.done()
        allow_request.set()
        assert request.result(timeout=1.0).first_request
        with pytest.raises(RuntimeError, match="shutdown begins"):
            registration.result(timeout=1.0)


def test_signal_handlers_are_installed_explicitly_and_repeated_signal_is_idempotent(monkeypatch):
    installed = {}
    coordinator = ShutdownCoordinator()
    monkeypatch.setattr(signal, "getsignal", lambda signum: f"previous:{signum}")
    monkeypatch.setattr(signal, "signal", lambda signum, handler: installed.__setitem__(signum, handler))

    previous = install_signal_handlers(coordinator, signals=(signal.SIGTERM,))
    handler = installed[signal.SIGTERM]
    handler(signal.SIGTERM, None)
    handler(signal.SIGTERM, None)

    assert previous == {signal.SIGTERM: f"previous:{signal.SIGTERM}"}
    assert coordinator.token.reason == "signal:SIGTERM"
    assert not coordinator.accepting_work
