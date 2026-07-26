"""Focused tests for the static payload registry and wire validators."""

from __future__ import annotations

import math
from pathlib import Path

import pytest
from purdue_rov.cv.v1 import (
    bounding_box_pb2,
    classification_pb2,
    clock_status_pb2,
    control_pb2,
    debug_snapshot_pb2,
    diagnostics_pb2,
    envelope_pb2,
    event_pb2,
    frame_index_pb2,
    module_state_pb2,
    registration_pb2,
    target_pose_pb2,
)

from purdue_rov_cv.wire.errors import (
    ERROR_CODE_CONTRACTS,
    ErrorCode,
    is_command_response_error_code,
    is_error_code,
)
from purdue_rov_cv.wire.identities import validate_dealer_identity
from purdue_rov_cv.wire.payloads import PAYLOAD_REGISTRY
from purdue_rov_cv.wire.topics import TopicKind, validate_topic
from purdue_rov_cv.wire.validators import (
    DEBUG_SNAPSHOT_ENVELOPE_LIMIT_BYTES,
    NORMAL_ENVELOPE_LIMIT_BYTES,
    SequenceTracker,
    validate_command_request,
    validate_command_response,
    validate_envelope,
    validate_module_registration,
    validate_module_registration_response,
    validate_multipart,
    validate_serialized_envelope,
)

SESSION = b"s" * 16
CAMERA_SESSION = b"c" * 16
JPEG_1X1 = (
    b"\xff\xd8"
    b"\xff\xe0\x00\x10JFIF\x00\x01\x01\x00\x00\x01\x00\x01\x00\x00"
    b"\xff\xc0\x00\x0b\x08\x00\x01\x00\x01\x01\x01\x11\x00"
    b"\xff\xda\x00\x08\x01\x01\x00\x00\x3f\x00\x00\xff\xd9"
)


def _payload_and_topic(payload_type: str):
    if payload_type == "bounding_boxes_v1":
        return (
            bounding_box_pb2.BoundingBoxResult(
                camera_id="front", camera_session_id=CAMERA_SESSION, frame_number=7,
                capture_time_unix_ns=12,
                detections=[bounding_box_pb2.Detection(confidence=0.8, x=0.1, y=0.2, width=0.3, height=0.4)],
            ),
            "cv.result.gate_detection.front",
        )
    if payload_type == "classification_result_v1":
        return (
            classification_pb2.ClassificationResult(
                camera_id="front", camera_session_id=CAMERA_SESSION, frame_number=7,
                capture_time_unix_ns=12,
                classes=[classification_pb2.ClassScore(confidence=0.8)],
            ),
            "cv.result.gate_detection.front",
        )
    if payload_type == "target_pose_v1":
        return (
            target_pose_pb2.TargetPoseResult(
                camera_id="front", camera_session_id=CAMERA_SESSION, frame_number=7,
                capture_time_unix_ns=12, coordinate_frame="camera_front", confidence=0.8,
            ),
            "cv.result.gate_detection.front",
        )
    if payload_type == "diagnostic_status_v1":
        return diagnostics_pb2.DiagnosticStatus(source_id="camera"), "cv.health.camera"
    if payload_type == "module_state_v1":
        return module_state_pb2.ModuleState(source_id="module", publisher_session_id=SESSION), "cv.state.module"
    if payload_type == "frame_index_v1":
        return (
            frame_index_pb2.FrameIndex(camera_id="front", camera_session_id=CAMERA_SESSION, frame_number=7, capture_time_unix_ns=12),
            "cv.frame_index.front",
        )
    if payload_type == "debug_snapshot_v1":
        return (
            debug_snapshot_pb2.DebugSnapshot(camera_id="front", camera_session_id=CAMERA_SESSION, frame_number=7,
                capture_time_unix_ns=12, width=1, height=1, jpeg_quality=70, jpeg_data=JPEG_1X1),
            "cv.debug_snapshot.front",
        )
    if payload_type == "clock_status_v1":
        return clock_status_pb2.ClockStatus(device_id="pi5", synchronized=True, offset_ms=0.1), "system.clock.pi5"
    if payload_type == "system_event_v1":
        return event_pb2.SystemEvent(event_type="camera_lost", source_id="pi5"), "system.event.camera_lost"
    raise AssertionError(payload_type)


def _envelope(payload_type: str):
    payload, topic = _payload_and_topic(payload_type)
    spec = PAYLOAD_REGISTRY[payload_type]
    envelope = envelope_pb2.MessageEnvelope(
        message_type=spec.message_type,
        payload_type=payload_type,
        source_id={
            "diagnostic_status_v1": "camera", "module_state_v1": "module", "clock_status_v1": "pi5", "system_event_v1": "pi5",
        }.get(payload_type, "module"),
        publisher_session_id=SESSION,
        schema_version=1,
        payload_encoding="protobuf",
        payload=payload.SerializeToString(),
    )
    envelope.payload_size_bytes = len(envelope.payload)
    if hasattr(payload, "camera_id"):
        envelope.camera_id = payload.camera_id
        envelope.camera_session_id = payload.camera_session_id
        envelope.frame_number = payload.frame_number
        envelope.capture_time_unix_ns = payload.capture_time_unix_ns
    if topic.startswith("cv.result."):
        envelope.task_id = "gate_detection"
    return envelope, topic, payload


@pytest.mark.parametrize("payload_type", sorted(PAYLOAD_REGISTRY))
def test_every_registered_payload_dispatches_and_validates(payload_type):
    envelope, topic, payload = _envelope(payload_type)
    result = validate_serialized_envelope(envelope.SerializeToString(), topic)

    assert result.valid, result.errors
    assert isinstance(result.payload, type(payload))


def test_payload_registry_exactly_covers_data_plane_payloads():
    assert set(PAYLOAD_REGISTRY) == {
        "bounding_boxes_v1",
        "classification_result_v1",
        "target_pose_v1",
        "diagnostic_status_v1",
        "module_state_v1",
        "frame_index_v1",
        "debug_snapshot_v1",
        "clock_status_v1",
        "system_event_v1",
    }
    assert {spec.message_class for spec in PAYLOAD_REGISTRY.values()} == {
        bounding_box_pb2.BoundingBoxResult,
        classification_pb2.ClassificationResult,
        target_pose_pb2.TargetPoseResult,
        diagnostics_pb2.DiagnosticStatus,
        module_state_pb2.ModuleState,
        frame_index_pb2.FrameIndex,
        debug_snapshot_pb2.DebugSnapshot,
        clock_status_pb2.ClockStatus,
        event_pb2.SystemEvent,
    }


@pytest.mark.parametrize(
    ("topic", "kind", "identifiers"),
    [
        ("cv.result.task.camera", TopicKind.CV_RESULT, {"task_id": "task", "camera_id": "camera"}),
        ("cv.health.source", TopicKind.CV_HEALTH, {"source_id": "source"}),
        ("cv.state.source", TopicKind.CV_STATE, {"source_id": "source"}),
        ("cv.frame_index.camera", TopicKind.CV_FRAME_INDEX, {"camera_id": "camera"}),
        ("cv.debug_snapshot.camera", TopicKind.CV_DEBUG_SNAPSHOT, {"camera_id": "camera"}),
        ("system.clock.device", TopicKind.SYSTEM_CLOCK, {"device_id": "device"}),
        ("system.health.device", TopicKind.SYSTEM_HEALTH, {"device_id": "device"}),
        ("system.event.event", TopicKind.SYSTEM_EVENT, {"event_type": "event"}),
    ],
)
def test_required_topics_are_valid(topic, kind, identifiers):
    result = validate_topic(topic)
    assert result.valid
    assert result.kind is kind
    assert result.identifiers == identifiers


@pytest.mark.parametrize("topic", [b"\xff", "CV.result.task.camera", "cv..result.task.camera", ".cv.result.task.camera", "cv.result.task.camera.", "cv.result.onlythree", "a" * 129])
def test_invalid_topics_are_rejected_without_raising(topic):
    assert not validate_topic(topic).valid


def test_unknown_payload_is_dropped_without_parsing():
    envelope, topic, _ = _envelope("bounding_boxes_v1")
    envelope.payload_type = "purdue_rov.cv.v1.DoesNotExist"
    result = validate_envelope(envelope, topic)
    assert result.payload is None
    assert ErrorCode.UNKNOWN_PAYLOAD_TYPE in {error.code for error in result.errors}


def test_malformed_envelope_and_payload_are_safe_failures():
    assert not validate_serialized_envelope(b"\x80", "cv.health.camera").valid
    envelope, topic, _ = _envelope("bounding_boxes_v1")
    envelope.payload = b"\x80"
    envelope.payload_size_bytes = 1
    assert not validate_envelope(envelope, topic).valid


@pytest.mark.parametrize(
    ("mutate", "expected_code"),
    [
        (lambda envelope: setattr(envelope, "publisher_session_id", b"short"), ErrorCode.INVALID_ENVELOPE),
        (lambda envelope: setattr(envelope, "camera_session_id", b"short"), ErrorCode.INVALID_ENVELOPE),
        (lambda envelope: setattr(envelope, "schema_version", 2), ErrorCode.UNSUPPORTED_SCHEMA_VERSION),
        (lambda envelope: setattr(envelope, "payload_size_bytes", 0), ErrorCode.INVALID_ENVELOPE),
        (lambda envelope: setattr(envelope, "message_type", envelope_pb2.DIAGNOSTIC), ErrorCode.INVALID_ENVELOPE),
    ],
)
def test_envelope_metadata_validation_branches(mutate, expected_code):
    envelope, topic, _ = _envelope("bounding_boxes_v1")
    mutate(envelope)
    result = validate_envelope(envelope, topic)
    assert expected_code in {error.code for error in result.errors}


def test_topic_conflict_and_payload_semantic_ranges_are_rejected():
    envelope, _, payload = _envelope("bounding_boxes_v1")
    assert not validate_envelope(envelope, "cv.result.other.front").valid
    payload.detections[0].confidence = math.inf
    envelope.payload = payload.SerializeToString()
    envelope.payload_size_bytes = len(envelope.payload)
    assert not validate_envelope(envelope, "cv.result.gate_detection.front").valid


def test_camera_envelope_metadata_is_required_and_consistent():
    envelope, topic, _ = _envelope("frame_index_v1")
    envelope.ClearField("camera_session_id")
    assert not validate_envelope(envelope, topic).valid
    envelope, topic, _ = _envelope("frame_index_v1")
    envelope.ClearField("frame_number")
    assert not validate_envelope(envelope, topic).valid
    envelope, topic, _ = _envelope("frame_index_v1")
    envelope.ClearField("capture_time_unix_ns")
    assert not validate_envelope(envelope, topic).valid


@pytest.mark.parametrize(
    "payload_type, mutate",
    [
        ("classification_result_v1", lambda payload: setattr(payload.classes[0], "confidence", -0.1)),
        ("classification_result_v1", lambda payload: payload.classes.add(confidence=0.9)),
        ("bounding_boxes_v1", lambda payload: setattr(payload.detections[0], "width", 0.95)),
        ("target_pose_v1", lambda payload: setattr(payload, "coordinate_frame", "world")),
        ("target_pose_v1", lambda payload: payload.covariance.extend([1.0] * 35)),
        ("debug_snapshot_v1", lambda payload: setattr(payload, "width", 641)),
        ("debug_snapshot_v1", lambda payload: setattr(payload, "jpeg_data", b"not-jpeg")),
        ("debug_snapshot_v1", lambda payload: setattr(payload, "jpeg_data", b"\xff\xd8x\xff\xd9")),
        ("clock_status_v1", lambda payload: setattr(payload, "offset_ms", math.nan)),
        ("system_event_v1", lambda payload: setattr(payload, "event_type", "other")),
    ],
)
def test_payload_semantic_validation_branches(payload_type, mutate):
    envelope, topic, payload = _envelope(payload_type)
    mutate(payload)
    envelope.payload = payload.SerializeToString()
    envelope.payload_size_bytes = len(envelope.payload)
    assert not validate_envelope(envelope, topic).valid


def test_message_size_limits_and_multipart_layouts():
    envelope, topic, _ = _envelope("bounding_boxes_v1")
    envelope.payload = b"x" * NORMAL_ENVELOPE_LIMIT_BYTES
    envelope.payload_size_bytes = len(envelope.payload)
    assert ErrorCode.MESSAGE_TOO_LARGE in {error.code for error in validate_envelope(envelope, topic).errors}
    assert not validate_serialized_envelope(b"x" * (DEBUG_SNAPSHOT_ENVELOPE_LIMIT_BYTES + 1), topic).valid
    assert not validate_multipart([topic.encode(), envelope.SerializeToString(), b"extra"]).valid
    assert not validate_multipart([topic, envelope.SerializeToString()]).valid


def test_sequence_tracker_rejects_replays_and_resets_with_new_session():
    tracker = SequenceTracker()
    envelope, topic, _ = _envelope("bounding_boxes_v1")
    envelope.sequence_number = 7
    assert validate_envelope(envelope, topic, sequence_tracker=tracker).valid
    assert not validate_envelope(envelope, topic, sequence_tracker=tracker).valid
    envelope.sequence_number = 8
    assert validate_envelope(envelope, topic, sequence_tracker=tracker).valid
    envelope.publisher_session_id = b"n" * 16
    envelope.sequence_number = 1
    assert validate_envelope(envelope, topic, sequence_tracker=tracker).valid


def test_diagnostic_payload_supports_both_health_topic_kinds():
    envelope, _, _ = _envelope("diagnostic_status_v1")
    envelope.source_id = "pi5"
    diagnostic = diagnostics_pb2.DiagnosticStatus(source_id="pi5")
    envelope.payload = diagnostic.SerializeToString()
    envelope.payload_size_bytes = len(envelope.payload)
    assert validate_envelope(envelope, "system.health.pi5").valid


def test_error_code_contract_is_complete_and_marks_non_response_codes():
    assert set(ERROR_CODE_CONTRACTS) == set(ErrorCode)
    assert not ERROR_CODE_CONTRACTS[ErrorCode.UNKNOWN_PAYLOAD_TYPE].command_response_allowed
    assert not ERROR_CODE_CONTRACTS[ErrorCode.INVALID_ENVELOPE].command_response_allowed
    assert ERROR_CODE_CONTRACTS[ErrorCode.INVALID_COMMAND].command_response_allowed
    assert not is_command_response_error_code("UNKNOWN_PAYLOAD_TYPE")
    assert is_command_response_error_code("INVALID_COMMAND")
    assert not is_command_response_error_code("not_a_code")
    assert is_error_code("UNKNOWN_PAYLOAD_TYPE")
    assert not is_error_code("not_a_code")
    contract_table = (Path(__file__).parents[2] / "docs" / "error-code-contract.md").read_text(encoding="utf-8")
    assert "| Error code | Emitter | Trigger | Command/result status |" in contract_table
    assert all(code.value in contract_table for code in ErrorCode)


@pytest.mark.parametrize(
    "identity",
    ["client:123e4567-e89b-12d3-a456-426614174000", "module:gate:123e4567-e89b-12d3-a456-426614174000"],
)
def test_dealer_identity_validation(identity):
    assert validate_dealer_identity(identity).valid


@pytest.mark.parametrize("identity", [b"\xff", "client:not-a-uuid", "client:123E4567-E89B-12D3-A456-426614174000", "module::123e4567-e89b-12d3-a456-426614174000", "x" * 129])
def test_invalid_dealer_identity_is_safe(identity):
    assert not validate_dealer_identity(identity).valid


def test_control_and_registration_uuid_validation():
    request = control_pb2.CommandRequest(target_id="gate")
    assert not validate_command_request(request).valid
    request.command_id = SESSION
    request.start.SetInParent()
    assert validate_command_request(request).valid
    request.get_command_status.target_command_id = b"short"
    # Setting this oneof replaces start and exercises the target UUID branch.
    assert not validate_command_request(request).valid

    registration = registration_pb2.ModuleRegistration(
        module_id="gate-module", task_id="gate_detection", module_session_id=SESSION,
        supported_command_types=["start", "get_status"], current_state=1,
        process_id=12, host_device_id="pi5",
    )
    assert validate_module_registration(registration).valid
    registration.module_session_id = b"short"
    assert not validate_module_registration(registration).valid


def test_command_and_registration_response_error_code_contracts():
    response = control_pb2.CommandResponse(
        command_id=SESSION,
        target_id="gate",
        status=control_pb2.COMMAND_STATUS_REJECTED,
        error_code="INVALID_COMMAND",
    )
    assert validate_command_response(response).valid
    response.error_code = "UNKNOWN_PAYLOAD_TYPE"
    assert not validate_command_response(response).valid
    response.error_code = "INVALID_COMMAND"
    response.status = control_pb2.COMMAND_STATUS_FAILED
    assert not validate_command_response(response).valid
    response.status = 999
    assert not validate_command_response(response).valid

    registration_response = registration_pb2.ModuleRegistrationResponse(error_code="INVALID_COMMAND")
    assert validate_module_registration_response(registration_response).valid
    registration_response.error_code = "not_a_code"
    assert not validate_module_registration_response(registration_response).valid
