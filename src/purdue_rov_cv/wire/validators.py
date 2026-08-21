"""Safe, side-effect-free validation for data-plane wire messages."""

from __future__ import annotations

import math
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Protocol

from google.protobuf.message import DecodeError, Message
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

from .errors import ERROR_CODE_CONTRACTS, ErrorCode, is_command_response_error_code, is_error_code
from .payloads import PAYLOAD_REGISTRY
from .topics import TopicKind, TopicValidationResult, validate_topic

NORMAL_ENVELOPE_LIMIT_BYTES = 1 * 1024 * 1024
DEBUG_SNAPSHOT_ENVELOPE_LIMIT_BYTES = 4 * 1024 * 1024
ZMQ_MAXMSGSIZE = DEBUG_SNAPSHOT_ENVELOPE_LIMIT_BYTES


@dataclass(frozen=True)
class ValidationError:
    code: str
    detail: str
    field: str | None = None


@dataclass(frozen=True)
class EnvelopeValidationResult:
    envelope: envelope_pb2.MessageEnvelope | None
    payload: Message | None
    topic: TopicValidationResult | None
    errors: tuple[ValidationError, ...] = ()

    @property
    def valid(self) -> bool:
        return not self.errors


@dataclass(frozen=True)
class ControlValidationResult:
    errors: tuple[ValidationError, ...] = ()

    @property
    def valid(self) -> bool:
        return not self.errors


class CameraPayload(Protocol):
    camera_id: str
    camera_session_id: bytes
    frame_number: int
    capture_time_unix_ns: int


@dataclass
class SequenceTracker:
    """Track strictly increasing data-plane sequences within publisher sessions.

    A receiver owns one tracker.  Validation itself stays side-effect-free unless
    a tracker is explicitly supplied.
    """

    gap_observer: Callable[[int], None] | None = None
    _last_by_session_and_source: dict[tuple[bytes, str], int] = field(default_factory=dict)

    def observe(self, envelope: envelope_pb2.MessageEnvelope) -> ValidationError | None:
        key = (bytes(envelope.publisher_session_id), envelope.source_id)
        last = self._last_by_session_and_source.get(key)
        if last is not None and envelope.sequence_number <= last:
            return _error(
                ErrorCode.INVALID_ENVELOPE,
                "sequence_number must strictly increase within a publisher session",
                "sequence_number",
            )
        if last is not None and envelope.sequence_number > last + 1 and self.gap_observer is not None:
            self.gap_observer(envelope.sequence_number - last - 1)
        self._last_by_session_and_source[key] = envelope.sequence_number
        return None


def _error(code: str | ErrorCode, detail: str, field: str | None = None) -> ValidationError:
    return ValidationError(str(code), detail, field)


def _finite_in_range(value: float, minimum: float, maximum: float) -> bool:
    return math.isfinite(value) and minimum <= value <= maximum


def _jpeg_dimensions(data: bytes) -> tuple[int, int] | None:
    """Read dimensions from a structurally valid baseline/progressive JPEG.

    This intentionally avoids an image-decoder dependency on the receive path,
    while rejecting marker-only and truncated byte strings.
    """
    if len(data) < 4 or not data.startswith(b"\xff\xd8"):
        return None
    index = 2
    dimensions: tuple[int, int] | None = None
    saw_scan = False
    sof_markers = frozenset({0xC0, 0xC1, 0xC2, 0xC3, 0xC5, 0xC6, 0xC7, 0xC9, 0xCA, 0xCB, 0xCD, 0xCE, 0xCF})
    while index < len(data):
        if data[index] != 0xFF:
            return None
        while index < len(data) and data[index] == 0xFF:
            index += 1
        if index >= len(data):
            return None
        marker = data[index]
        index += 1
        if marker == 0xD9:
            return dimensions if saw_scan else None
        if marker in {0xD8, 0x01} or 0xD0 <= marker <= 0xD7:
            continue
        if index + 2 > len(data):
            return None
        segment_length = int.from_bytes(data[index : index + 2], "big")
        if segment_length < 2 or index + segment_length > len(data):
            return None
        segment = data[index + 2 : index + segment_length]
        if marker in sof_markers:
            if len(segment) < 6:
                return None
            height = int.from_bytes(segment[1:3], "big")
            width = int.from_bytes(segment[3:5], "big")
            if not width or not height:
                return None
            dimensions = (width, height)
        elif marker == 0xDA:
            if dimensions is None or len(segment) < 6 or not data.endswith(b"\xff\xd9"):
                return None
            saw_scan = True
            # Entropy-coded data may contain arbitrary bytes, so its interior
            # is deliberately not interpreted as marker segments.
            return dimensions
        index += segment_length
    return None


def _validate_common_camera_payload(
    payload: CameraPayload, envelope: envelope_pb2.MessageEnvelope
) -> list[ValidationError]:
    errors: list[ValidationError] = []
    camera_id = payload.camera_id
    camera_session_id = payload.camera_session_id
    if not camera_id:
        errors.append(_error(ErrorCode.INVALID_ENVELOPE, "payload camera_id is empty", "payload.camera_id"))
    if len(camera_session_id) != 16:
        errors.append(
            _error(
                ErrorCode.INVALID_ENVELOPE, "payload camera_session_id must be 16 bytes", "payload.camera_session_id"
            )
        )
    for field_info, payload_value in (
        ("camera_id", camera_id),
        ("camera_session_id", camera_session_id),
        ("frame_number", payload.frame_number),
        ("capture_time_unix_ns", payload.capture_time_unix_ns),
    ):
        if not envelope.HasField(field_info):
            errors.append(
                _error(ErrorCode.INVALID_ENVELOPE, f"{field_info} is required for camera payloads", field_info)
            )
        elif getattr(envelope, field_info) != payload_value:
            errors.append(_error(ErrorCode.INVALID_ENVELOPE, f"payload and envelope {field_info} differ", field_info))
    return errors


def _validate_payload(payload: Message, envelope: envelope_pb2.MessageEnvelope) -> list[ValidationError]:
    errors: list[ValidationError] = []
    if isinstance(payload, bounding_box_pb2.BoundingBoxResult):
        errors.extend(_validate_common_camera_payload(payload, envelope))
        for index, detection in enumerate(payload.detections):
            if not _finite_in_range(detection.confidence, 0.0, 1.0):
                errors.append(
                    _error(
                        ErrorCode.INVALID_ENVELOPE,
                        "confidence must be finite and in [0, 1]",
                        f"detections[{index}].confidence",
                    )
                )
            for field in ("x", "y"):
                if not _finite_in_range(getattr(detection, field), 0.0, 1.0):
                    errors.append(
                        _error(
                            ErrorCode.INVALID_ENVELOPE,
                            f"{field} must be finite and in [0, 1]",
                            f"detections[{index}].{field}",
                        )
                    )
            for field in ("width", "height"):
                value = getattr(detection, field)
                if not _finite_in_range(value, 0.0, 1.0) or value <= 0.0:
                    errors.append(
                        _error(
                            ErrorCode.INVALID_ENVELOPE,
                            f"{field} must be finite and in (0, 1]",
                            f"detections[{index}].{field}",
                        )
                    )
            if math.isfinite(detection.x) and math.isfinite(detection.width) and detection.x + detection.width > 1.0:
                errors.append(_error(ErrorCode.INVALID_ENVELOPE, "x + width must not exceed 1", f"detections[{index}]"))
            if math.isfinite(detection.y) and math.isfinite(detection.height) and detection.y + detection.height > 1.0:
                errors.append(
                    _error(ErrorCode.INVALID_ENVELOPE, "y + height must not exceed 1", f"detections[{index}]")
                )
    elif isinstance(payload, classification_pb2.ClassificationResult):
        errors.extend(_validate_common_camera_payload(payload, envelope))
        for index, score in enumerate(payload.classes):
            if not _finite_in_range(score.confidence, 0.0, 1.0):
                errors.append(
                    _error(
                        ErrorCode.INVALID_ENVELOPE,
                        "confidence must be finite and in [0, 1]",
                        f"classes[{index}].confidence",
                    )
                )
            if index and score.confidence > payload.classes[index - 1].confidence:
                errors.append(
                    _error(ErrorCode.INVALID_ENVELOPE, "classes must be sorted by descending confidence", "classes")
                )
    elif isinstance(payload, target_pose_pb2.TargetPoseResult):
        errors.extend(_validate_common_camera_payload(payload, envelope))
        if payload.coordinate_frame not in {f"camera_{payload.camera_id}", "rov_body", "mission_local"}:
            errors.append(_error(ErrorCode.INVALID_ENVELOPE, "coordinate_frame is not allowed", "coordinate_frame"))
        if not _finite_in_range(payload.confidence, 0.0, 1.0):
            errors.append(_error(ErrorCode.INVALID_ENVELOPE, "confidence must be finite and in [0, 1]", "confidence"))
        if len(payload.covariance) not in {0, 36}:
            errors.append(
                _error(
                    ErrorCode.INVALID_ENVELOPE, "covariance must contain exactly 36 values when present", "covariance"
                )
            )
        if not all(
            math.isfinite(value)
            for value in (
                *payload.covariance,
                payload.x_m,
                payload.y_m,
                payload.z_m,
                payload.roll_rad,
                payload.pitch_rad,
                payload.yaw_rad,
            )
        ):
            errors.append(_error(ErrorCode.INVALID_ENVELOPE, "pose values must be finite", "pose"))
    elif isinstance(payload, debug_snapshot_pb2.DebugSnapshot):
        errors.extend(_validate_common_camera_payload(payload, envelope))
        if not 1 <= payload.width <= 640:
            errors.append(_error(ErrorCode.INVALID_ENVELOPE, "width must be in [1, 640]", "width"))
        if not 1 <= payload.height <= 360:
            errors.append(_error(ErrorCode.INVALID_ENVELOPE, "height must be in [1, 360]", "height"))
        if not 1 <= payload.jpeg_quality <= 95:
            errors.append(_error(ErrorCode.INVALID_ENVELOPE, "jpeg_quality must be in [1, 95]", "jpeg_quality"))
        dimensions = _jpeg_dimensions(payload.jpeg_data)
        if dimensions is None:
            errors.append(
                _error(
                    ErrorCode.INVALID_ENVELOPE, "jpeg_data must contain a structurally valid JPEG image", "jpeg_data"
                )
            )
        elif dimensions != (payload.width, payload.height):
            errors.append(
                _error(ErrorCode.INVALID_ENVELOPE, "JPEG dimensions must match width and height", "jpeg_data")
            )
    elif isinstance(payload, frame_index_pb2.FrameIndex):
        errors.extend(_validate_common_camera_payload(payload, envelope))
    elif isinstance(payload, diagnostics_pb2.DiagnosticStatus):
        if not payload.source_id or payload.source_id != envelope.source_id:
            errors.append(_error(ErrorCode.INVALID_ENVELOPE, "payload and envelope source_id differ", "source_id"))
        if not math.isfinite(payload.process_cpu_percent) or payload.process_cpu_percent < 0.0:
            errors.append(
                _error(
                    ErrorCode.INVALID_ENVELOPE,
                    "process_cpu_percent must be finite and non-negative",
                    "process_cpu_percent",
                )
            )
        if not math.isfinite(payload.uptime_seconds) or payload.uptime_seconds < 0.0:
            errors.append(
                _error(ErrorCode.INVALID_ENVELOPE, "uptime_seconds must be finite and non-negative", "uptime_seconds")
            )
        if payload.HasField("last_error_code") and not is_error_code(payload.last_error_code):
            errors.append(_error(ErrorCode.INVALID_ENVELOPE, "last_error_code is not canonical", "last_error_code"))
    elif isinstance(payload, module_state_pb2.ModuleState):
        if not payload.source_id or payload.source_id != envelope.source_id:
            errors.append(_error(ErrorCode.INVALID_ENVELOPE, "payload and envelope source_id differ", "source_id"))
        if len(payload.publisher_session_id) != 16 or payload.publisher_session_id != envelope.publisher_session_id:
            errors.append(
                _error(
                    ErrorCode.INVALID_ENVELOPE,
                    "payload publisher_session_id must match envelope",
                    "publisher_session_id",
                )
            )
        if payload.error_code and not is_error_code(payload.error_code):
            errors.append(_error(ErrorCode.INVALID_ENVELOPE, "error_code is not canonical", "error_code"))
    elif isinstance(payload, clock_status_pb2.ClockStatus):
        if not payload.device_id or not math.isfinite(payload.offset_ms):
            errors.append(
                _error(
                    ErrorCode.INVALID_ENVELOPE, "clock payload requires device_id and finite offset_ms", "clock_status"
                )
            )
    elif isinstance(payload, event_pb2.SystemEvent):
        if not payload.event_type or not payload.source_id or payload.source_id != envelope.source_id:
            errors.append(_error(ErrorCode.INVALID_ENVELOPE, "event identity does not match envelope", "event"))
        if payload.HasField("error_code") and not is_error_code(payload.error_code):
            errors.append(_error(ErrorCode.INVALID_ENVELOPE, "error_code is not canonical", "error_code"))
    return errors


def _validate_topic_conflicts(
    envelope: envelope_pb2.MessageEnvelope, topic: TopicValidationResult, payload_type: str
) -> list[ValidationError]:
    errors: list[ValidationError] = []
    if not topic.valid or topic.kind is None:
        return errors
    spec = PAYLOAD_REGISTRY.get(payload_type)
    if spec is not None and topic.kind not in spec.topic_kinds:
        errors.append(_error(ErrorCode.INVALID_ENVELOPE, "topic kind is incompatible with payload_type", "topic"))
    if topic.kind is TopicKind.CV_RESULT:
        if not envelope.task_id or envelope.task_id != topic.identifiers["task_id"]:
            errors.append(_error(ErrorCode.INVALID_ENVELOPE, "topic task_id conflicts with envelope", "task_id"))
        if not envelope.HasField("camera_id") or envelope.camera_id != topic.identifiers["camera_id"]:
            errors.append(_error(ErrorCode.INVALID_ENVELOPE, "topic camera_id conflicts with envelope", "camera_id"))
    elif topic.kind in {TopicKind.CV_HEALTH, TopicKind.CV_STATE}:
        if envelope.source_id != topic.identifiers["source_id"]:
            errors.append(_error(ErrorCode.INVALID_ENVELOPE, "topic source_id conflicts with envelope", "source_id"))
    elif topic.kind in {TopicKind.CV_FRAME_INDEX, TopicKind.CV_DEBUG_SNAPSHOT}:
        if not envelope.HasField("camera_id") or envelope.camera_id != topic.identifiers["camera_id"]:
            errors.append(_error(ErrorCode.INVALID_ENVELOPE, "topic camera_id conflicts with envelope", "camera_id"))
    elif topic.kind in {TopicKind.SYSTEM_CLOCK, TopicKind.SYSTEM_HEALTH}:
        if envelope.source_id != topic.identifiers["device_id"]:
            errors.append(_error(ErrorCode.INVALID_ENVELOPE, "topic device_id conflicts with envelope", "source_id"))
    elif topic.kind is TopicKind.SYSTEM_EVENT and payload_type == "system_event_v1":
        # The payload check below verifies event_type after parsing.
        pass
    return errors


def validate_envelope(
    envelope: envelope_pb2.MessageEnvelope,
    topic: str | bytes,
    *,
    serialized_size: int | None = None,
    sequence_tracker: SequenceTracker | None = None,
) -> EnvelopeValidationResult:
    """Validate a parsed envelope and its registered payload without raising."""
    topic_result = validate_topic(topic)
    errors = [_error(ErrorCode.INVALID_ENVELOPE, detail, "topic") for detail in topic_result.errors]
    if not isinstance(envelope, envelope_pb2.MessageEnvelope):
        return EnvelopeValidationResult(
            None,
            None,
            topic_result,
            tuple(errors + [_error(ErrorCode.INVALID_ENVELOPE, "object is not a MessageEnvelope")]),
        )
    message_size = envelope.ByteSize()
    if serialized_size is not None:
        if isinstance(serialized_size, bool) or not isinstance(serialized_size, int) or serialized_size < 0:
            errors.append(
                _error(ErrorCode.INVALID_ENVELOPE, "serialized_size must be a non-negative integer", "envelope")
            )
        else:
            message_size = max(message_size, serialized_size)
    limit = (
        DEBUG_SNAPSHOT_ENVELOPE_LIMIT_BYTES
        if envelope.payload_type == "debug_snapshot_v1"
        else NORMAL_ENVELOPE_LIMIT_BYTES
    )
    if message_size > limit:
        errors.append(
            _error(ErrorCode.MESSAGE_TOO_LARGE, f"envelope is {message_size} bytes; limit is {limit}", "envelope")
        )
    if envelope.payload_encoding != "protobuf":
        errors.append(_error(ErrorCode.INVALID_ENVELOPE, "payload_encoding must be protobuf", "payload_encoding"))
    if envelope.schema_version != 1:
        errors.append(_error(ErrorCode.UNSUPPORTED_SCHEMA_VERSION, "schema_version must be 1", "schema_version"))
    if not envelope.source_id:
        errors.append(_error(ErrorCode.INVALID_ENVELOPE, "source_id is required", "source_id"))
    if len(envelope.publisher_session_id) != 16:
        errors.append(
            _error(ErrorCode.INVALID_ENVELOPE, "publisher_session_id must be 16 bytes", "publisher_session_id")
        )
    if envelope.HasField("camera_session_id") and len(envelope.camera_session_id) != 16:
        errors.append(_error(ErrorCode.INVALID_ENVELOPE, "camera_session_id must be 16 bytes", "camera_session_id"))
    spec = PAYLOAD_REGISTRY.get(envelope.payload_type)
    if spec is None:
        errors.append(_error(ErrorCode.UNKNOWN_PAYLOAD_TYPE, "payload_type is not registered", "payload_type"))
        return EnvelopeValidationResult(envelope, None, topic_result, tuple(errors))
    if envelope.message_type != spec.message_type:
        errors.append(_error(ErrorCode.INVALID_ENVELOPE, "message_type does not match payload_type", "message_type"))
    if envelope.payload_size_bytes != len(envelope.payload):
        errors.append(
            _error(ErrorCode.INVALID_ENVELOPE, "payload_size_bytes does not match payload", "payload_size_bytes")
        )
    errors.extend(_validate_topic_conflicts(envelope, topic_result, envelope.payload_type))
    if errors:
        return EnvelopeValidationResult(envelope, None, topic_result, tuple(errors))
    payload = spec.message_class()
    try:
        payload.ParseFromString(envelope.payload)
    except (DecodeError, TypeError, ValueError):
        return EnvelopeValidationResult(
            envelope,
            None,
            topic_result,
            (_error(ErrorCode.INVALID_ENVELOPE, "payload protobuf parsing failed", "payload"),),
        )
    errors.extend(_validate_payload(payload, envelope))
    if isinstance(payload, clock_status_pb2.ClockStatus) and topic_result.kind is TopicKind.SYSTEM_CLOCK:
        if payload.device_id != topic_result.identifiers["device_id"]:
            errors.append(
                _error(ErrorCode.INVALID_ENVELOPE, "topic device_id conflicts with clock payload", "device_id")
            )
    if isinstance(payload, event_pb2.SystemEvent) and topic_result.kind is TopicKind.SYSTEM_EVENT:
        if payload.event_type != topic_result.identifiers["event_type"]:
            errors.append(
                _error(ErrorCode.INVALID_ENVELOPE, "topic event_type conflicts with event payload", "event_type")
            )
    if not errors and sequence_tracker is not None:
        sequence_error = sequence_tracker.observe(envelope)
        if sequence_error is not None:
            errors.append(sequence_error)
    return EnvelopeValidationResult(envelope, payload if not errors else None, topic_result, tuple(errors))


def validate_serialized_envelope(
    data: bytes,
    topic: str | bytes,
    *,
    sequence_tracker: SequenceTracker | None = None,
) -> EnvelopeValidationResult:
    """Parse then validate serialized data; malformed input always becomes a result."""
    topic_result = validate_topic(topic)
    if not isinstance(data, bytes):
        return EnvelopeValidationResult(
            None, None, topic_result, (_error(ErrorCode.INVALID_ENVELOPE, "envelope frame must be bytes", "envelope"),)
        )
    if len(data) > ZMQ_MAXMSGSIZE:
        return EnvelopeValidationResult(
            None,
            None,
            topic_result,
            (_error(ErrorCode.MESSAGE_TOO_LARGE, "envelope exceeds ZeroMQ MAXMSGSIZE", "envelope"),),
        )
    envelope = envelope_pb2.MessageEnvelope()
    try:
        envelope.ParseFromString(data)
    except (DecodeError, TypeError, ValueError):
        return EnvelopeValidationResult(
            None,
            None,
            topic_result,
            (_error(ErrorCode.INVALID_ENVELOPE, "envelope protobuf parsing failed", "envelope"),),
        )
    return validate_envelope(envelope, topic, serialized_size=len(data), sequence_tracker=sequence_tracker)


def validate_multipart(
    frames: Sequence[bytes],
    *,
    sequence_tracker: SequenceTracker | None = None,
) -> EnvelopeValidationResult:
    """Validate the two-frame ZeroMQ data-plane layout and its envelope."""
    if not isinstance(frames, Sequence) or isinstance(frames, (bytes, bytearray, str)) or len(frames) != 2:
        return EnvelopeValidationResult(
            None,
            None,
            None,
            (_error("INVALID_MULTIPART_MESSAGE", "data-plane publication must contain exactly two frames"),),
        )
    topic, envelope_data = frames
    if not isinstance(topic, bytes) or not isinstance(envelope_data, bytes):
        return EnvelopeValidationResult(
            None, None, None, (_error("INVALID_MULTIPART_MESSAGE", "both multipart frames must be bytes"),)
        )
    return validate_serialized_envelope(envelope_data, topic, sequence_tracker=sequence_tracker)


COMMAND_ONEOF_NAMES = frozenset(
    field.name for field in control_pb2.CommandRequest.DESCRIPTOR.oneofs_by_name["command"].fields
)


def validate_command_request(request: control_pb2.CommandRequest) -> ControlValidationResult:
    """Validate UUID and oneof invariants that proto3 itself cannot enforce."""
    errors: list[ValidationError] = []
    if not isinstance(request, control_pb2.CommandRequest):
        return ControlValidationResult((_error(ErrorCode.INVALID_COMMAND, "object is not a CommandRequest"),))
    if len(request.command_id) != 16:
        errors.append(_error(ErrorCode.INVALID_COMMAND, "command_id must be a 16-byte UUID", "command_id"))
    if not request.target_id:
        errors.append(_error(ErrorCode.INVALID_COMMAND, "target_id is required", "target_id"))
    command_name = request.WhichOneof("command")
    if command_name not in COMMAND_ONEOF_NAMES:
        errors.append(_error(ErrorCode.INVALID_COMMAND, "exactly one supported command is required", "command"))
    if command_name == "get_command_status" and len(request.get_command_status.target_command_id) != 16:
        errors.append(
            _error(ErrorCode.INVALID_COMMAND, "target_command_id must be a 16-byte UUID", "target_command_id")
        )
    return ControlValidationResult(tuple(errors))


def validate_command_response(response: control_pb2.CommandResponse) -> ControlValidationResult:
    """Validate response UUID, status, and error-code/status compatibility."""
    if not isinstance(response, control_pb2.CommandResponse):
        return ControlValidationResult((_error(ErrorCode.INVALID_COMMAND, "object is not a CommandResponse"),))
    errors: list[ValidationError] = []
    if len(response.command_id) != 16:
        errors.append(_error(ErrorCode.INVALID_COMMAND, "command_id must be a 16-byte UUID", "command_id"))
    if not response.target_id:
        errors.append(_error(ErrorCode.INVALID_COMMAND, "target_id is required", "target_id"))
    try:
        status_name = control_pb2.CommandStatus.Name(response.status).removeprefix("COMMAND_STATUS_")
    except ValueError:
        status_name = ""
    if status_name == "UNSPECIFIED" or not status_name:
        errors.append(_error(ErrorCode.INVALID_COMMAND, "status must be a known non-zero CommandStatus", "status"))
    if response.error_code:
        if not is_command_response_error_code(response.error_code):
            errors.append(
                _error(ErrorCode.INVALID_COMMAND, "error_code is not permitted in CommandResponse", "error_code")
            )
        elif status_name and ERROR_CODE_CONTRACTS[ErrorCode(response.error_code)].command_status != status_name:
            errors.append(_error(ErrorCode.INVALID_COMMAND, "error_code is incompatible with status", "error_code"))
    return ControlValidationResult(tuple(errors))


def validate_module_registration(registration: registration_pb2.ModuleRegistration) -> ControlValidationResult:
    """Validate the fields required by the control-plane registration contract."""
    errors: list[ValidationError] = []
    if not isinstance(registration, registration_pb2.ModuleRegistration):
        return ControlValidationResult((_error(ErrorCode.INVALID_ENVELOPE, "object is not a ModuleRegistration"),))
    if not registration.module_id or not registration.task_id or not registration.host_device_id:
        errors.append(
            _error(ErrorCode.INVALID_ENVELOPE, "module_id, task_id, and host_device_id are required", "registration")
        )
    if len(registration.module_session_id) != 16:
        errors.append(
            _error(ErrorCode.INVALID_ENVELOPE, "module_session_id must be a 16-byte UUID", "module_session_id")
        )
    try:
        state_name = module_state_pb2.ComponentState.Name(registration.current_state)
    except ValueError:
        state_name = ""
    if state_name == "COMPONENT_STATE_UNSPECIFIED" or not state_name:
        errors.append(
            _error(ErrorCode.INVALID_ENVELOPE, "current_state must be a known non-zero state", "current_state")
        )
    if registration.process_id == 0:
        errors.append(_error(ErrorCode.INVALID_ENVELOPE, "process_id must be non-zero", "process_id"))
    invalid_commands = set(registration.supported_command_types) - COMMAND_ONEOF_NAMES
    if invalid_commands:
        errors.append(
            _error(
                ErrorCode.INVALID_ENVELOPE,
                "supported_command_types contains non-canonical name",
                "supported_command_types",
            )
        )
    return ControlValidationResult(tuple(errors))


def validate_module_registration_response(
    response: registration_pb2.ModuleRegistrationResponse,
) -> ControlValidationResult:
    """Validate the canonical error-code field on a registration reply."""
    if not isinstance(response, registration_pb2.ModuleRegistrationResponse):
        return ControlValidationResult(
            (_error(ErrorCode.INVALID_COMMAND, "object is not a ModuleRegistrationResponse"),)
        )
    if response.error_code and not is_error_code(response.error_code):
        return ControlValidationResult(
            (_error(ErrorCode.INVALID_COMMAND, "error_code is not canonical", "error_code"),)
        )
    return ControlValidationResult()
