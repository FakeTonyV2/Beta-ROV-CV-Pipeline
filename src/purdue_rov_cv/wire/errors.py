"""Canonical error identifiers and their machine-readable contract metadata."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class ErrorCode(StrEnum):
    CONFIG_INVALID = "CONFIG_INVALID"
    RESTART_REQUIRED = "RESTART_REQUIRED"
    CAMERA_NOT_FOUND = "CAMERA_NOT_FOUND"
    CAMERA_MODE_UNSUPPORTED = "CAMERA_MODE_UNSUPPORTED"
    CAMERA_FRAME_TIMEOUT = "CAMERA_FRAME_TIMEOUT"
    SHARED_MEMORY_INVALID = "SHARED_MEMORY_INVALID"
    MODEL_NOT_FOUND = "MODEL_NOT_FOUND"
    MODEL_HASH_MISMATCH = "MODEL_HASH_MISMATCH"
    MODEL_LOAD_FAILED = "MODEL_LOAD_FAILED"
    RUNTIME_UNAVAILABLE = "RUNTIME_UNAVAILABLE"
    TARGET_INCOMPATIBLE = "TARGET_INCOMPATIBLE"
    BROKER_UNAVAILABLE = "BROKER_UNAVAILABLE"
    CONTROL_ROUTER_UNAVAILABLE = "CONTROL_ROUTER_UNAVAILABLE"
    TARGET_UNAVAILABLE = "TARGET_UNAVAILABLE"
    TARGET_SEND_TIMEOUT = "TARGET_SEND_TIMEOUT"
    COMMAND_ACK_TIMEOUT = "COMMAND_ACK_TIMEOUT"
    COMMAND_COMPLETION_TIMEOUT = "COMMAND_COMPLETION_TIMEOUT"
    COMMAND_OUTCOME_UNKNOWN = "COMMAND_OUTCOME_UNKNOWN"
    DUPLICATE_COMMAND_ID = "DUPLICATE_COMMAND_ID"
    INVALID_COMMAND = "INVALID_COMMAND"
    INVALID_STATE_TRANSITION = "INVALID_STATE_TRANSITION"
    MODULE_BUSY = "MODULE_BUSY"
    PROCESSING_FAILURE = "PROCESSING_FAILURE"
    PROCESSING_WATCHDOG_EXCEEDED = "PROCESSING_WATCHDOG_EXCEEDED"
    UNKNOWN_PAYLOAD_TYPE = "UNKNOWN_PAYLOAD_TYPE"
    UNSUPPORTED_SCHEMA_VERSION = "UNSUPPORTED_SCHEMA_VERSION"
    INVALID_ENVELOPE = "INVALID_ENVELOPE"
    MESSAGE_TOO_LARGE = "MESSAGE_TOO_LARGE"
    CLOCK_UNSYNCHRONIZED = "CLOCK_UNSYNCHRONIZED"
    VIDEO_STREAM_LOST = "VIDEO_STREAM_LOST"
    FRAME_INDEX_MISS = "FRAME_INDEX_MISS"
    RECORDER_QUEUE_FULL = "RECORDER_QUEUE_FULL"
    DISK_SPACE_LOW = "DISK_SPACE_LOW"
    INTERNAL_ERROR = "INTERNAL_ERROR"


@dataclass(frozen=True)
class ErrorCodeContract:
    emitter: str
    trigger: str
    command_status: str
    state_effect: str
    exit_code: str
    recovery: str
    required_test: str
    command_response_allowed: bool


def _contract(
    emitter: str,
    trigger: str,
    command_status: str,
    state_effect: str,
    exit_code: str,
    recovery: str,
    required_test: str,
    *,
    command_response_allowed: bool,
) -> ErrorCodeContract:
    return ErrorCodeContract(
        emitter,
        trigger,
        command_status,
        state_effect,
        exit_code,
        recovery,
        required_test,
        command_response_allowed,
    )


ERROR_CODE_CONTRACTS: dict[ErrorCode, ErrorCodeContract] = {
    ErrorCode.CONFIG_INVALID: _contract(
        "module",
        "startup config fails validation",
        "N/A (diagnostic)",
        "ERROR",
        "78",
        "fix config and restart",
        "test_config_invalid",
        command_response_allowed=False,
    ),
    ErrorCode.RESTART_REQUIRED: _contract(
        "module",
        "immutable setting changed",
        "REJECTED",
        "DEGRADED",
        "0",
        "restart module",
        "test_restart_required",
        command_response_allowed=True,
    ),
    ErrorCode.CAMERA_NOT_FOUND: _contract(
        "camera service",
        "configured device missing",
        "FAILED",
        "ERROR",
        "75",
        "restore device and retry",
        "test_camera_not_found",
        command_response_allowed=True,
    ),
    ErrorCode.CAMERA_MODE_UNSUPPORTED: _contract(
        "camera service",
        "requested format unsupported",
        "FAILED",
        "ERROR",
        "0",
        "select supported mode",
        "test_camera_mode_unsupported",
        command_response_allowed=True,
    ),
    ErrorCode.CAMERA_FRAME_TIMEOUT: _contract(
        "camera service",
        "frame deadline elapsed",
        "N/A (diagnostic)",
        "DEGRADED",
        "0",
        "restart capture pipeline",
        "test_camera_frame_timeout",
        command_response_allowed=False,
    ),
    ErrorCode.SHARED_MEMORY_INVALID: _contract(
        "camera/module",
        "shared-memory contract invalid",
        "N/A (diagnostic)",
        "ERROR",
        "75",
        "restart affected services",
        "test_shared_memory_invalid",
        command_response_allowed=False,
    ),
    ErrorCode.MODEL_NOT_FOUND: _contract(
        "module",
        "model artifact absent",
        "FAILED",
        "ERROR",
        "78",
        "deploy model then retry",
        "test_model_not_found",
        command_response_allowed=True,
    ),
    ErrorCode.MODEL_HASH_MISMATCH: _contract(
        "module",
        "model checksum differs",
        "FAILED",
        "ERROR",
        "78",
        "replace model artifact",
        "test_model_hash_mismatch",
        command_response_allowed=True,
    ),
    ErrorCode.MODEL_LOAD_FAILED: _contract(
        "module",
        "model loader errors",
        "FAILED",
        "ERROR",
        "78",
        "inspect artifact/runtime",
        "test_model_load_failed",
        command_response_allowed=True,
    ),
    ErrorCode.RUNTIME_UNAVAILABLE: _contract(
        "module",
        "required inference runtime absent",
        "FAILED",
        "ERROR",
        "78",
        "install compatible runtime",
        "test_runtime_unavailable",
        command_response_allowed=True,
    ),
    ErrorCode.TARGET_INCOMPATIBLE: _contract(
        "module",
        "artifact cannot run on host",
        "FAILED",
        "ERROR",
        "78",
        "deploy compatible artifact",
        "test_target_incompatible",
        command_response_allowed=True,
    ),
    ErrorCode.BROKER_UNAVAILABLE: _contract(
        "publisher/subscriber",
        "data broker unreachable",
        "N/A (diagnostic)",
        "DEGRADED",
        "0",
        "reconnect with backoff",
        "test_broker_unavailable",
        command_response_allowed=False,
    ),
    ErrorCode.CONTROL_ROUTER_UNAVAILABLE: _contract(
        "control client",
        "router unreachable",
        "N/A (client outcome)",
        "unchanged",
        "0",
        "retry command",
        "test_control_router_unavailable",
        command_response_allowed=False,
    ),
    ErrorCode.TARGET_UNAVAILABLE: _contract(
        "control router",
        "target not registered",
        "REJECTED",
        "unchanged",
        "0",
        "start target then retry",
        "test_target_unavailable",
        command_response_allowed=True,
    ),
    ErrorCode.TARGET_SEND_TIMEOUT: _contract(
        "control router",
        "dealer send deadline elapsed",
        "REJECTED",
        "unchanged",
        "0",
        "retry command",
        "test_target_send_timeout",
        command_response_allowed=True,
    ),
    ErrorCode.COMMAND_ACK_TIMEOUT: _contract(
        "control client",
        "no received/accepted reply",
        "N/A (client outcome)",
        "unchanged",
        "0",
        "query command status",
        "test_command_ack_timeout",
        command_response_allowed=False,
    ),
    ErrorCode.COMMAND_COMPLETION_TIMEOUT: _contract(
        "control client",
        "completion deadline elapsed",
        "N/A (client outcome)",
        "unchanged",
        "0",
        "query command status",
        "test_command_completion_timeout",
        command_response_allowed=False,
    ),
    ErrorCode.COMMAND_OUTCOME_UNKNOWN: _contract(
        "control client",
        "status cannot be determined",
        "OUTCOME_UNKNOWN",
        "unchanged",
        "0",
        "query status or inspect diagnostics",
        "test_command_outcome_unknown",
        command_response_allowed=True,
    ),
    ErrorCode.DUPLICATE_COMMAND_ID: _contract(
        "target module",
        "command UUID already seen",
        "REJECTED",
        "unchanged",
        "0",
        "use cached result or new UUID",
        "test_duplicate_command_id",
        command_response_allowed=True,
    ),
    ErrorCode.INVALID_COMMAND: _contract(
        "target module",
        "request shape or values invalid",
        "REJECTED",
        "unchanged",
        "0",
        "correct request",
        "test_invalid_command",
        command_response_allowed=True,
    ),
    ErrorCode.INVALID_STATE_TRANSITION: _contract(
        "target module",
        "command invalid for state",
        "REJECTED",
        "unchanged",
        "0",
        "wait for valid state",
        "test_invalid_state_transition",
        command_response_allowed=True,
    ),
    ErrorCode.MODULE_BUSY: _contract(
        "target module",
        "exclusive work in progress",
        "REJECTED",
        "unchanged",
        "0",
        "retry after completion",
        "test_module_busy",
        command_response_allowed=True,
    ),
    ErrorCode.PROCESSING_FAILURE: _contract(
        "module",
        "inference/postprocess exception",
        "N/A (diagnostic)",
        "DEGRADED",
        "0",
        "restart task or inspect logs",
        "test_processing_failure",
        command_response_allowed=False,
    ),
    ErrorCode.PROCESSING_WATCHDOG_EXCEEDED: _contract(
        "module",
        "processing deadline exceeded",
        "N/A (diagnostic)",
        "DEGRADED",
        "75",
        "reduce load or restart",
        "test_processing_watchdog_exceeded",
        command_response_allowed=False,
    ),
    ErrorCode.UNKNOWN_PAYLOAD_TYPE: _contract(
        "receiver",
        "payload_type absent from static registry",
        "N/A (data-plane drop)",
        "unchanged",
        "0",
        "update compatible receiver",
        "test_unknown_payload_type",
        command_response_allowed=False,
    ),
    ErrorCode.UNSUPPORTED_SCHEMA_VERSION: _contract(
        "receiver",
        "schema_version is not supported",
        "N/A (data-plane drop)",
        "unchanged",
        "0",
        "upgrade compatible component",
        "test_unsupported_schema_version",
        command_response_allowed=False,
    ),
    ErrorCode.INVALID_ENVELOPE: _contract(
        "receiver",
        "envelope or payload validation fails",
        "N/A (data-plane drop)",
        "unchanged",
        "0",
        "inspect publisher/logs",
        "test_invalid_envelope",
        command_response_allowed=False,
    ),
    ErrorCode.MESSAGE_TOO_LARGE: _contract(
        "publisher/receiver",
        "envelope exceeds applicable limit",
        "N/A (data-plane drop)",
        "unchanged",
        "0",
        "reduce payload",
        "test_message_too_large",
        command_response_allowed=False,
    ),
    ErrorCode.CLOCK_UNSYNCHRONIZED: _contract(
        "clock monitor",
        "clock offset invalid",
        "N/A (diagnostic)",
        "DEGRADED",
        "0",
        "restore time sync",
        "test_clock_unsynchronized",
        command_response_allowed=False,
    ),
    ErrorCode.VIDEO_STREAM_LOST: _contract(
        "video receiver",
        "RTP/video stream absent",
        "N/A (diagnostic)",
        "DEGRADED",
        "0",
        "restart stream",
        "test_video_stream_lost",
        command_response_allowed=False,
    ),
    ErrorCode.FRAME_INDEX_MISS: _contract(
        "video receiver",
        "no matching frame index",
        "N/A (diagnostic)",
        "unchanged",
        "0",
        "continue and record metric",
        "test_frame_index_miss",
        command_response_allowed=False,
    ),
    ErrorCode.RECORDER_QUEUE_FULL: _contract(
        "recorder",
        "record queue capacity reached",
        "FAILED",
        "DEGRADED",
        "0",
        "drain queue or reduce rate",
        "test_recorder_queue_full",
        command_response_allowed=True,
    ),
    ErrorCode.DISK_SPACE_LOW: _contract(
        "recorder",
        "free disk below threshold",
        "FAILED",
        "DEGRADED",
        "0",
        "free space then retry",
        "test_disk_space_low",
        command_response_allowed=True,
    ),
    ErrorCode.INTERNAL_ERROR: _contract(
        "any component",
        "unexpected failure",
        "FAILED",
        "ERROR",
        "70",
        "inspect logs and restart",
        "test_internal_error",
        command_response_allowed=True,
    ),
}


COMMAND_RESPONSE_ERROR_CODES = frozenset(
    code for code, contract in ERROR_CODE_CONTRACTS.items() if contract.command_response_allowed
)


def is_command_response_error_code(value: str) -> bool:
    """Return whether a canonical code is permitted in CommandResponse.error_code."""
    try:
        return ErrorCode(value) in COMMAND_RESPONSE_ERROR_CODES
    except ValueError:
        return False


def is_error_code(value: str) -> bool:
    """Return whether *value* is one of the canonical protocol error codes."""
    try:
        ErrorCode(value)
    except ValueError:
        return False
    return True
