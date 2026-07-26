"""Static mapping from wire payload_type strings to protobuf classes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TypeAlias

from google.protobuf.message import Message
from purdue_rov.cv.v1 import (
    bounding_box_pb2,
    classification_pb2,
    clock_status_pb2,
    debug_snapshot_pb2,
    diagnostics_pb2,
    envelope_pb2,
    event_pb2,
    frame_index_pb2,
    module_state_pb2,
    target_pose_pb2,
)

from .topics import TopicKind

MessageClass: TypeAlias = type[Message]


@dataclass(frozen=True)
class PayloadSpec:
    message_class: MessageClass
    message_type: int
    topic_kinds: frozenset[TopicKind]


# This registry is deliberately static. Never import from payload_type text.
PAYLOAD_REGISTRY: dict[str, PayloadSpec] = {
    "bounding_boxes_v1": PayloadSpec(
        bounding_box_pb2.BoundingBoxResult,
        envelope_pb2.CV_RESULT,
        frozenset({TopicKind.CV_RESULT}),
    ),
    "classification_result_v1": PayloadSpec(
        classification_pb2.ClassificationResult,
        envelope_pb2.CV_RESULT,
        frozenset({TopicKind.CV_RESULT}),
    ),
    "target_pose_v1": PayloadSpec(
        target_pose_pb2.TargetPoseResult,
        envelope_pb2.CV_RESULT,
        frozenset({TopicKind.CV_RESULT}),
    ),
    "diagnostic_status_v1": PayloadSpec(
        diagnostics_pb2.DiagnosticStatus,
        envelope_pb2.DIAGNOSTIC,
        frozenset({TopicKind.CV_HEALTH, TopicKind.SYSTEM_HEALTH}),
    ),
    "module_state_v1": PayloadSpec(
        module_state_pb2.ModuleState,
        envelope_pb2.MODULE_STATE,
        frozenset({TopicKind.CV_STATE}),
    ),
    "frame_index_v1": PayloadSpec(
        frame_index_pb2.FrameIndex,
        envelope_pb2.FRAME_INDEX,
        frozenset({TopicKind.CV_FRAME_INDEX}),
    ),
    "debug_snapshot_v1": PayloadSpec(
        debug_snapshot_pb2.DebugSnapshot,
        envelope_pb2.DEBUG_SNAPSHOT,
        frozenset({TopicKind.CV_DEBUG_SNAPSHOT}),
    ),
    "clock_status_v1": PayloadSpec(
        clock_status_pb2.ClockStatus,
        envelope_pb2.CLOCK_STATUS,
        frozenset({TopicKind.SYSTEM_CLOCK}),
    ),
    "system_event_v1": PayloadSpec(
        event_pb2.SystemEvent,
        envelope_pb2.EVENT,
        frozenset({TopicKind.SYSTEM_EVENT}),
    ),
}
