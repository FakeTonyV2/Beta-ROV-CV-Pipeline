"""Pure validation of data-plane topic bytes and their canonical forms."""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import StrEnum

_TOPIC_RE = re.compile(r"^[a-z0-9_.]+$")
MAX_TOPIC_BYTES = 128


class TopicKind(StrEnum):
    CV_RESULT = "cv.result"
    CV_HEALTH = "cv.health"
    CV_STATE = "cv.state"
    CV_FRAME_INDEX = "cv.frame_index"
    CV_DEBUG_SNAPSHOT = "cv.debug_snapshot"
    SYSTEM_CLOCK = "system.clock"
    SYSTEM_HEALTH = "system.health"
    SYSTEM_EVENT = "system.event"


@dataclass(frozen=True)
class TopicValidationResult:
    topic: str | None
    kind: TopicKind | None
    identifiers: dict[str, str]
    errors: tuple[str, ...] = ()

    @property
    def valid(self) -> bool:
        return not self.errors


def validate_topic(topic: str | bytes) -> TopicValidationResult:
    """Validate a topic without touching transport or protobuf state."""
    if isinstance(topic, bytes):
        try:
            topic = topic.decode("utf-8")
        except UnicodeDecodeError:
            return TopicValidationResult(None, None, {}, ("topic is not valid UTF-8",))
    if not isinstance(topic, str):
        return TopicValidationResult(None, None, {}, ("topic must be str or UTF-8 bytes",))
    if len(topic.encode("utf-8")) > MAX_TOPIC_BYTES:
        return TopicValidationResult(topic, None, {}, ("topic exceeds 128 bytes",))
    if not _TOPIC_RE.fullmatch(topic):
        return TopicValidationResult(topic, None, {}, ("topic must use lowercase ASCII",))
    if topic.startswith(".") or topic.endswith(".") or ".." in topic:
        return TopicValidationResult(topic, None, {}, ("topic has invalid period placement",))

    parts = topic.split(".")
    templates: tuple[tuple[TopicKind, tuple[str, ...]], ...] = (
        (TopicKind.CV_RESULT, ("cv", "result", "task_id", "camera_id")),
        (TopicKind.CV_HEALTH, ("cv", "health", "source_id")),
        (TopicKind.CV_STATE, ("cv", "state", "source_id")),
        (TopicKind.CV_FRAME_INDEX, ("cv", "frame_index", "camera_id")),
        (TopicKind.CV_DEBUG_SNAPSHOT, ("cv", "debug_snapshot", "camera_id")),
        (TopicKind.SYSTEM_CLOCK, ("system", "clock", "device_id")),
        (TopicKind.SYSTEM_HEALTH, ("system", "health", "device_id")),
        (TopicKind.SYSTEM_EVENT, ("system", "event", "event_type")),
    )
    for kind, template in templates:
        if len(parts) != len(template):
            continue
        identifiers: dict[str, str] = {}
        for part, expected in zip(parts, template, strict=True):
            if expected in {"task_id", "camera_id", "source_id", "device_id", "event_type"}:
                identifiers[expected] = part
            elif part != expected:
                break
        else:
            return TopicValidationResult(topic, kind, identifiers)
    return TopicValidationResult(topic, None, {}, ("topic is not a required topic form",))
