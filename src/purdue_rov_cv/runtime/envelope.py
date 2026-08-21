"""Publisher-side builder reusing the canonical protobuf and wire validator."""

from __future__ import annotations

import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass

from google.protobuf.message import Message
from purdue_rov.cv.v1 import envelope_pb2

from purdue_rov_cv.wire.payloads import PAYLOAD_REGISTRY
from purdue_rov_cv.wire.validators import (
    EnvelopeValidationResult,
    SequenceTracker,
    ValidationError,
    validate_envelope,
    validate_multipart,
)

from .metrics import RuntimeMetrics
from .publisher import PublicationIdentity, PublisherSequence


@dataclass(frozen=True)
class BuiltEnvelope:
    topic: bytes
    envelope: envelope_pb2.MessageEnvelope
    serialized_envelope: bytes
    payload: Message
    publication: PublicationIdentity

    @property
    def frames(self) -> tuple[bytes, bytes]:
        return self.topic, self.serialized_envelope


class EnvelopeBuildError(ValueError):
    def __init__(self, errors: tuple[ValidationError, ...], publication: PublicationIdentity):
        self.errors = errors
        self.publication = publication
        super().__init__("; ".join(f"{error.code} {error.field or '<envelope>'}: {error.detail}" for error in errors))


class EnvelopeBuilder:
    """Build and validate a two-frame publication before a transport send is attempted."""

    def __init__(
        self,
        publisher: PublisherSequence,
        *,
        unix_time_ns: Callable[[], int] = time.time_ns,
        monotonic_ns: Callable[[], int] = time.monotonic_ns,
    ) -> None:
        self._publisher = publisher
        self._unix_time_ns = unix_time_ns
        self._monotonic_ns = monotonic_ns

    def build(
        self,
        *,
        topic: str | bytes,
        payload_type: str,
        payload: Message,
        task_id: str,
        source_id: str,
        camera_id: str | None = None,
        camera_session_id: bytes | None = None,
        frame_number: int | None = None,
        capture_time_unix_ns: int | None = None,
    ) -> BuiltEnvelope:
        # Sequence consumption occurs first so rejected or failed sends never reuse it.
        publication = self._publisher.next_attempt()
        spec = PAYLOAD_REGISTRY.get(payload_type)
        if spec is None:
            raise EnvelopeBuildError(
                (ValidationError("UNKNOWN_PAYLOAD_TYPE", "payload_type is not registered", "payload_type"),),
                publication,
            )
        if not isinstance(payload, spec.message_class):
            raise EnvelopeBuildError(
                (ValidationError("INVALID_ENVELOPE", "payload instance does not match payload_type", "payload"),),
                publication,
            )
        serialized_payload = payload.SerializeToString()
        envelope = envelope_pb2.MessageEnvelope(
            message_type=spec.message_type,
            payload_type=payload_type,
            task_id=task_id,
            source_id=source_id,
            publish_time_unix_ns=self._unix_time_ns(),
            source_monotonic_ns=self._monotonic_ns(),
            publisher_session_id=publication.publisher_session_id,
            sequence_number=publication.sequence_number,
            schema_version=1,
            payload_encoding="protobuf",
            payload_size_bytes=len(serialized_payload),
            payload=serialized_payload,
        )
        if camera_id is not None:
            envelope.camera_id = camera_id
        if camera_session_id is not None:
            envelope.camera_session_id = camera_session_id
        if frame_number is not None:
            envelope.frame_number = frame_number
        if capture_time_unix_ns is not None:
            envelope.capture_time_unix_ns = capture_time_unix_ns
        serialized_envelope = envelope.SerializeToString()
        result = validate_envelope(envelope, topic, serialized_size=len(serialized_envelope))
        if not result.valid:
            raise EnvelopeBuildError(result.errors, publication)
        topic_bytes = topic if isinstance(topic, bytes) else topic.encode("utf-8")
        return BuiltEnvelope(topic_bytes, envelope, serialized_envelope, payload, publication)


def validate_received_multipart(
    frames: Sequence[bytes],
    *,
    metrics: RuntimeMetrics,
    sequence_tracker: SequenceTracker | None = None,
) -> EnvelopeValidationResult:
    """Integrate wire validation with receiver-side canonical counters."""
    result = validate_multipart(frames, sequence_tracker=sequence_tracker)
    if not result.valid:
        metrics.increment("invalid_messages")
        codes = {error.code for error in result.errors}
        if "INVALID_MULTIPART_MESSAGE" in codes:
            metrics.increment("invalid_multipart_message")
        if "UNKNOWN_PAYLOAD_TYPE" in codes:
            metrics.increment("unknown_payload_types")
    else:
        metrics.increment("messages_received")
    return result


class ReceivedMultipartValidator:
    """Persistent receiver validation with missing-sequence-value accounting.

    Gap semantics are the number of missing sequence values, keyed by the
    publisher session and source ID. New sessions reset naturally; duplicate or
    reordered messages are invalid and never produce negative gap counts.
    """

    def __init__(self, metrics: RuntimeMetrics) -> None:
        self._metrics = metrics

        def observe_gap(missing: int) -> None:
            metrics.increment("observed_sequence_gaps", missing)

        self._sequence_tracker = SequenceTracker(gap_observer=observe_gap)

    def validate(self, frames: Sequence[bytes]) -> EnvelopeValidationResult:
        return validate_received_multipart(
            frames,
            metrics=self._metrics,
            sequence_tracker=self._sequence_tracker,
        )


__all__ = [
    "BuiltEnvelope",
    "EnvelopeBuildError",
    "EnvelopeBuilder",
    "ReceivedMultipartValidator",
    "validate_received_multipart",
]
