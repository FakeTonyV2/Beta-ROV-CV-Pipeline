"""Chunked indexed MCAP recording of canonical MessageEnvelope bytes."""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from threading import Event
from typing import Protocol

from mcap.writer import CompressionType, IndexType, Writer
from mcap_protobuf.schema import build_file_descriptor_set
from purdue_rov.cv.v1 import envelope_pb2

from purdue_rov_cv.runtime.queues import ReceiveStatus, RecorderQueue
from purdue_rov_cv.runtime.shutdown import ShutdownToken

MESSAGE_ENVELOPE_SCHEMA_NAME = "purdue_rov.cv.v1.MessageEnvelope"
MESSAGE_ENVELOPE_SCHEMA_ENCODING = "protobuf"
MESSAGE_ENCODING = "protobuf"
FLUSH_MESSAGE_COUNT = 100
FLUSH_INTERVAL_NS = 250_000_000
MAX_CHANNELS_PER_SESSION = 4096
MCAP_SEQUENCE_MAX = (1 << 32) - 1


@dataclass(frozen=True, slots=True)
class StructuredRecord:
    topic: str
    envelope: envelope_pb2.MessageEnvelope
    serialized_envelope: bytes
    receive_time_unix_ns: int


class StructuredSink(Protocol):
    def write(self, record: StructuredRecord) -> None: ...

    def flush(self) -> None: ...

    def finish(self) -> None: ...


@dataclass(slots=True)
class FlushPolicy:
    """Pure count/deadline policy used by the writer loop and deterministic tests."""

    messages: int = 0
    deadline_ns: int | None = None

    def on_message(self, now_ns: int) -> bool:
        if self.messages == 0:
            self.deadline_ns = now_ns + FLUSH_INTERVAL_NS
        self.messages += 1
        return self.messages >= FLUSH_MESSAGE_COUNT or self.time_due(now_ns)

    def time_due(self, now_ns: int) -> bool:
        return self.messages > 0 and self.deadline_ns is not None and now_ns >= self.deadline_ns

    def reset(self) -> None:
        self.messages = 0
        self.deadline_ns = None


class McapSessionWriter:
    """One schema and one stable channel per topic for a structured session."""

    def __init__(
        self,
        path: Path,
        *,
        chunk_size_bytes: int = 1_048_576,
        compression: str = "zstd",
    ) -> None:
        if chunk_size_bytes != 1_048_576:
            raise ValueError("Phase 8 MCAP chunk size must be exactly 1,048,576 bytes")
        if compression.lower() != "zstd":
            raise ValueError("Phase 8 MCAP compression must be zstd")
        self.path = path
        self._stream = path.open("xb")
        try:
            self._writer = Writer(
                self._stream,
                chunk_size=chunk_size_bytes,
                compression=CompressionType.ZSTD,
                index_types=IndexType.ALL,
                repeat_channels=True,
                repeat_schemas=True,
                use_chunking=True,
                use_statistics=True,
                use_summary_offsets=True,
                enable_crcs=True,
            )
            self._writer.start(profile="protobuf", library="purdue-rov-cv-recorder")
            descriptor_set = build_file_descriptor_set(envelope_pb2.MessageEnvelope)
            self.schema_id = self._writer.register_schema(
                name=MESSAGE_ENVELOPE_SCHEMA_NAME,
                encoding=MESSAGE_ENVELOPE_SCHEMA_ENCODING,
                data=descriptor_set.SerializeToString(),
            )
            self._channels: dict[str, int] = {}
            self._finished = False
        except BaseException:
            self._stream.close()
            try:
                path.unlink()
            except OSError:
                pass
            raise

    @property
    def channels(self) -> dict[str, int]:
        return dict(self._channels)

    def write(self, record: StructuredRecord) -> None:
        if self._finished:
            raise RuntimeError("MCAP writer is finalized")
        if record.envelope.sequence_number > MCAP_SEQUENCE_MAX:
            raise ValueError(
                "MessageEnvelope sequence_number exceeds the MCAP v1 uint32 sequence field; "
                "recording refuses to truncate or rewrite it"
            )
        channel_id = self._channels.get(record.topic)
        if channel_id is None:
            if len(self._channels) >= MAX_CHANNELS_PER_SESSION:
                raise RuntimeError(f"MCAP channel limit {MAX_CHANNELS_PER_SESSION} exceeded")
            channel_id = self._writer.register_channel(
                topic=record.topic,
                message_encoding=MESSAGE_ENCODING,
                schema_id=self.schema_id,
            )
            self._channels[record.topic] = channel_id
        self._writer.add_message(
            channel_id=channel_id,
            log_time=record.receive_time_unix_ns,
            publish_time=record.envelope.publish_time_unix_ns,
            sequence=record.envelope.sequence_number,
            data=record.serialized_envelope,
        )

    def flush(self) -> None:
        if self._finished:
            return
        # mcap-python 1.4 has no public runtime chunk flush. Phase 8 pins the
        # 1.x API, whose implementation provides this name-mangled operation.
        # Finalizing the current chunk is required so a 1-99 message idle tail
        # is actually written rather than remaining solely in ChunkBuilder.
        self._writer._Writer__finalize_chunk()
        self._stream.flush()

    def finish(self) -> None:
        if self._finished:
            return
        try:
            self._writer.finish()
            self._stream.flush()
        finally:
            self._finished = True
            self._stream.close()


class StructuredWriterLoop:
    """Count-or-deadline flush policy with bounded, interruptible polling."""

    def __init__(
        self,
        records: RecorderQueue[StructuredRecord],
        sink: StructuredSink,
        shutdown: ShutdownToken,
        *,
        monotonic_ns: Callable[[], int] = time.monotonic_ns,
        accepting_done: Event | None = None,
        drain_deadline_seconds: float = 3.0,
        on_abandoned: Callable[[int], None] | None = None,
    ) -> None:
        self.records = records
        self.sink = sink
        self.shutdown = shutdown
        self._monotonic_ns = monotonic_ns
        self.accepting_done = accepting_done or Event()
        self.drain_deadline_seconds = drain_deadline_seconds
        self._on_abandoned = on_abandoned
        self.messages_since_flush = 0
        self.flush_count = 0
        self.abandoned_records = 0
        self.policy = FlushPolicy()

    def _flush(self) -> None:
        if self.messages_since_flush:
            self.sink.flush()
            self.messages_since_flush = 0
            self.flush_count += 1
            self.policy.reset()

    def run(self) -> None:
        drain_started_ns: int | None = None
        try:
            while True:
                now = self._monotonic_ns()
                if self.shutdown.is_requested and drain_started_ns is None:
                    drain_started_ns = now
                if drain_started_ns is not None and now - drain_started_ns >= int(
                    self.drain_deadline_seconds * 1_000_000_000
                ):
                    self.abandoned_records = self.records.qsize()
                    if self.abandoned_records and self._on_abandoned is not None:
                        self._on_abandoned(self.abandoned_records)
                    break
                if self.shutdown.is_requested and self.accepting_done.is_set() and self.records.qsize() == 0:
                    break
                timeout = 0.250
                if self.policy.deadline_ns is not None:
                    timeout = min(timeout, max(0.0, (self.policy.deadline_ns - now) / 1_000_000_000))
                received = self.records.receive(timeout_seconds=timeout)
                if received.status is ReceiveStatus.ITEM:
                    assert received.item is not None
                    self.sink.write(received.item)
                    self.messages_since_flush += 1
                    if self.policy.on_message(self._monotonic_ns()):
                        self._flush()
                now = self._monotonic_ns()
                if self.policy.time_due(now):
                    self._flush()
        finally:
            try:
                self._flush()
            finally:
                self.sink.finish()


__all__ = [
    "FLUSH_INTERVAL_NS",
    "FLUSH_MESSAGE_COUNT",
    "FlushPolicy",
    "MESSAGE_ENVELOPE_SCHEMA_ENCODING",
    "MESSAGE_ENVELOPE_SCHEMA_NAME",
    "MESSAGE_ENCODING",
    "MAX_CHANNELS_PER_SESSION",
    "McapSessionWriter",
    "StructuredRecord",
    "StructuredSink",
    "StructuredWriterLoop",
]
