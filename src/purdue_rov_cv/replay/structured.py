"""Index-required MCAP replay with absolute monotonic scheduling."""

from __future__ import annotations

import time
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from enum import StrEnum
from ipaddress import ip_address
from pathlib import Path
from urllib.parse import urlsplit

import zmq
from mcap.reader import SeekingReader, make_reader
from mcap_protobuf.schema import build_file_descriptor_set
from purdue_rov.cv.v1 import envelope_pb2

from purdue_rov_cv.module_runner.publisher import configure_result_publisher
from purdue_rov_cv.recording.mcap_writer import (
    MESSAGE_ENCODING,
    MESSAGE_ENVELOPE_SCHEMA_ENCODING,
    MESSAGE_ENVELOPE_SCHEMA_NAME,
)
from purdue_rov_cv.runtime.shutdown import ShutdownToken
from purdue_rov_cv.wire.validators import validate_envelope

DEFAULT_REPLAY_PUBLISHER_ENDPOINT = "tcp://127.0.0.1:5655"
DEFAULT_REPLAY_SUBSCRIBER_ENDPOINT = "tcp://127.0.0.1:5656"


class ReplayRate(StrEnum):
    QUARTER = "0.25"
    HALF = "0.5"
    NORMAL = "1"
    DOUBLE = "2"
    MAXIMUM = "max"

    @property
    def multiplier(self) -> float | None:
        return None if self is ReplayRate.MAXIMUM else float(self.value)

    @classmethod
    def parse(cls, value: str | float) -> ReplayRate:
        normalized = str(value).lower().removesuffix("x").strip()
        aliases = {
            "0.25": cls.QUARTER,
            "0.5": cls.HALF,
            "1": cls.NORMAL,
            "1.0": cls.NORMAL,
            "2": cls.DOUBLE,
            "2.0": cls.DOUBLE,
            "max": cls.MAXIMUM,
        }
        try:
            return aliases[normalized]
        except KeyError as error:
            raise ValueError("rate must be one of 0.25, 0.5, 1, 2, or max") from error


@dataclass(frozen=True, slots=True)
class ReplayRecord:
    topic: str
    envelope: envelope_pb2.MessageEnvelope
    serialized_envelope: bytes
    log_time: int
    publish_time: int


class IndexedMcapSource:
    """Reject missing/corrupt/unindexed files instead of scanning from byte zero."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def records(self, *, start_time: int | None = None, end_time: int | None = None) -> Iterator[ReplayRecord]:
        try:
            stream = self.path.open("rb")
        except OSError as error:
            raise OSError(f"cannot open MCAP recording {self.path}: {error}") from error
        with stream:
            try:
                reader = make_reader(stream, validate_crcs=True)
                if not isinstance(reader, SeekingReader):
                    raise ValueError("MCAP replay requires a seekable indexed reader")
                summary = reader.get_summary()
                if summary is None:
                    raise ValueError("MCAP recording has no summary")
                if len(summary.schemas) != 1:
                    raise ValueError("MCAP recording must contain exactly one schema")
                schema = next(iter(summary.schemas.values()))
                expected_descriptor = build_file_descriptor_set(envelope_pb2.MessageEnvelope).SerializeToString()
                if (
                    schema.name != MESSAGE_ENVELOPE_SCHEMA_NAME
                    or schema.encoding != MESSAGE_ENVELOPE_SCHEMA_ENCODING
                    or schema.data != expected_descriptor
                ):
                    raise ValueError("MCAP recording does not contain the canonical MessageEnvelope protobuf schema")
                for channel in summary.channels.values():
                    if channel.schema_id != schema.id or channel.message_encoding != MESSAGE_ENCODING:
                        raise ValueError("MCAP channel does not use the canonical MessageEnvelope schema and encoding")
                if summary.statistics is None:
                    raise ValueError("MCAP recording has no summary statistics")
                message_count = summary.statistics.message_count
                if message_count and not summary.chunk_indexes:
                    raise ValueError("MCAP recording has messages but no chunk index")
                for schema, channel, message in reader.iter_messages(
                    start_time=start_time,
                    end_time=end_time,
                    log_time_order=True,
                ):
                    if schema is None or schema.name != MESSAGE_ENVELOPE_SCHEMA_NAME:
                        raise ValueError("MCAP channel does not use the canonical MessageEnvelope schema")
                    envelope = envelope_pb2.MessageEnvelope.FromString(message.data)
                    result = validate_envelope(envelope, channel.topic, serialized_size=len(message.data))
                    if not result.valid:
                        detail = "; ".join(f"{item.code}: {item.detail}" for item in result.errors)
                        raise ValueError(f"invalid recorded MessageEnvelope: {detail}")
                    if message.publish_time != envelope.publish_time_unix_ns:
                        raise ValueError("MCAP publish_time does not match the recorded MessageEnvelope")
                    if message.sequence != envelope.sequence_number:
                        raise ValueError("MCAP sequence does not match the recorded MessageEnvelope")
                    yield ReplayRecord(channel.topic, envelope, message.data, message.log_time, message.publish_time)
            except Exception as error:
                raise ValueError(f"cannot replay indexed MCAP {self.path}: {error}") from error


class ReplayScheduler:
    def __init__(
        self,
        rate: ReplayRate,
        shutdown: ShutdownToken,
        *,
        monotonic_ns: Callable[[], int] = time.monotonic_ns,
        wait: Callable[[float], bool] | None = None,
    ) -> None:
        self.rate = rate
        self.shutdown = shutdown
        self._monotonic_ns = monotonic_ns
        self._wait = wait or shutdown.wait

    def deadlines(self, log_times: Sequence[int], replay_start_ns: int) -> tuple[int, ...]:
        if not log_times:
            return ()
        multiplier = self.rate.multiplier
        if multiplier is None:
            return tuple(replay_start_ns for _ in log_times)
        first = log_times[0]
        return tuple(replay_start_ns + int((value - first) / multiplier) for value in log_times)

    def wait_until(self, deadline_ns: int) -> bool:
        while not self.shutdown.is_requested:
            remaining_ns = deadline_ns - self._monotonic_ns()
            if remaining_ns <= 0:
                return True
            if self._wait(min(remaining_ns / 1_000_000_000, 0.250)):
                return False
        return False


def _endpoint_identity(endpoint: str) -> tuple[str, str, int | None]:
    try:
        parsed = urlsplit(endpoint)
        if parsed.scheme.lower() != "tcp" or parsed.hostname is None or parsed.port is None:
            return "raw", endpoint, None
        host = parsed.hostname.lower()
        if host == "localhost":
            host = "<loopback>"
        try:
            address = ip_address(host)
            mapped = getattr(address, "ipv4_mapped", None)
            if mapped is not None:
                address = mapped
            host = "<loopback>" if address.is_loopback else address.compressed
        except ValueError:
            pass
        return "tcp", host, parsed.port
    except ValueError:
        return "raw", endpoint, None


def validate_replay_endpoint(endpoint: str, live_endpoints: frozenset[str], allow_live_broker: bool) -> None:
    candidate = _endpoint_identity(endpoint)
    conflicts = False
    for live_endpoint in live_endpoints:
        live = _endpoint_identity(live_endpoint)
        if candidate == live:
            conflicts = True
            break
        if candidate[0] == "tcp" and live[0] == "tcp" and candidate[2] == live[2] and live[1] in {"*", "0.0.0.0", "::"}:
            conflicts = True
            break
    if conflicts and not allow_live_broker:
        raise ValueError("refusing to publish replay traffic to a live mission broker without --allow-live-broker")


class StructuredReplayer:
    def __init__(
        self,
        source: IndexedMcapSource,
        *,
        endpoint: str = DEFAULT_REPLAY_PUBLISHER_ENDPOINT,
        rate: ReplayRate = ReplayRate.NORMAL,
        live_endpoints: frozenset[str] = frozenset(),
        allow_live_broker: bool = False,
        shutdown: ShutdownToken | None = None,
        context: zmq.Context | None = None,
        monotonic_ns: Callable[[], int] = time.monotonic_ns,
        startup_delay_seconds: float = 0.250,
    ) -> None:
        validate_replay_endpoint(endpoint, live_endpoints, allow_live_broker)
        self.source = source
        self.endpoint = endpoint
        self.rate = rate
        self.shutdown = shutdown or ShutdownToken()
        self._context = context
        self._monotonic_ns = monotonic_ns
        self.startup_delay_seconds = startup_delay_seconds

    def run(self, *, start_time: int | None = None, end_time: int | None = None) -> int:
        own_context = self._context is None
        context = self._context or zmq.Context()
        socket: zmq.Socket[bytes] | None = None
        count = 0
        try:
            socket = context.socket(zmq.PUB)
            configure_result_publisher(socket)
            socket.setsockopt(zmq.SNDTIMEO, 250)
            socket.connect(self.endpoint)
            if self.shutdown.wait(self.startup_delay_seconds):
                return 0
            first_log_time: int | None = None
            replay_start = self._monotonic_ns()
            multiplier = self.rate.multiplier
            scheduler = ReplayScheduler(self.rate, self.shutdown, monotonic_ns=self._monotonic_ns)
            for record in self.source.records(start_time=start_time, end_time=end_time):
                if self.shutdown.is_requested:
                    break
                if first_log_time is None:
                    first_log_time = record.log_time
                if multiplier is not None:
                    deadline = replay_start + int((record.log_time - first_log_time) / multiplier)
                    if not scheduler.wait_until(deadline):
                        break
                while not self.shutdown.is_requested:
                    try:
                        socket.send_multipart([record.topic.encode("utf-8"), record.serialized_envelope])
                        break
                    except zmq.Again:
                        continue
                if self.shutdown.is_requested:
                    break
                count += 1
            return count
        finally:
            if socket is not None:
                socket.close(linger=0)
            if own_context:
                context.term()


__all__ = [
    "DEFAULT_REPLAY_PUBLISHER_ENDPOINT",
    "DEFAULT_REPLAY_SUBSCRIBER_ENDPOINT",
    "IndexedMcapSource",
    "ReplayRate",
    "ReplayRecord",
    "ReplayScheduler",
    "StructuredReplayer",
    "validate_replay_endpoint",
]
