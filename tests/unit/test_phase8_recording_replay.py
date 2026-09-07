from __future__ import annotations

import time
import tomllib
from datetime import UTC, datetime
from pathlib import Path
from threading import Event, Thread
from uuid import UUID

import pytest
import zmq
from mcap.reader import SeekingReader, make_reader
from mcap.records import Chunk
from mcap.stream_reader import StreamReader
from mcap.writer import IndexType, Writer
from mcap_protobuf.schema import build_file_descriptor_set
from purdue_rov.cv.v1 import diagnostics_pb2, envelope_pb2

from purdue_rov_cv.config.loader import load_config
from purdue_rov_cv.recording import (
    FLUSH_INTERVAL_NS,
    GIB,
    DiskSpaceGuard,
    EncodedMatroskaRecorder,
    FlushPolicy,
    McapSessionWriter,
    RecorderSubscriber,
    StructuredRecord,
    StructuredWriterLoop,
    VideoSegmentPaths,
)
from purdue_rov_cv.recording.disk import RUNTIME_MINIMUM_BYTES, START_MINIMUM_BYTES
from purdue_rov_cv.recording.entrypoints import recorder_main
from purdue_rov_cv.recording.service import RecorderService
from purdue_rov_cv.replay import IndexedMcapSource, ReplayRate, ReplayScheduler, validate_replay_endpoint
from purdue_rov_cv.replay.entrypoints import replay_broker_main, replay_main, video_replay_main
from purdue_rov_cv.runtime import (
    ComponentState,
    ComponentStateMachine,
    EnvelopeBuilder,
    PublisherSequence,
    RecorderQueue,
    RuntimeMetrics,
    ShutdownToken,
)
from purdue_rov_cv.video.entrypoints import video_receiver_main


def _built(topic: str, sequence: int = 0, publish_time: int = 123):
    publisher = PublisherSequence(uuid_factory=lambda: UUID(int=25))
    for _ in range(sequence):
        publisher.next_attempt()
    return EnvelopeBuilder(
        publisher,
        unix_time_ns=lambda: publish_time,
        monotonic_ns=lambda: 45,
    ).build(
        topic=topic,
        payload_type="diagnostic_status_v1",
        payload=diagnostics_pb2.DiagnosticStatus(source_id=topic.rsplit(".", 1)[-1], report_time_unix_ns=123),
        task_id="",
        source_id=topic.rsplit(".", 1)[-1],
    )


def _record(topic: str, receive_time: int, sequence: int = 0) -> StructuredRecord:
    built = _built(topic, sequence, publish_time=receive_time - 10)
    return StructuredRecord(topic, built.envelope, built.serialized_envelope, receive_time)


@pytest.mark.parametrize(
    "main",
    [recorder_main, replay_main, replay_broker_main, video_replay_main, video_receiver_main],
)
def test_phase8_cli_entrypoints_expose_help(main, capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as raised:
        main(["--help"])
    assert raised.value.code == 0
    assert "usage:" in capsys.readouterr().out


def test_phase8_console_scripts_are_registered() -> None:
    project = tomllib.loads((Path(__file__).parents[2] / "pyproject.toml").read_text(encoding="utf-8"))["project"]
    scripts = project["scripts"]
    assert scripts["purdue-cv-recorder"] == "purdue_rov_cv.recording.entrypoints:recorder_entrypoint"
    assert scripts["purdue-cv-replay"] == "purdue_rov_cv.replay.entrypoints:replay_entrypoint"
    assert scripts["purdue-cv-replay-broker"] == "purdue_rov_cv.replay.entrypoints:replay_broker_entrypoint"
    assert scripts["purdue-cv-video-replay"] == "purdue_rov_cv.replay.entrypoints:video_replay_entrypoint"


def test_mcap_one_schema_per_topic_channels_and_fidelity(tmp_path: Path) -> None:
    path = tmp_path / "session.mcap"
    writer = McapSessionWriter(path)
    expected = [
        _record("cv.health.camera", 1_000, 4),
        _record("cv.health.module", 2_000, 8),
        _record("cv.health.camera", 3_000, 5),
    ]
    for item in expected:
        writer.write(item)
    assert len(writer.channels) == 2
    camera_channel_id = writer.channels["cv.health.camera"]
    module_channel_id = writer.channels["cv.health.module"]
    assert camera_channel_id != module_channel_id
    writer.finish()

    with path.open("rb") as stream:
        reader = make_reader(stream, validate_crcs=True)
        assert isinstance(reader, SeekingReader)
        summary = reader.get_summary()
        assert summary is not None
        assert len(summary.schemas) == 1
        schema = next(iter(summary.schemas.values()))
        assert schema.name == "purdue_rov.cv.v1.MessageEnvelope"
        assert schema.encoding == "protobuf"
        assert schema.data == build_file_descriptor_set(envelope_pb2.MessageEnvelope).SerializeToString()
        assert len(summary.channels) == 2
        assert summary.chunk_indexes
        messages = list(reader.iter_messages(log_time_order=True))
    assert [channel.topic for _, channel, _ in messages] == [item.topic for item in expected]
    assert [channel.id for _, channel, _ in messages] == [camera_channel_id, module_channel_id, camera_channel_id]
    for item, (_, _, message) in zip(expected, messages, strict=True):
        assert message.log_time == item.receive_time_unix_ns
        assert message.publish_time == item.envelope.publish_time_unix_ns
        assert message.sequence == item.envelope.sequence_number
        assert message.data == item.serialized_envelope

    chunks = [
        record
        for record in StreamReader(str(path), emit_chunks=True, validate_crcs=True).records
        if isinstance(record, Chunk)
    ]
    assert chunks and {chunk.compression for chunk in chunks} == {"zstd"}

    replayed = list(IndexedMcapSource(path).records(start_time=1_500, end_time=3_001))
    assert [item.topic for item in replayed] == ["cv.health.module", "cv.health.camera"]
    assert [item.envelope.sequence_number for item in replayed] == [8, 5]


def test_mcap_refuses_collision_wrong_chunk_and_compression(tmp_path: Path) -> None:
    path = tmp_path / "existing.mcap"
    path.touch()
    with pytest.raises(FileExistsError):
        McapSessionWriter(path)
    with pytest.raises(ValueError, match="1,048,576"):
        McapSessionWriter(tmp_path / "wrong.mcap", chunk_size_bytes=1)
    with pytest.raises(ValueError, match="zstd"):
        McapSessionWriter(tmp_path / "wrong2.mcap", compression="none")


def test_mcap_refuses_to_truncate_uint64_envelope_sequence(tmp_path: Path) -> None:
    path = tmp_path / "sequence-width.mcap"
    writer = McapSessionWriter(path)
    original = _record("cv.health.camera", 1_000)
    original.envelope.sequence_number = 1 << 32
    record = StructuredRecord(
        original.topic,
        original.envelope,
        original.envelope.SerializeToString(deterministic=True),
        original.receive_time_unix_ns,
    )

    with pytest.raises(ValueError, match="refuses to truncate or rewrite"):
        writer.write(record)
    writer.finish()


def test_phase8_recording_configuration_is_canonical() -> None:
    fixture = Path(__file__).parents[1] / "fixtures" / "config" / "valid" / "single_camera.yaml"
    config = load_config(fixture)
    assert config.recording.video_segment_seconds == 300
    assert config.recording.minimum_free_space_gib == 10
    assert config.recording.structured.chunk_size_bytes == 1_048_576
    assert config.recording.structured.compression == "zstd"


@pytest.mark.parametrize(
    ("free", "start_allowed", "runtime_low"),
    [
        (11 * GIB, True, False),
        (START_MINIMUM_BYTES, True, False),
        (START_MINIMUM_BYTES - 1, False, False),
        (RUNTIME_MINIMUM_BYTES, False, False),
        (RUNTIME_MINIMUM_BYTES - 1, False, True),
    ],
)
def test_disk_boundaries(tmp_path: Path, free: int, start_allowed: bool, runtime_low: bool) -> None:
    guard = DiskSpaceGuard(lambda _path: free)
    if start_allowed:
        assert guard.allows_start(tmp_path).free_bytes == free
    else:
        with pytest.raises(OSError):
            guard.allows_start(tmp_path)
    assert (guard.runtime_is_low(tmp_path).free_bytes < RUNTIME_MINIMUM_BYTES) is runtime_low


def test_flush_policy_count_time_and_simultaneous_boundary() -> None:
    policy = FlushPolicy()
    for index in range(99):
        assert not policy.on_message(index)
    assert policy.messages == 99
    assert not policy.time_due(FLUSH_INTERVAL_NS - 1)
    assert policy.time_due(FLUSH_INTERVAL_NS)
    assert policy.on_message(FLUSH_INTERVAL_NS)
    policy.reset()
    assert policy.messages == 0 and policy.deadline_ns is None
    for index in range(99):
        assert not policy.on_message(index)
    assert policy.on_message(99)


def test_writer_loop_flushes_one_idle_message_and_finalizes() -> None:
    flushed = Event()

    class Sink:
        def __init__(self) -> None:
            self.values: list[StructuredRecord] = []
            self.flushes = 0
            self.finished = False

        def write(self, record: StructuredRecord) -> None:
            self.values.append(record)

        def flush(self) -> None:
            self.flushes += 1
            flushed.set()

        def finish(self) -> None:
            self.finished = True

    metrics = RuntimeMetrics()
    queue = RecorderQueue[StructuredRecord](event=lambda _event: None, degrade=lambda _code: None, metrics=metrics)
    shutdown, accepting_done, sink = ShutdownToken(), Event(), Sink()
    loop = StructuredWriterLoop(queue, sink, shutdown, accepting_done=accepting_done)
    thread = Thread(target=loop.run)
    thread.start()
    started = time.monotonic()
    queue.offer(_record("cv.health.camera", 1_000))
    assert flushed.wait(0.600)
    assert time.monotonic() - started < 0.600
    assert sink.flushes == 1
    shutdown.request("test")
    accepting_done.set()
    thread.join(1.0)
    assert not thread.is_alive()
    assert sink.finished


def test_writer_loop_count_flush_and_finish_survives_flush_failure() -> None:
    reached_99 = Event()
    flushed = Event()

    class CountingSink:
        def __init__(self) -> None:
            self.writes = 0

        def write(self, _record: StructuredRecord) -> None:
            self.writes += 1
            if self.writes == 99:
                reached_99.set()

        def flush(self) -> None:
            flushed.set()

        def finish(self) -> None:
            pass

    records = RecorderQueue[StructuredRecord](event=lambda _event: None, degrade=lambda _code: None)
    shutdown, accepting_done, sink = ShutdownToken(), Event(), CountingSink()
    loop = StructuredWriterLoop(records, sink, shutdown, accepting_done=accepting_done)
    thread = Thread(target=loop.run)
    thread.start()
    for sequence in range(99):
        records.offer(_record("cv.health.camera", sequence + 1, sequence))
    assert reached_99.wait(1)
    assert not flushed.is_set()
    records.offer(_record("cv.health.camera", 100, 99))
    assert flushed.wait(1)
    shutdown.request("test")
    accepting_done.set()
    thread.join(1)
    assert sink.writes == 100

    class FlushFailingSink:
        finished = False

        def write(self, _record: StructuredRecord) -> None:
            pass

        def flush(self) -> None:
            raise OSError("injected flush failure")

        def finish(self) -> None:
            self.finished = True

    failing_records = RecorderQueue[StructuredRecord](event=lambda _event: None, degrade=lambda _code: None)
    failing_records.offer(_record("cv.health.camera", 1))
    failing_shutdown, failing_done = ShutdownToken(), Event()
    failing_shutdown.request("test")
    failing_done.set()
    failing_sink = FlushFailingSink()
    with pytest.raises(OSError, match="injected flush failure"):
        StructuredWriterLoop(failing_records, failing_sink, failing_shutdown, accepting_done=failing_done).run()
    assert failing_sink.finished


def test_recorder_joins_video_first_session_and_propagates_writer_failure(tmp_path: Path) -> None:
    (tmp_path / "shared_session" / "front_camera").mkdir(parents=True)
    service = RecorderService(
        recording_root=tmp_path,
        session_label="shared_session",
        subscriber_endpoint="inproc://phase8-shared-session-sub",
        publisher_endpoint="inproc://phase8-shared-session-pub",
        device_id="surface",
        receive_hwm=5,
        max_message_bytes=4 * 1024 * 1024,
        disk_guard=DiskSpaceGuard(lambda _path: 11 * GIB),
    )
    service.initialize()
    assert service.active
    assert service.close().completed
    assert list(IndexedMcapSource(service.structured_path).records()) == []

    finished = Event()

    class FailingSink:
        def write(self, _record: StructuredRecord) -> None:
            raise OSError("injected MCAP write failure")

        def flush(self) -> None:
            pass

        def finish(self) -> None:
            finished.set()

    failed = RecorderService(
        recording_root=tmp_path,
        session_label="writer_failure",
        subscriber_endpoint="inproc://phase8-writer-failure-sub",
        publisher_endpoint="inproc://phase8-writer-failure-pub",
        device_id="surface",
        receive_hwm=5,
        max_message_bytes=4 * 1024 * 1024,
        disk_guard=DiskSpaceGuard(lambda _path: 11 * GIB),
        writer_factory=lambda _path, _chunk, _compression: FailingSink(),
    )
    failed.records.offer(_record("cv.health.camera", 1))
    with pytest.raises(RuntimeError, match="worker failed"):
        failed.run()
    assert finished.is_set()
    assert failed.metrics.snapshot().values["last_error_code"] == "INTERNAL_ERROR"
    assert failed.state_machine.state is ComponentState.STOPPED


def test_runtime_low_disk_stops_service_without_automatic_restart(tmp_path: Path) -> None:
    free = [11 * GIB]
    service = RecorderService(
        recording_root=tmp_path,
        session_label="low_disk",
        subscriber_endpoint="inproc://phase8-low-sub",
        publisher_endpoint="inproc://phase8-low-pub",
        device_id="surface",
        receive_hwm=5,
        max_message_bytes=4 * 1024 * 1024,
        disk_guard=DiskSpaceGuard(lambda _path: free[0]),
    )
    service.initialize()
    assert service.active
    free[0] = RUNTIME_MINIMUM_BYTES
    assert service.check_disk()
    free[0] -= 1
    assert not service.check_disk()
    assert not service.active
    assert service.metrics.snapshot().values["last_error_code"] == "DISK_SPACE_LOW"
    result = service.close()
    assert result.completed and not result.timed_out
    assert service.state_machine.state is ComponentState.STOPPED
    assert list(IndexedMcapSource(service.structured_path).records()) == []


def test_replay_rates_absolute_deadlines_and_interruptible_wait() -> None:
    times = [0, 100_000_000, 300_000_000]
    token = ShutdownToken()
    expected = {
        ReplayRate.QUARTER: (10, 400_000_010, 1_200_000_010),
        ReplayRate.HALF: (10, 200_000_010, 600_000_010),
        ReplayRate.NORMAL: (10, 100_000_010, 300_000_010),
        ReplayRate.DOUBLE: (10, 50_000_010, 150_000_010),
        ReplayRate.MAXIMUM: (10, 10, 10),
    }
    for rate, deadlines in expected.items():
        assert ReplayScheduler(rate, token).deadlines(times, 10) == deadlines
    long_times = tuple(index * 10_000_000 for index in range(10_000))
    long_deadlines = ReplayScheduler(ReplayRate.QUARTER, token).deadlines(long_times, 987)
    assert long_deadlines[-1] == 987 + 4 * long_times[-1]
    token.request("test")
    assert not ReplayScheduler(ReplayRate.QUARTER, token).wait_until(10**18)
    with pytest.raises(ValueError):
        ReplayRate.parse("3")


def test_full_envelope_identity_round_trips_without_rebuild(tmp_path: Path) -> None:
    built = EnvelopeBuilder(
        PublisherSequence(uuid_factory=lambda: UUID(int=501)),
        unix_time_ns=lambda: 123_456_789,
        monotonic_ns=lambda: 987_654_321,
    ).build(
        topic="cv.health.front_camera",
        payload_type="diagnostic_status_v1",
        payload=diagnostics_pb2.DiagnosticStatus(source_id="front_camera", report_time_unix_ns=123_456_789),
        task_id="task_a",
        source_id="front_camera",
        camera_id="front_camera",
        camera_session_id=UUID(int=502).bytes,
        frame_number=77,
        capture_time_unix_ns=123_450_000,
    )
    path = tmp_path / "identity.mcap"
    writer = McapSessionWriter(path)
    writer.write(StructuredRecord("cv.health.front_camera", built.envelope, built.serialized_envelope, 123_456_999))
    writer.finish()
    [replayed] = list(IndexedMcapSource(path).records())
    assert replayed.topic == "cv.health.front_camera"
    assert replayed.serialized_envelope == built.serialized_envelope
    assert replayed.envelope == built.envelope


def test_live_endpoint_guard_fails_closed() -> None:
    live = frozenset({"tcp://192.168.50.2:5555", "tcp://192.168.50.2:5556"})
    with pytest.raises(ValueError, match="allow-live-broker"):
        validate_replay_endpoint("tcp://192.168.50.2:5555", live, False)
    validate_replay_endpoint("tcp://192.168.50.2:5555", live, True)
    validate_replay_endpoint("tcp://127.0.0.1:5655", live, False)
    with pytest.raises(ValueError, match="allow-live-broker"):
        validate_replay_endpoint("tcp://localhost:5555", frozenset({"tcp://127.0.0.1:5555"}), False)
    with pytest.raises(ValueError, match="allow-live-broker"):
        validate_replay_endpoint("tcp://[::1]:5555", frozenset({"tcp://127.0.0.1:5555"}), False)
    with pytest.raises(ValueError, match="allow-live-broker"):
        validate_replay_endpoint("tcp://[::ffff:127.0.0.1]:5555", frozenset({"tcp://localhost:5555"}), False)
    with pytest.raises(ValueError, match="allow-live-broker"):
        validate_replay_endpoint("tcp://127.0.0.1:5555", frozenset({"tcp://*:5555"}), False)


def test_video_segment_paths_are_utc_safe_unique_and_per_camera(tmp_path: Path) -> None:
    instant = datetime(2026, 9, 5, 12, 34, 56, 123456, tzinfo=UTC)
    front = VideoSegmentPaths(tmp_path, "dive_one", "front_camera", utc_now=lambda: instant)
    first, second = front.next_path(), front.next_path()
    rear = VideoSegmentPaths(tmp_path, "dive_one", "rear_camera", utc_now=lambda: instant).next_path()
    assert first == tmp_path / "dive_one" / "front_camera" / "20260905T123456.123456Z.mkv"
    assert second.name == "20260905T123456.123456Z-001.mkv"
    assert rear.parent.name == "rear_camera"
    with pytest.raises(ValueError):
        VideoSegmentPaths(tmp_path, "../../escape", "front_camera")


def test_encoded_video_topology_is_before_decode_and_has_no_encoder(tmp_path: Path) -> None:
    recorder = EncodedMatroskaRecorder(VideoSegmentPaths(tmp_path, "dive", "front_camera"))
    description = recorder.pipeline_description()
    assert "h264parse" in description
    assert "splitmuxsink" in description
    assert "matroskamux" in description
    assert "max-size-time=300000000000" in description
    for forbidden in ("avdec_h264", "x264enc", "openh264enc", "vaapih264enc", "nvh264enc"):
        assert forbidden not in description


def test_production_subscriber_validates_and_hands_off_receive_time() -> None:
    context = zmq.Context()
    endpoint = "inproc://phase8-recorder-subscriber"
    publisher = context.socket(zmq.PUB)
    publisher.setsockopt(zmq.LINGER, 0)
    publisher.bind(endpoint)
    metrics = RuntimeMetrics()
    state = ComponentStateMachine(ComponentState.RUNNING)
    records: RecorderQueue[StructuredRecord] = RecorderQueue(
        event=lambda _event: None,
        degrade=lambda _code: state.transition_to(ComponentState.DEGRADED),
        metrics=metrics,
    )
    shutdown = ShutdownToken()
    subscriber = RecorderSubscriber(
        endpoint,
        records,
        metrics=metrics,
        shutdown=shutdown,
        context=context,
        receive_hwm=5,
        max_message_bytes=4 * 1024 * 1024,
        unix_time_ns=lambda: 999,
    )
    thread = Thread(target=subscriber.run)
    thread.start()
    try:
        assert subscriber.ready.wait(1)
        built = _built("cv.health.camera")
        deadline = time.monotonic() + 2
        while records.qsize() == 0 and time.monotonic() < deadline:
            publisher.send_multipart(list(built.frames))
            time.sleep(0.01)
        assert records.qsize() == 1
        item = records.get_nowait()
        assert item.topic == "cv.health.camera"
        assert item.serialized_envelope == built.serialized_envelope
        assert item.receive_time_unix_ns == 999
        publisher.send_multipart([b"cv.health.camera"])
        time.sleep(0.05)
        assert metrics.snapshot().values["invalid_messages"] >= 1
    finally:
        shutdown.request("test complete")
        thread.join(1)
        publisher.close(linger=0)
        context.term()
    assert not thread.is_alive()


def test_production_subscriber_full_queue_drops_newest_without_blocking() -> None:
    context = zmq.Context()
    endpoint = "inproc://phase8-recorder-overflow"
    publisher = context.socket(zmq.PUB)
    publisher.setsockopt(zmq.LINGER, 0)
    publisher.bind(endpoint)
    metrics = RuntimeMetrics()
    state = ComponentStateMachine(ComponentState.RUNNING)
    events = []
    records: RecorderQueue[StructuredRecord] = RecorderQueue(
        event=events.append,
        degrade=lambda _code: state.transition_to(ComponentState.DEGRADED),
        metrics=metrics,
    )
    retained = _record("cv.health.camera", 1)
    for _ in range(4096):
        assert records.offer(retained).accepted
    shutdown = ShutdownToken()
    subscriber = RecorderSubscriber(
        endpoint,
        records,
        metrics=metrics,
        shutdown=shutdown,
        context=context,
        receive_hwm=5,
        max_message_bytes=4 * 1024 * 1024,
    )
    thread = Thread(target=subscriber.run)
    thread.start()
    try:
        assert subscriber.ready.wait(1)
        incoming = _built("cv.health.module")
        deadline = time.monotonic() + 2
        while metrics.snapshot().values["recorder_queue_overflow"] == 0 and time.monotonic() < deadline:
            publisher.send_multipart(list(incoming.frames))
            time.sleep(0.01)
        assert records.qsize() == 4096
        assert records.get_nowait() == retained
        assert metrics.snapshot().values["recorder_queue_overflow"] >= 1
        assert state.state is ComponentState.DEGRADED
        assert events and events[0].level == "CRITICAL"
    finally:
        shutdown.request("test")
        thread.join(1)
        publisher.close(linger=0)
        context.term()


def test_indexed_source_rejects_missing_truncated_unindexed_and_invalid_envelope(tmp_path: Path) -> None:
    with pytest.raises(OSError, match="cannot open"):
        list(IndexedMcapSource(tmp_path / "missing.mcap").records())
    bad = tmp_path / "bad.mcap"
    bad.write_bytes(b"not mcap")
    with pytest.raises(ValueError, match="MCAP"):
        list(IndexedMcapSource(bad).records())

    complete = tmp_path / "complete.mcap"
    complete_writer = McapSessionWriter(complete)
    complete_writer.write(_record("cv.health.camera", 1_000))
    complete_writer.finish()
    truncated = tmp_path / "truncated.mcap"
    truncated.write_bytes(complete.read_bytes()[:-16])
    with pytest.raises(ValueError, match="indexed MCAP"):
        list(IndexedMcapSource(truncated).records())

    unindexed = tmp_path / "unindexed.mcap"
    record = _record("cv.health.camera", 1_000)
    with unindexed.open("wb") as stream:
        writer = Writer(stream, index_types=IndexType.NONE)
        writer.start(profile="protobuf", library="test")
        schema_id = writer.register_schema(
            name="purdue_rov.cv.v1.MessageEnvelope",
            encoding="protobuf",
            data=build_file_descriptor_set(envelope_pb2.MessageEnvelope).SerializeToString(),
        )
        channel_id = writer.register_channel(
            topic=record.topic,
            message_encoding="protobuf",
            schema_id=schema_id,
        )
        writer.add_message(
            channel_id=channel_id,
            log_time=record.receive_time_unix_ns,
            publish_time=record.envelope.publish_time_unix_ns,
            sequence=record.envelope.sequence_number,
            data=record.serialized_envelope,
        )
        writer.finish()
    with pytest.raises(ValueError, match="chunk index"):
        list(IndexedMcapSource(unindexed).records())

    invalid_envelope = tmp_path / "invalid-envelope.mcap"
    invalid_writer = McapSessionWriter(invalid_envelope)
    invalid_writer.write(
        StructuredRecord(record.topic, record.envelope, b"not a protobuf envelope", record.receive_time_unix_ns)
    )
    invalid_writer.finish()
    with pytest.raises(ValueError, match="cannot replay indexed MCAP"):
        list(IndexedMcapSource(invalid_envelope).records())


def test_mcap_constructor_failure_closes_and_removes_partial_file(tmp_path: Path, monkeypatch) -> None:
    class FailingWriter:
        def __init__(self, *_args, **_kwargs) -> None:
            raise OSError("injected writer construction failure")

    monkeypatch.setattr("purdue_rov_cv.recording.mcap_writer.Writer", FailingWriter)
    path = tmp_path / "partial.mcap"
    with pytest.raises(OSError, match="injected writer construction failure"):
        McapSessionWriter(path)
    assert not path.exists()


def test_video_stop_reports_eos_timeout_and_releases_pipeline(tmp_path: Path) -> None:
    class FakeGst:
        class FlowReturn:
            OK = "ok"

        class MessageType:
            EOS = 1
            ERROR = 2

        class State:
            NULL = "null"

        class StateChangeReturn:
            FAILURE = "failure"

    class Source:
        def emit(self, _name: str) -> str:
            return FakeGst.FlowReturn.OK

    class Bus:
        def timed_pop_filtered(self, _timeout: int, _types: int):
            return None

    class Pipeline:
        def get_bus(self) -> Bus:
            return Bus()

        def set_state(self, _state: str) -> str:
            return "success"

        def get_state(self, _timeout: int):
            return "success", FakeGst.State.NULL, None

    recorder = EncodedMatroskaRecorder(VideoSegmentPaths(tmp_path, "dive", "front_camera"))
    recorder._gst = FakeGst  # type: ignore[attr-defined]
    recorder._pipeline = Pipeline()  # type: ignore[attr-defined]
    recorder._source = Source()  # type: ignore[attr-defined]
    recorder._pushed_units = 1  # type: ignore[attr-defined]
    with pytest.raises(RuntimeError, match="timed out waiting for Matroska EOS"):
        recorder.stop()
    assert not recorder.running

    empty = EncodedMatroskaRecorder(VideoSegmentPaths(tmp_path, "empty", "front_camera"))
    empty._gst = FakeGst  # type: ignore[attr-defined]
    empty._pipeline = Pipeline()  # type: ignore[attr-defined]
    empty._source = Source()  # type: ignore[attr-defined]
    empty.stop()
    assert not empty.running
