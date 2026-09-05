"""Phase 7 cache, correlation, subscriber, fan-out, and lifecycle tests."""

from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pytest
import zmq
from purdue_rov.cv.v1 import diagnostics_pb2, frame_index_pb2

from purdue_rov_cv.config.models import CameraAdapter, CameraConfig, CameraFormat, CameraPathKind
from purdue_rov_cv.runtime.envelope import EnvelopeBuilder
from purdue_rov_cv.runtime.metrics import RuntimeMetrics
from purdue_rov_cv.runtime.publisher import PublisherSequence
from purdue_rov_cv.runtime.shutdown import ShutdownToken
from purdue_rov_cv.runtime.state import ComponentState
from purdue_rov_cv.video import (
    CacheInsertResult,
    CorrelationQuality,
    DecodedVideoFrame,
    FrameCorrelator,
    FrameIndexCache,
    FrameIndexPublisher,
    FrameIndexSubscriber,
    GStreamerRtpReceiver,
    LocalVideoFanout,
    ReceiverCallbacks,
    VideoReceiverBackendError,
    VideoReceiverService,
    configure_frame_index_subscriber,
)
from purdue_rov_cv.video.mapping import RtpFrameIndexMapper


class _Clock:
    def __init__(self) -> None:
        self.ns = 10_000_000_000

    def monotonic(self) -> float:
        return self.ns / 1_000_000_000

    def monotonic_ns(self) -> int:
        return self.ns

    def advance_ns(self, value: int) -> None:
        self.ns += value

    def advance(self, value: float) -> None:
        self.advance_ns(int(value * 1_000_000_000))


def _index(
    camera_id: str = "front_camera",
    *,
    ssrc: int = 7,
    timestamp: int = 90_000,
    frame_number: int = 4,
    session: bytes | None = None,
) -> frame_index_pb2.FrameIndex:
    return frame_index_pb2.FrameIndex(
        camera_id=camera_id,
        camera_session_id=session or uuid4().bytes,
        frame_number=frame_number,
        capture_time_unix_ns=1_234,
        capture_monotonic_ns=567,
        rtp_timestamp=timestamp,
        rtp_ssrc=ssrc,
        rtp_payload_type=96,
    )


def _frame(*, ssrc: int = 7, timestamp: int = 90_000, received_ns: int = 10_000_000_000) -> DecodedVideoFrame:
    return DecodedVideoFrame(bytes(12), 2, 2, 6, "BGR", 55, ssrc, timestamp, received_ns)


def test_frame_index_cache_exact_duplicate_conflict_and_ssrc_namespace() -> None:
    clock = _Clock()
    cache = FrameIndexCache("front_camera", monotonic_ns=clock.monotonic_ns)
    original = _index(session=uuid4().bytes)
    assert cache.insert(original) is CacheInsertResult.INSERTED
    assert cache.insert(original) is CacheInsertResult.DUPLICATE
    assert cache.exact(7, 90_000).frame_number == 4
    assert cache.exact(8, 90_000) is None

    conflict = _index(session=original.camera_session_id, frame_number=5)
    assert cache.insert(conflict) is CacheInsertResult.CONFLICT
    assert cache.exact(7, 90_000) is None
    assert len(cache) == 1


def test_frame_index_cache_ttl_boundary_and_capacity_are_per_camera() -> None:
    clock = _Clock()
    first = FrameIndexCache("camera_a", capacity=3, ttl_ns=2_000_000_000, monotonic_ns=clock.monotonic_ns)
    second = FrameIndexCache("camera_b", capacity=3, ttl_ns=2_000_000_000, monotonic_ns=clock.monotonic_ns)
    entry = _index("camera_a", timestamp=1)
    first.insert(entry)
    clock.advance_ns(1_999_999_999)
    assert first.exact(7, 1) is not None
    clock.advance_ns(1)
    assert first.exact(7, 1) is None

    for timestamp in range(5):
        first.insert(_index("camera_a", timestamp=timestamp))
    second.insert(_index("camera_b", timestamp=99))
    assert len(first) == 3
    assert first.exact(7, 0) is None
    assert first.exact(7, 4) is not None
    assert second.exact(7, 99) is not None

    canonical = FrameIndexCache("camera_c", monotonic_ns=clock.monotonic_ns)
    for timestamp in range(513):
        canonical.insert(_index("camera_c", timestamp=timestamp))
    assert len(canonical) == 512
    assert canonical.exact(7, 0) is None
    assert canonical.exact(7, 512) is not None


def test_correlation_exact_delayed_unmatched_and_no_stale_reuse() -> None:
    clock = _Clock()
    metrics = RuntimeMetrics(monotonic=clock.monotonic)
    delivered = []
    cache = FrameIndexCache("front_camera", monotonic_ns=clock.monotonic_ns)
    correlator = FrameCorrelator(cache, delivered.append, metrics=metrics, monotonic_ns=clock.monotonic_ns)
    mapping = _index(timestamp=100)

    cache.insert(mapping)
    correlator.submit_frame(_frame(timestamp=100))
    correlator.submit_frame(_frame(timestamp=101))
    assert [item.correlation.quality for item in delivered] == [CorrelationQuality.EXACT]
    delayed = _index(timestamp=101, frame_number=5, session=mapping.camera_session_id)
    correlator.submit_index(delayed)
    assert [item.correlation.quality for item in delivered] == [CorrelationQuality.EXACT, CorrelationQuality.EXACT]

    correlator.submit_frame(_frame(timestamp=102))
    clock.advance_ns(100_000_000)
    assert correlator.expire() == 1
    assert delivered[-1].correlation.quality is CorrelationQuality.UNMATCHED
    assert delivered[-1].correlation.identity is None
    values = metrics.snapshot().values
    assert values["frame_index_hits"] == 2
    assert values["frame_index_misses"] == 1

    # Even the same RTP key cannot reuse an identity that was already
    # consumed by a decoded frame.
    correlator.submit_frame(_frame(timestamp=100))
    clock.advance_ns(100_000_000)
    correlator.expire()
    assert delivered[-1].correlation.quality is CorrelationQuality.UNMATCHED
    assert delivered[-1].correlation.identity is None


def test_approximate_debug_is_wrap_aware_and_never_counts_as_exact() -> None:
    clock = _Clock()
    metrics = RuntimeMetrics(monotonic=clock.monotonic)
    delivered = []
    cache = FrameIndexCache("front_camera", monotonic_ns=clock.monotonic_ns)
    cache.insert(_index(timestamp=0xFFFFFFF0))
    correlator = FrameCorrelator(
        cache,
        delivered.append,
        metrics=metrics,
        approximate_debug=True,
        wait_ns=0,
        monotonic_ns=clock.monotonic_ns,
    )
    correlator.submit_frame(_frame(timestamp=20))
    assert delivered[0].correlation.quality is CorrelationQuality.APPROXIMATE
    assert metrics.snapshot().values["frame_index_hits"] == 0
    assert metrics.snapshot().values["frame_index_misses"] == 0

    ordinary = []
    no_debug = FrameCorrelator(cache, ordinary.append, metrics=metrics, wait_ns=0, monotonic_ns=clock.monotonic_ns)
    no_debug.submit_frame(_frame(timestamp=20))
    assert ordinary[0].correlation.quality is CorrelationQuality.UNMATCHED


def test_pending_correlation_is_bounded_without_head_of_line_blocking() -> None:
    clock = _Clock()
    metrics = RuntimeMetrics(monotonic=clock.monotonic)
    delivered = []
    cache = FrameIndexCache("front_camera", monotonic_ns=clock.monotonic_ns)
    correlator = FrameCorrelator(
        cache,
        delivered.append,
        metrics=metrics,
        pending_capacity=2,
        monotonic_ns=clock.monotonic_ns,
    )
    correlator.submit_frame(_frame(timestamp=1))
    correlator.submit_frame(_frame(timestamp=2))
    correlator.submit_frame(_frame(timestamp=3))
    assert correlator.pending_count == 2
    assert delivered[0].frame.rtp_timestamp == 1
    correlator.submit_index(_index(timestamp=3))
    assert delivered[-1].frame.rtp_timestamp == 3
    assert delivered[-1].correlation.quality is CorrelationQuality.EXACT


def test_correlation_shutdown_cancels_pending_without_delivery() -> None:
    clock = _Clock()
    delivered = []
    correlator = FrameCorrelator(
        FrameIndexCache("front_camera", monotonic_ns=clock.monotonic_ns),
        delivered.append,
        metrics=RuntimeMetrics(monotonic=clock.monotonic),
        monotonic_ns=clock.monotonic_ns,
    )
    correlator.submit_frame(_frame(timestamp=1))
    assert correlator.pending_count == 1
    correlator.close()
    assert correlator.pending_count == 0
    clock.advance_ns(100_000_000)
    assert correlator.expire() == 0
    assert delivered == []


def test_local_fanout_slow_consumer_keeps_only_latest() -> None:
    fanout: LocalVideoFanout[int] = LocalVideoFanout()
    operator = fanout.subscribe("operator")
    debug = fanout.subscribe("debug")
    fanout.publish(1)
    fanout.publish(2)
    fanout.publish(3)
    assert operator.receive(0) == 3
    assert debug.receive(0) == 3
    operator.close()
    fanout.close()


def _built_frame_index(builder: EnvelopeBuilder, value: frame_index_pb2.FrameIndex) -> tuple[bytes, bytes]:
    return builder.build(
        topic=f"cv.frame_index.{value.camera_id}",
        payload_type="frame_index_v1",
        payload=value,
        task_id="camera",
        source_id=value.camera_id,
        camera_id=value.camera_id,
        camera_session_id=value.camera_session_id,
        frame_number=value.frame_number,
        capture_time_unix_ns=value.capture_time_unix_ns,
    ).frames


def test_frame_index_subscriber_socket_settings_validation_and_sequence_gaps() -> None:
    context = zmq.Context()
    socket: zmq.Socket[bytes] = context.socket(zmq.SUB)
    try:
        configure_frame_index_subscriber(socket, "front_camera")
        assert socket.getsockopt(zmq.RCVHWM) == 5
        assert socket.getsockopt(zmq.RCVTIMEO) == 250
        assert socket.getsockopt(zmq.LINGER) == 0
        assert socket.getsockopt(zmq.RECONNECT_IVL) == 250
        assert socket.getsockopt(zmq.RECONNECT_IVL_MAX) == 2_000
        assert socket.getsockopt(zmq.TCP_KEEPALIVE) == 1
        assert socket.getsockopt(zmq.TCP_KEEPALIVE_IDLE) == 5
        assert socket.getsockopt(zmq.TCP_KEEPALIVE_INTVL) == 1
        assert socket.getsockopt(zmq.TCP_KEEPALIVE_CNT) == 3
        assert socket.getsockopt(zmq.MAXMSGSIZE) == 4 * 1024 * 1024
        assert socket.getsockopt(zmq.CONFLATE) == 0
    finally:
        socket.close(linger=0)
        context.term()

    clock = _Clock()
    metrics = RuntimeMetrics(monotonic=clock.monotonic)
    cache = FrameIndexCache("front_camera", monotonic_ns=clock.monotonic_ns)
    correlator = FrameCorrelator(cache, lambda _frame: None, metrics=metrics, monotonic_ns=clock.monotonic_ns)
    subscriber = FrameIndexSubscriber(
        "inproc://unused",
        "front_camera",
        96,
        correlator,
        metrics=metrics,
        shutdown=ShutdownToken(),
    )
    builder = EnvelopeBuilder(PublisherSequence())
    assert not subscriber.process_frames([b"only-one-frame"])
    first = _index(timestamp=1)
    assert subscriber.process_frames(_built_frame_index(builder, first))
    _built_frame_index(builder, _index(timestamp=2))
    assert subscriber.process_frames(_built_frame_index(builder, _index(timestamp=3)))
    values = metrics.snapshot().values
    assert values["invalid_multipart_message"] == 1
    assert values["observed_sequence_gaps"] == 1

    wrong_pt = _index(timestamp=4)
    wrong_pt.rtp_payload_type = 97
    assert not subscriber.process_frames(_built_frame_index(builder, wrong_pt))

    wrong_builder = EnvelopeBuilder(PublisherSequence())
    wrong = wrong_builder.build(
        topic="cv.health.front_camera",
        payload_type="diagnostic_status_v1",
        payload=diagnostics_pb2.DiagnosticStatus(source_id="front_camera"),
        task_id="camera",
        source_id="front_camera",
    )
    assert not subscriber.process_frames(wrong.frames)

    prefix_collision = _index("front_camera_shadow", timestamp=5)
    assert not subscriber.process_frames(_built_frame_index(EnvelopeBuilder(PublisherSequence()), prefix_collision))
    assert cache.exact(prefix_collision.rtp_ssrc, prefix_collision.rtp_timestamp) is None
    assert metrics.snapshot().values["invalid_messages"] >= 2


def test_frame_index_publisher_counts_local_queue_eviction() -> None:
    metrics = RuntimeMetrics()
    publisher = FrameIndexPublisher(
        "inproc://unused",
        "front_camera",
        metrics=metrics,
        shutdown=ShutdownToken(),
    )
    for timestamp in range(6):
        publisher.publish(_index(timestamp=timestamp))
    assert metrics.snapshot().values["zmq_send_dropped"] == 1


def test_sender_mapping_is_bounded_and_deduplicates_fragmented_rtp() -> None:
    session = uuid4().bytes
    mapper = RtpFrameIndexMapper(
        "front_camera",
        session,
        time_ns=lambda: 123,
        monotonic_ns=lambda: 456,
    )
    for frame_number in range(300):
        mapper.observe_source(frame_number)
        assert mapper.observe_encoder_input(frame_number)
        assert mapper.observe_encoded_output(frame_number + 1_000)
        value = mapper.frame_index_for_packet(frame_number + 1_000, 7, frame_number, 96)
        assert value is not None and value.frame_number == frame_number
        assert mapper.frame_index_for_packet(frame_number + 1_000, 7, frame_number, 96) is None
    assert mapper.encoded_mapping_count == 256


class _Backend:
    def __init__(self, callbacks: ReceiverCallbacks) -> None:
        self.callbacks = callbacks
        self.started = False
        self.stopped = False

    @property
    def running(self) -> bool:
        return self.started and not self.stopped

    def start(self) -> None:
        self.started = True

    def check_bus(self) -> None:
        return

    def stop(self) -> None:
        self.stopped = True


def _camera() -> CameraConfig:
    return CameraConfig(
        adapter=CameraAdapter.V4L2,
        device_path=Path("/dev/simulated"),
        device_path_kind=CameraPathKind.FALLBACK,
        format=CameraFormat.H264,
        width=2,
        height=2,
        frame_rate=30,
        stream_index=2,
        stream_to_surface=True,
        cv_enabled=True,
        allow_software_encode=False,
        slot_capacity_bytes=12,
    )


def test_receiver_timeout_rebuild_and_five_consecutive_frame_recovery() -> None:
    clock = _Clock()
    backends: list[_Backend] = []

    def factory(callbacks: ReceiverCallbacks) -> _Backend:
        backend = _Backend(callbacks)
        backends.append(backend)
        return backend

    service = VideoReceiverService(
        "front_camera",
        _camera(),
        "tcp://127.0.0.1:65431",
        "tcp://127.0.0.1:65432",
        health_interval_ms=1_000,
        backend_factory=factory,
        metrics=RuntimeMetrics(monotonic=clock.monotonic),
        monotonic=clock.monotonic,
        monotonic_ns=clock.monotonic_ns,
    )
    try:
        service.initialize()
        assert service.state_machine.state is ComponentState.READY
        backends[0].callbacks.on_decoded(_frame(received_ns=clock.monotonic_ns()))
        assert service.state_machine.state is ComponentState.RUNNING
        clock.advance(2.0)
        service.cache.insert(_index(timestamp=123))
        service.correlator.submit_frame(_frame(timestamp=124, received_ns=clock.monotonic_ns()))
        assert service.correlator.pending_count == 2
        service.step()
        assert service.state_machine.state is ComponentState.DEGRADED
        assert service.stream_status == "STREAM LOST"
        assert backends[0].stopped
        assert len(service.cache) == 0
        assert service.correlator.pending_count == 0
        assert service.metrics.snapshot().values["stream_restarts"] == 0
        clock.advance(1.0)
        service.step()
        assert len(backends) == 2
        assert service.state_machine.state is ComponentState.DEGRADED
        assert service.metrics.snapshot().values["stream_restarts"] == 1

        backends[1].callbacks.on_packet(7, 90_000, clock.monotonic_ns())
        backends[1].callbacks.on_packet_lost(2)

        for _ in range(4):
            backends[1].callbacks.on_decoded(_frame(received_ns=clock.monotonic_ns()))
        backends[1].callbacks.on_invalid_decoded("injected")
        for _ in range(4):
            backends[1].callbacks.on_decoded(_frame(received_ns=clock.monotonic_ns()))
        assert service.state_machine.state is ComponentState.DEGRADED
        backends[1].callbacks.on_decoded(_frame(received_ns=clock.monotonic_ns()))
        assert service.state_machine.state is ComponentState.RUNNING
        clock.advance(0.025)
        service.step()
        values = service.metrics.snapshot().values
        assert values["rtp_packets_received"] == 1
        assert values["rtp_packets_lost"] == 2
        assert values["last_frame_age_ms"] == 25.0
        health = service.health.health()
        assert health.video.decoded_frames == values["decoded_frames"]
        assert health.video.stream_restarts == 1
        assert health.messaging.messages_received == values["messages_received"]
    finally:
        result = service.close()
    assert result.completed and not result.failures
    assert service.state_machine.state is ComponentState.STOPPED


def test_receiver_detects_unexpected_subscriber_exit() -> None:
    service = VideoReceiverService(
        "front_camera",
        _camera(),
        "inproc://unused-subscriber",
        "inproc://unused-publisher",
        health_interval_ms=1_000,
        backend_factory=lambda callbacks: _Backend(callbacks),
    )
    service.subscriber.run = lambda: None  # type: ignore[method-assign]
    try:
        service.initialize()
        assert service._subscriber_thread is not None
        service._subscriber_thread.join(1)
        with pytest.raises(VideoReceiverBackendError, match="FrameIndex subscriber exited unexpectedly"):
            service.step()
        assert service.state_machine.state is ComponentState.ERROR
        values = service.metrics.snapshot().values
        assert values["last_error_code"] == "INTERNAL_ERROR"
    finally:
        result = service.close()
    assert result.completed and not result.failures


def test_receiver_pipeline_contract_is_bounded_and_canonical() -> None:
    receiver = GStreamerRtpReceiver(
        5004,
        98,
        on_packet=lambda _ssrc, _timestamp, _now: None,
        on_packet_lost=lambda _count: None,
        on_decoded=lambda _frame: None,
        on_invalid_decoded=lambda _reason: None,
        on_encoded=lambda _unit: None,
    )
    description = receiver.pipeline_description()
    assert "port=5004" in description
    assert "buffer-size=4194304" in description
    assert "payload=(int)98,clock-rate=(int)90000" in description
    assert "rtpjitterbuffer" in description
    assert "latency=50 drop-on-latency=true do-lost=true" in description
    assert "rtph264depay" in description and "h264parse" in description and "tee" in description
    assert "max-size-buffers=1" in description and "leaky=downstream" in description
    assert "max-buffers=1 drop=true sync=false" in description
