from __future__ import annotations

import multiprocessing
import os
import signal
import socket
import time
from pathlib import Path

import gi
import psutil
import pytest
import zmq
from purdue_rov.cv.v1 import diagnostics_pb2

from purdue_rov_cv.messaging.broker import DataBrokerService
from purdue_rov_cv.messaging.client import ControlClient
from purdue_rov_cv.recording import GIB, DiskSpaceGuard, EncodedMatroskaRecorder, VideoSegmentPaths
from purdue_rov_cv.recording.service import RecorderService
from purdue_rov_cv.recording.video import VideoDiskSpaceLow
from purdue_rov_cv.replay import IndexedMcapSource, MatroskaVideoReplay, ReplayRate, StructuredReplayer
from purdue_rov_cv.runtime import EnvelopeBuilder, PublisherSequence, ReceivedMultipartValidator, RuntimeMetrics
from purdue_rov_cv.runtime.state import ComponentState
from purdue_rov_cv.video import GStreamerRtpSender, VideoReceiverService
from purdue_rov_cv.video.models import EncodedAccessUnit
from tests.integration.test_phase4_processes import _run_module as _run_control_module
from tests.integration.test_phase4_processes import _run_router as _run_control_router
from tests.integration.test_phase5_module_runner_processes import (
    _config,
    _FrameWriter,
    _request,
    _run_router,
    _run_runner,
    _wait_state,
)
from tests.integration.test_phase7_gstreamer import _camera as _phase7_camera
from tests.integration.test_phase7_gstreamer import _receiver_process

gi.require_version("Gst", "1.0")
from gi.repository import Gst  # noqa: E402


def _free_endpoint() -> str:
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    port = listener.getsockname()[1]
    listener.close()
    return f"tcp://127.0.0.1:{port}"


def _broker_process(publisher_endpoint: str, subscriber_endpoint: str) -> None:
    DataBrokerService(publisher_endpoint, subscriber_endpoint, install_signals=True).run()


def _recorder_process(root: Path, publisher_endpoint: str, subscriber_endpoint: str) -> None:
    RecorderService(
        recording_root=root,
        session_label="integration",
        subscriber_endpoint=subscriber_endpoint,
        publisher_endpoint=publisher_endpoint,
        device_id="surface",
        receive_hwm=100,
        max_message_bytes=4 * 1024 * 1024,
        disk_guard=DiskSpaceGuard(lambda _path: 11 * GIB),
        install_signals=True,
    ).run()


def _low_disk_recorder_process(
    root: Path,
    low_flag: Path,
    publisher_endpoint: str,
    subscriber_endpoint: str,
) -> None:
    def available(_path: Path) -> int:
        return GIB if low_flag.exists() else 11 * GIB

    RecorderService(
        recording_root=root,
        session_label="disk_event",
        subscriber_endpoint=subscriber_endpoint,
        publisher_endpoint=publisher_endpoint,
        device_id="surface",
        receive_hwm=5,
        max_message_bytes=4 * 1024 * 1024,
        health_interval_ms=500,
        disk_guard=DiskSpaceGuard(available),
        install_signals=True,
    ).run()


def _slow_writer_recorder_process(
    root: Path,
    subscriber_endpoint: str,
    result: multiprocessing.Queue,
    finish: multiprocessing.synchronize.Event,
) -> None:
    class SlowSink:
        def write(self, _record: object) -> None:
            time.sleep(0.0005)

        def flush(self) -> None:
            pass

        def finish(self) -> None:
            pass

    metrics = RuntimeMetrics()
    service = RecorderService(
        recording_root=root,
        session_label="slow_writer",
        subscriber_endpoint=subscriber_endpoint,
        publisher_endpoint="tcp://127.0.0.1:59999",
        device_id="surface",
        receive_hwm=10_000,
        max_message_bytes=4 * 1024 * 1024,
        metrics=metrics,
        disk_guard=DiskSpaceGuard(lambda _path: 11 * GIB),
        writer_factory=lambda _path, _chunk, _compression: SlowSink(),
    )
    service.initialize()
    result.put(("ready", 0, service.records.qsize(), service.state_machine.state.value))
    deadline = time.monotonic() + 10
    while metrics.snapshot().values["recorder_queue_overflow"] == 0 and time.monotonic() < deadline:
        time.sleep(0.01)
    values = metrics.snapshot().values
    result.put(
        (
            "result",
            int(values["recorder_queue_overflow"]),
            service.records.qsize(),
            service.state_machine.state.value,
        )
    )
    finish.wait(5)
    service.close()


def _stop(process: multiprocessing.Process) -> float:
    started = time.monotonic()
    os.kill(process.pid, signal.SIGTERM)
    process.join(5.0)
    elapsed = time.monotonic() - started
    assert not process.is_alive()
    assert process.exitcode == 0
    assert elapsed < 5.0
    return elapsed


def _recording_video_receiver_process(root: Path, status: multiprocessing.Queue) -> None:
    camera_id = "phase8_camera"
    camera = _phase7_camera(27)
    recorder = EncodedMatroskaRecorder(
        VideoSegmentPaths(root, "rtp_session", camera_id),
        segment_seconds=1,
        disk_guard=DiskSpaceGuard(lambda _path: 11 * GIB),
    )
    service = VideoReceiverService(
        camera_id,
        camera,
        "tcp://127.0.0.1:65491",
        "tcp://127.0.0.1:65492",
        health_interval_ms=500,
        encoded_recorder=recorder,
        install_signals=True,
    )
    service.initialize()
    status.put(("ready", service.allocation.rtp_port, service.allocation.rtp_payload_type))
    service.run()


def test_real_process_broker_recorder_mcap_and_replay_isolation(tmp_path: Path) -> None:
    process_context = multiprocessing.get_context("fork")
    live_pub, live_sub = _free_endpoint(), _free_endpoint()
    broker = process_context.Process(target=_broker_process, args=(live_pub, live_sub), name="phase8-live-broker")
    recorder = process_context.Process(
        target=_recorder_process,
        args=(tmp_path, live_pub, live_sub),
        name="phase8-recorder",
    )
    broker.start()
    time.sleep(0.2)
    recorder.start()
    structured_path = tmp_path / "integration" / "structured.mcap"
    deadline = time.monotonic() + 5.0
    while not structured_path.exists() and time.monotonic() < deadline:
        time.sleep(0.02)
    assert structured_path.exists()

    context = zmq.Context()
    publisher = context.socket(zmq.PUB)
    subscriber = context.socket(zmq.SUB)
    for value in (publisher, subscriber):
        value.setsockopt(zmq.LINGER, 0)
    subscriber.setsockopt(zmq.SUBSCRIBE, b"cv.health.integration_source")
    publisher.connect(live_pub)
    subscriber.connect(live_sub)
    validator = ReceivedMultipartValidator(RuntimeMetrics())
    builder = EnvelopeBuilder(PublisherSequence())
    received: list[bytes] | None = None
    try:
        time.sleep(0.5)
        deadline = time.monotonic() + 3.0
        while received is None and time.monotonic() < deadline:
            built = builder.build(
                topic="cv.health.integration_source",
                payload_type="diagnostic_status_v1",
                payload=diagnostics_pb2.DiagnosticStatus(
                    source_id="integration_source",
                    report_time_unix_ns=time.time_ns(),
                ),
                task_id="",
                source_id="integration_source",
            )
            publisher.send_multipart(list(built.frames))
            if subscriber.poll(100):
                received = subscriber.recv_multipart()
        assert received is not None
        assert validator.validate(received).valid
    finally:
        publisher.close(linger=0)
        subscriber.close(linger=0)
        context.term()
    time.sleep(0.3)
    assert _stop(recorder) < 5.0
    assert _stop(broker) < 5.0

    recorded = list(IndexedMcapSource(structured_path).records())
    expected = next(item for item in recorded if item.topic == "cv.health.integration_source")
    assert expected.serialized_envelope == received[1]

    replay_pub, replay_sub = _free_endpoint(), _free_endpoint()
    isolation_live_pub, isolation_live_sub = _free_endpoint(), _free_endpoint()
    replay_broker = process_context.Process(
        target=_broker_process,
        args=(replay_pub, replay_sub),
        name="phase8-replay-broker",
    )
    isolation_live_broker = process_context.Process(
        target=_broker_process,
        args=(isolation_live_pub, isolation_live_sub),
        name="phase8-isolation-live-broker",
    )
    replay_broker.start()
    isolation_live_broker.start()
    replay_context = zmq.Context()
    replay_listener = replay_context.socket(zmq.SUB)
    live_listener = replay_context.socket(zmq.SUB)
    replay_listener.setsockopt(zmq.LINGER, 0)
    live_listener.setsockopt(zmq.LINGER, 0)
    replay_listener.setsockopt(zmq.SUBSCRIBE, b"cv.health.integration_source")
    live_listener.setsockopt(zmq.SUBSCRIBE, b"cv.health.integration_source")
    replay_listener.connect(replay_sub)
    live_listener.connect(isolation_live_sub)
    try:
        time.sleep(0.2)
        sent = StructuredReplayer(
            IndexedMcapSource(structured_path),
            endpoint=replay_pub,
            rate=ReplayRate.MAXIMUM,
            live_endpoints=frozenset({isolation_live_pub, isolation_live_sub}),
            startup_delay_seconds=0.5,
        ).run()
        assert sent == len(recorded)
        assert replay_listener.poll(2_000)
        replayed = replay_listener.recv_multipart()
        assert replayed[0] == received[0]
        assert replayed[1] == received[1]
        assert not live_listener.poll(250)

        opted_in = StructuredReplayer(
            IndexedMcapSource(structured_path),
            endpoint=isolation_live_pub,
            rate=ReplayRate.MAXIMUM,
            live_endpoints=frozenset({isolation_live_pub, isolation_live_sub}),
            allow_live_broker=True,
            startup_delay_seconds=0.5,
        ).run()
        assert opted_in == len(recorded)
        assert live_listener.poll(2_000)
        assert live_listener.recv_multipart() == received
    finally:
        replay_listener.close(linger=0)
        live_listener.close(linger=0)
        replay_context.term()
        _stop(replay_broker)
        _stop(isolation_live_broker)


def _simulated_camera(path: Path, stop: multiprocessing.synchronize.Event) -> None:
    writer = _FrameWriter(path)
    try:
        while not stop.is_set():
            writer.publish()
            time.sleep(0.02)
    finally:
        writer.close()


def test_phase30_real_broker_router_camera_module_subscriber_recorder(tmp_path: Path) -> None:
    process_context = multiprocessing.get_context("fork")
    pub, sub, client_endpoint = _free_endpoint(), _free_endpoint(), _free_endpoint()
    module_endpoint = f"ipc://{tmp_path / 'phase8-control.sock'}"
    config = _config(pub, sub, client_endpoint, module_endpoint)
    identities = process_context.Queue()
    camera_stop = process_context.Event()
    broker = process_context.Process(target=_broker_process, args=(pub, sub), name="phase8-30-broker")
    router = process_context.Process(
        target=_run_router,
        args=(client_endpoint, module_endpoint, {"task_a"}),
        name="phase8-30-router",
    )
    camera = process_context.Process(
        target=_simulated_camera,
        args=(tmp_path / "task_a", camera_stop),
        name="phase8-30-simulated-camera",
    )
    module = process_context.Process(
        target=_run_runner,
        args=(config, "task_a", tmp_path, "task_a", identities),
        name="phase8-30-module",
    )
    recorder = process_context.Process(
        target=_recorder_process,
        args=(tmp_path, pub, sub),
        name="phase8-30-recorder",
    )
    crash_recorder = process_context.Process(
        target=_recorder_process,
        args=(tmp_path / "crash-peer", pub, sub),
        name="phase8-30-crash-recorder",
    )
    receiver_status = process_context.Queue()
    video_receiver = process_context.Process(
        target=_receiver_process,
        args=("phase8_isolation_camera", 24, receiver_status),
        name="phase8-30-video-receiver",
    )
    context = zmq.Context()
    subscriber = context.socket(zmq.SUB)
    subscriber.setsockopt(zmq.LINGER, 0)
    subscriber.setsockopt(zmq.SUBSCRIBE, b"cv.result.task_a.front_camera")
    subscriber.connect(sub)
    client = ControlClient(client_endpoint, acknowledgement_timeout_seconds=0.3)
    received: list[bytes] | None = None
    try:
        broker.start()
        time.sleep(0.2)
        router.start()
        recorder.start()
        crash_recorder.start()
        video_receiver.start()
        camera.start()
        deadline = time.monotonic() + 3.0
        while not (tmp_path / "task_a").exists() and time.monotonic() < deadline:
            time.sleep(0.02)
        module.start()
        identities.get(timeout=5.0)
        _wait_state(client, "task_a", ComponentState.READY)
        response = client.execute_command(_request("task_a", "start"))
        assert response.resulting_state == ComponentState.RUNNING
        assert subscriber.poll(5_000)
        received = subscriber.recv_multipart()
        assert ReceivedMultipartValidator(RuntimeMetrics()).validate(received).valid
        os.kill(crash_recorder.pid, signal.SIGKILL)
        crash_recorder.join(2.0)
        assert crash_recorder.exitcode == -signal.SIGKILL
        assert all(process.is_alive() for process in (broker, router, camera, module, recorder, video_receiver))
        stopped = client.execute_command(_request("task_a", "stop"))
        assert stopped.resulting_state == ComponentState.READY
    finally:
        client.close()
        subscriber.close(linger=0)
        context.term()
        camera_stop.set()
        camera.join(2.0)
        if crash_recorder.is_alive():
            os.kill(crash_recorder.pid, signal.SIGKILL)
            crash_recorder.join(2.0)
        for process in (video_receiver, module, recorder, router, broker):
            if process.pid is not None and process.is_alive():
                _stop(process)
    assert received is not None
    records = list(IndexedMcapSource(tmp_path / "integration" / "structured.mcap").records())
    assert any(item.serialized_envelope == received[1] for item in records)


def test_recorder_crash_isolated_and_runtime_disk_low_event_is_real(tmp_path: Path) -> None:
    process_context = multiprocessing.get_context("fork")
    pub, sub = _free_endpoint(), _free_endpoint()
    broker = process_context.Process(target=_broker_process, args=(pub, sub), name="phase8-isolation-broker")
    crash_recorder = process_context.Process(
        target=_recorder_process,
        args=(tmp_path / "crash", pub, sub),
        name="phase8-crash-recorder",
    )
    context = zmq.Context()
    publisher = context.socket(zmq.PUB)
    subscriber = context.socket(zmq.SUB)
    for value in (publisher, subscriber):
        value.setsockopt(zmq.LINGER, 0)
    subscriber.setsockopt(zmq.SUBSCRIBE, b"cv.health.isolation")
    publisher.connect(pub)
    subscriber.connect(sub)
    broker.start()
    crash_recorder.start()
    try:
        deadline = time.monotonic() + 4
        recording = tmp_path / "crash" / "integration" / "structured.mcap"
        while not recording.exists() and time.monotonic() < deadline:
            time.sleep(0.02)
        assert recording.exists()
        os.kill(crash_recorder.pid, signal.SIGKILL)
        crash_recorder.join(2)
        assert crash_recorder.exitcode == -signal.SIGKILL
        builder = EnvelopeBuilder(PublisherSequence())
        deadline = time.monotonic() + 3
        forwarded = None
        while forwarded is None and time.monotonic() < deadline:
            built = builder.build(
                topic="cv.health.isolation",
                payload_type="diagnostic_status_v1",
                payload=diagnostics_pb2.DiagnosticStatus(source_id="isolation", report_time_unix_ns=time.time_ns()),
                task_id="",
                source_id="isolation",
            )
            publisher.send_multipart(list(built.frames))
            if subscriber.poll(100):
                forwarded = subscriber.recv_multipart()
        assert forwarded is not None
    finally:
        publisher.close(linger=0)
        subscriber.close(linger=0)
        context.term()
        if crash_recorder.is_alive():
            os.kill(crash_recorder.pid, signal.SIGKILL)
            crash_recorder.join(2)
        _stop(broker)

    low_pub, low_sub = _free_endpoint(), _free_endpoint()
    low_broker = process_context.Process(
        target=_broker_process,
        args=(low_pub, low_sub),
        name="phase8-low-disk-broker",
    )
    flag = tmp_path / "disk-low"
    low_recorder = process_context.Process(
        target=_low_disk_recorder_process,
        args=(tmp_path / "low", flag, low_pub, low_sub),
        name="phase8-low-disk-recorder",
    )
    event_context = zmq.Context()
    event_listener = event_context.socket(zmq.SUB)
    event_listener.setsockopt(zmq.LINGER, 0)
    event_listener.setsockopt(zmq.SUBSCRIBE, b"system.event.disk_space_low")
    event_listener.setsockopt(zmq.SUBSCRIBE, b"cv.health.recorder")
    event_listener.connect(low_sub)
    low_broker.start()
    low_recorder.start()
    try:
        recording = tmp_path / "low" / "disk_event" / "structured.mcap"
        deadline = time.monotonic() + 4
        while not recording.exists() and time.monotonic() < deadline:
            time.sleep(0.02)
        assert recording.exists()
        # A file proves the writer initialized, but not that the PUB/SUB paths
        # have completed their asynchronous ZeroMQ subscription handshake.
        deadline = time.monotonic() + 4
        health_ready = False
        while not health_ready and time.monotonic() < deadline:
            if event_listener.poll(500):
                health_ready = event_listener.recv_multipart()[0] == b"cv.health.recorder"
        assert health_ready
        flag.touch()
        deadline = time.monotonic() + 4
        event_frames = None
        while event_frames is None and time.monotonic() < deadline:
            if event_listener.poll(500):
                candidate = event_listener.recv_multipart()
                if candidate[0] == b"system.event.disk_space_low":
                    event_frames = candidate
        assert event_frames is not None
        result = ReceivedMultipartValidator(RuntimeMetrics()).validate(event_frames)
        assert result.valid
        assert result.envelope is not None
        assert result.envelope.payload_type == "system_event_v1"
        low_recorder.join(5)
        assert low_recorder.exitcode == 0
    finally:
        event_listener.close(linger=0)
        event_context.term()
        if low_recorder.is_alive():
            _stop(low_recorder)
        _stop(low_broker)


def test_real_slow_writer_pressure_is_bounded_drop_newest(tmp_path: Path) -> None:
    process_context = multiprocessing.get_context("fork")
    publisher_endpoint, subscriber_endpoint = _free_endpoint(), _free_endpoint()
    control_client_endpoint, control_module_endpoint = _free_endpoint(), _free_endpoint()
    broker = process_context.Process(
        target=_broker_process,
        args=(publisher_endpoint, subscriber_endpoint),
        name="phase8-pressure-broker",
    )
    router_ready = process_context.Event()
    router = process_context.Process(
        target=_run_control_router,
        args=(control_client_endpoint, control_module_endpoint, 3.5, router_ready),
        name="phase8-pressure-control-router",
    )
    module = process_context.Process(
        target=_run_control_module,
        args=(control_module_endpoint,),
        name="phase8-pressure-control-module",
    )
    context = zmq.Context()
    publisher = context.socket(zmq.PUB)
    independent_subscriber = context.socket(zmq.SUB)
    publisher.setsockopt(zmq.LINGER, 0)
    publisher.setsockopt(zmq.SNDHWM, 20_000)
    independent_subscriber.setsockopt(zmq.LINGER, 0)
    independent_subscriber.setsockopt(zmq.RCVHWM, 20_000)
    independent_subscriber.setsockopt(zmq.SUBSCRIBE, b"cv.health.pressure")
    publisher.connect(publisher_endpoint)
    independent_subscriber.connect(subscriber_endpoint)
    result = process_context.Queue()
    finish = process_context.Event()
    recorder = process_context.Process(
        target=_slow_writer_recorder_process,
        args=(tmp_path, subscriber_endpoint, result, finish),
        name="phase8-slow-writer-recorder",
    )
    broker.start()
    router.start()
    assert router_ready.wait(3)
    module.start()
    recorder.start()
    client = ControlClient(control_client_endpoint, acknowledgement_timeout_seconds=0.3)
    try:
        assert result.get(timeout=3)[0] == "ready"
        _wait_state(client, "gate_detection", ComponentState.READY)
        time.sleep(0.3)
        builder = EnvelopeBuilder(PublisherSequence())
        recorder_process = psutil.Process(recorder.pid)
        rss_before = recorder_process.memory_info().rss
        file_descriptors_before = recorder_process.num_fds()
        send_started = time.monotonic()
        sent = 0
        for _ in range(12_000):
            built = builder.build(
                topic="cv.health.pressure",
                payload_type="diagnostic_status_v1",
                payload=diagnostics_pb2.DiagnosticStatus(source_id="pressure", report_time_unix_ns=time.time_ns()),
                task_id="",
                source_id="pressure",
            )
            try:
                publisher.send_multipart(list(built.frames), flags=zmq.DONTWAIT)
                sent += 1
            except zmq.Again:
                pass
        send_elapsed = time.monotonic() - send_started
        kind, overflows, queue_size, state = result.get(timeout=12)
        assert kind == "result"
        assert overflows > 0
        assert queue_size <= 4096
        assert state == ComponentState.DEGRADED
        assert sent > 4096
        assert send_elapsed < 5
        assert independent_subscriber.poll(2_000)
        assert independent_subscriber.recv_multipart()[0] == b"cv.health.pressure"
        assert recorder_process.memory_info().rss - rss_before < 64 * 1024 * 1024
        assert recorder_process.num_fds() - file_descriptors_before < 8
        started = client.execute_command(_request("gate_detection", "start"))
        assert started.resulting_state == ComponentState.RUNNING
        stopped = client.execute_command(_request("gate_detection", "stop"))
        assert stopped.resulting_state == ComponentState.READY
        finish.set()
        recorder.join(5)
        assert recorder.exitcode == 0
    finally:
        finish.set()
        client.close()
        publisher.close(linger=0)
        independent_subscriber.close(linger=0)
        context.term()
        if recorder.is_alive():
            _stop(recorder)
        for process in (module, router, broker):
            if process.is_alive():
                _stop(process)


def _encoded_units(count: int = 20) -> list[EncodedAccessUnit]:
    Gst.init(None)
    pipeline = Gst.parse_launch(
        f"videotestsrc num-buffers={count} ! video/x-raw,width=160,height=120,framerate=10/1 "
        "! videoconvert ! video/x-raw,format=I420 ! openh264enc gop-size=5 "
        "! h264parse config-interval=-1 ! video/x-h264,stream-format=byte-stream,alignment=au "
        "! appsink name=encoded emit-signals=false sync=false"
    )
    sink = pipeline.get_by_name("encoded")
    pipeline.set_state(Gst.State.PLAYING)
    units: list[EncodedAccessUnit] = []
    try:
        while len(units) < count:
            sample = sink.emit("try-pull-sample", 1_000_000_000)
            assert sample is not None
            buffer = sample.get_buffer()
            mapped, mapping = buffer.map(Gst.MapFlags.READ)
            assert mapped
            try:
                data = bytes(mapping.data)
            finally:
                buffer.unmap(mapping)
            units.append(
                EncodedAccessUnit(
                    data,
                    int(buffer.pts),
                    bool(buffer.has_flags(Gst.BufferFlags.DELTA_UNIT)),
                )
            )
    finally:
        pipeline.set_state(Gst.State.NULL)
    return units


def test_real_encoded_matroska_segmentation_and_video_replay(tmp_path: Path) -> None:
    units = _encoded_units()
    recorder = EncodedMatroskaRecorder(
        VideoSegmentPaths(tmp_path, "video_session", "front_camera"),
        segment_seconds=1,
        disk_guard=DiskSpaceGuard(lambda _path: 11 * GIB),
    )
    recorder.start()
    for unit in units:
        assert recorder.push(unit)
    recorder.stop()
    segments = sorted((tmp_path / "video_session" / "front_camera").glob("*.mkv"))
    assert len(segments) >= 2
    assert all(path.stat().st_size > 0 for path in segments)

    decoded = []
    replay = MatroskaVideoReplay(segments[0], decoded.append, rate=ReplayRate.MAXIMUM)
    frame_count = replay.run()
    assert frame_count == len(decoded) > 0
    assert all(frame.pixel_format == "BGR" for frame in decoded)

    durations = {}
    for rate in (ReplayRate.QUARTER, ReplayRate.HALF, ReplayRate.NORMAL, ReplayRate.DOUBLE):
        decoded_at_rate = []
        started = time.monotonic()
        count_at_rate = MatroskaVideoReplay(segments[0], decoded_at_rate.append, rate=rate).run()
        durations[rate] = time.monotonic() - started
        assert count_at_rate == len(decoded_at_rate) > 0
    assert durations[ReplayRate.QUARTER] > durations[ReplayRate.HALF]
    assert durations[ReplayRate.HALF] > durations[ReplayRate.NORMAL]
    assert durations[ReplayRate.NORMAL] > durations[ReplayRate.DOUBLE]

    free = [11 * GIB]
    disk_recorder = EncodedMatroskaRecorder(
        VideoSegmentPaths(tmp_path, "video_disk", "front_camera"),
        disk_guard=DiskSpaceGuard(lambda _path: free[0]),
        disk_check_interval_seconds=0,
    )
    disk_recorder.start()
    assert disk_recorder.push(units[0])
    free[0] = GIB
    with pytest.raises(VideoDiskSpaceLow):
        disk_recorder.check_bus()
    assert not disk_recorder.running
    disk_recorder.check_bus()


@pytest.mark.timeout(30)
def test_real_rtp_receiver_records_sigterm_finalizes_and_replays(tmp_path: Path) -> None:
    process_context = multiprocessing.get_context("spawn")
    status = process_context.Queue()
    receiver = process_context.Process(
        target=_recording_video_receiver_process,
        args=(tmp_path, status),
        name="phase8-recording-video-receiver",
    )
    receiver.start()
    sender = None
    try:
        kind, port, payload_type = status.get(timeout=8)
        assert kind == "ready"
        sender = GStreamerRtpSender(
            "phase8_camera",
            b"\x44" * 16,
            "127.0.0.1",
            port,
            payload_type,
            width=160,
            height=120,
            frame_rate=20,
            on_frame_index=lambda _value: None,
        )
        sender.start()
        deadline = time.monotonic() + 4.5
        while time.monotonic() < deadline:
            time.sleep(0.05)
        shutdown_seconds = _stop(receiver)
        assert shutdown_seconds < 5.0
    finally:
        if sender is not None:
            sender.stop()
        if receiver.is_alive():
            receiver.kill()
            receiver.join(2)

    segments = sorted((tmp_path / "rtp_session" / "phase8_camera").glob("*.mkv"))
    assert len(segments) >= 2
    decoded_counts = []
    for segment in segments:
        assert segment.stat().st_size > 0
        decoded_counts.append(MatroskaVideoReplay(segment, lambda _frame: None, rate=ReplayRate.MAXIMUM).run())
    assert all(count > 0 for count in decoded_counts)


def test_actual_filesystem_disk_probe_uses_recording_filesystem() -> None:
    root = Path(__file__).parents[2]
    snapshot = DiskSpaceGuard().inspect(root)
    assert snapshot.path == root
    assert snapshot.free_bytes > 0
