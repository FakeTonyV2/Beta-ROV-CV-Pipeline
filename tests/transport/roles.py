"""Separate-process roles used by the Phase 7.5 namespace acceptance suite."""

from __future__ import annotations

import argparse
import json
import os
import signal
import threading
import time
from collections.abc import Callable
from pathlib import Path
from uuid import uuid4

import numpy as np
import zmq
from purdue_rov.cv.v1 import bounding_box_pb2, control_pb2

from purdue_rov_cv.config.models import (
    CameraAdapter,
    CameraConfig,
    CameraFormat,
    CameraPathKind,
    CameraResolutionTier,
)
from purdue_rov_cv.messaging import broker as broker_module
from purdue_rov_cv.messaging import router as router_module
from purdue_rov_cv.messaging.broker import DataBrokerService
from purdue_rov_cv.messaging.client import ControlClient
from purdue_rov_cv.messaging.fake_module import FakeModuleService
from purdue_rov_cv.messaging.router import ControlRouterService
from purdue_rov_cv.messaging.sockets import TRANSPORT_MAX_MESSAGE_BYTES
from purdue_rov_cv.module_runner import publisher as result_publisher_module
from purdue_rov_cv.module_runner.publisher import PublicationItem, ResultPublisher
from purdue_rov_cv.modules.base import Frame
from purdue_rov_cv.runtime.envelope import ReceivedMultipartValidator
from purdue_rov_cv.runtime.metrics import RuntimeMetrics
from purdue_rov_cv.runtime.queues import CvResultQueue
from purdue_rov_cv.runtime.shutdown import ShutdownToken
from purdue_rov_cv.runtime.state import ComponentState, ComponentStateMachine
from purdue_rov_cv.video import sender as video_sender_module
from purdue_rov_cv.video import subscriber as video_subscriber_module
from purdue_rov_cv.video.sender import FrameIndexPublisher, GStreamerRtpSender
from purdue_rov_cv.video.service import VideoReceiverService
from purdue_rov_cv.wire.validators import validate_command_response


class EventWriter:
    def __init__(self, path: str) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    def write(self, event: str, **values: object) -> None:
        record = {
            "event": event,
            "pid": os.getpid(),
            "monotonic": time.monotonic(),
            "unix_ns": time.time_ns(),
            **values,
        }
        with self._lock, self.path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(record, sort_keys=True) + "\n")


def _camera(stream_index: int) -> CameraConfig:
    return CameraConfig(
        adapter=CameraAdapter.V4L2,
        device_path=Path("/dev/simulated"),
        device_path_kind=CameraPathKind.FALLBACK,
        resolution_tier=CameraResolutionTier.ID_PATH,
        stable_identity="test-simulated-camera",
        format=CameraFormat.H264,
        width=160,
        height=120,
        frame_rate=20,
        stream_index=stream_index,
        stream_to_surface=True,
        cv_enabled=True,
        allow_software_encode=False,
        slot_capacity_bytes=160 * 120 * 3,
    )


def run_broker(args: argparse.Namespace, writer: EventWriter) -> None:
    writer.write("ready", role="broker")
    original_xsub = broker_module.configure_xsub
    original_xpub = broker_module.configure_xpub

    def observe_socket(
        kind: str,
        configure: Callable[[zmq.Socket[bytes]], None],
        socket: zmq.Socket[bytes],
    ) -> None:
        configure(socket)
        writer.write(
            "socket_config",
            role="broker",
            socket_kind=kind,
            rcvhwm=socket.getsockopt(zmq.RCVHWM),
            sndhwm=socket.getsockopt(zmq.SNDHWM),
        )

    broker_module.configure_xsub = lambda socket: observe_socket("XSUB", original_xsub, socket)
    broker_module.configure_xpub = lambda socket: observe_socket("XPUB", original_xpub, socket)
    try:
        DataBrokerService(args.publisher_endpoint, args.subscriber_endpoint, install_signals=True).run()
    finally:
        broker_module.configure_xsub = original_xsub
        broker_module.configure_xpub = original_xpub


def run_router(args: argparse.Namespace, writer: EventWriter) -> None:
    writer.write("ready", role="router")
    original_configure = router_module.configure_router

    def observe_router_socket(socket: zmq.Socket[bytes], endpoint: str) -> None:
        original_configure(socket, endpoint)
        writer.write(
            "socket_config",
            role="router",
            socket_kind="ROUTER",
            endpoint=endpoint,
            rcvhwm=socket.getsockopt(zmq.RCVHWM),
            sndhwm=socket.getsockopt(zmq.SNDHWM),
        )

    router_module.configure_router = observe_router_socket

    class TestStartAuthorizer:
        def authorize(self, *, startup_dependencies_satisfied: bool) -> tuple[bool, str]:
            return startup_dependencies_satisfied, "transport test authorization"

    try:
        ControlRouterService(
            args.client_endpoint,
            args.module_endpoint,
            device_id="rov_pi5",
            allowed_module_ids={"gate_detection"},
            start_authorizer=TestStartAuthorizer(),
            install_signals=True,
        ).run()
    finally:
        router_module.configure_router = original_configure


def run_module(args: argparse.Namespace, writer: EventWriter) -> None:
    writer.write("ready", role="module")
    service = FakeModuleService(
        args.module_endpoint,
        module_id="gate_detection",
        task_id="gate_detection",
        host_device_id="rov_pi5",
        heartbeat_interval_seconds=0.25,
        registration_retry_seconds=0.25,
        registration_ack_timeout_seconds=0.2,
        install_signals=True,
    )
    exit_code = int(service.run())
    if exit_code:
        raise SystemExit(exit_code)


def run_publisher(args: argparse.Namespace, writer: EventWriter) -> None:
    shutdown = ShutdownToken()
    signal.signal(signal.SIGTERM, lambda _signum, _frame: shutdown.request("SIGTERM"))
    signal.signal(signal.SIGINT, lambda _signum, _frame: shutdown.request("SIGINT"))
    context = zmq.Context()
    metrics = RuntimeMetrics()
    queue = CvResultQueue[PublicationItem](metrics=metrics)
    state = ComponentStateMachine()
    state.transition_to(ComponentState.READY)
    state.transition_to(ComponentState.RUNNING)
    original_configure = result_publisher_module.configure_result_publisher

    def observe_publisher_socket(socket: zmq.Socket[bytes]) -> None:
        original_configure(socket)
        writer.write(
            "socket_config",
            role="publisher",
            socket_kind="PUB",
            sndhwm=socket.getsockopt(zmq.SNDHWM),
            send_timeout_ms=socket.getsockopt(zmq.SNDTIMEO),
        )

    result_publisher_module.configure_result_publisher = observe_publisher_socket
    publisher = ResultPublisher(
        args.publisher_endpoint,
        topic="cv.result.gate_detection.front_camera",
        payload_type="bounding_boxes_v1",
        task_id="gate_detection",
        module_id="gate_detection",
        device_id="rov_pi5",
        health_interval_ms=500,
        queue=queue,
        metrics=metrics,
        state_machine=state,
        shutdown=shutdown,
        context=context,
    )
    thread = threading.Thread(target=publisher.run, name="production-result-publisher")
    thread.start()
    if not publisher.ready.wait(2):
        raise RuntimeError("production publisher did not become ready")
    camera_session = uuid4().bytes
    pixels = np.zeros((2, 2, 3), dtype=np.uint8)
    period = 1.0 / args.rate_hz
    next_publish = time.monotonic()
    next_sample = next_publish
    frame_number = 0
    maximum_queue = 0
    writer.write(
        "ready",
        role="publisher",
        publisher_session_id=publisher.publisher_session_id.hex(),
        rate_hz=args.rate_hz,
    )
    try:
        while not shutdown.is_requested:
            now = time.monotonic()
            if now < next_publish:
                time.sleep(min(next_publish - now, 0.005))
                continue
            captured_unix = time.time_ns()
            frame = Frame(pixels, "front_camera", camera_session, frame_number, captured_unix, time.monotonic_ns())
            payload = bounding_box_pb2.BoundingBoxResult(
                camera_id="front_camera",
                camera_session_id=camera_session,
                frame_number=frame_number,
                capture_time_unix_ns=captured_unix,
                detections=[
                    bounding_box_pb2.Detection(
                        class_id=0,
                        # A valid, near-contract-limit payload makes the
                        # 100-Mbit rule and HWM pressure observable without
                        # changing production queue or socket capacities.
                        class_name="x" * args.payload_bytes,
                        confidence=0.5,
                        x=0.0,
                        y=0.0,
                        width=1.0,
                        height=1.0,
                    )
                ],
            )
            queue.offer(PublicationItem(payload, frame))
            maximum_queue = max(maximum_queue, queue.qsize())
            frame_number += 1
            next_publish += period
            if next_publish < now - period:
                next_publish = now + period
            if now >= next_sample:
                writer.write(
                    "publisher_sample",
                    frame_number=frame_number,
                    queue_size=queue.qsize(),
                    queue_maximum=maximum_queue,
                    metrics=metrics.snapshot().values,
                )
                next_sample = now + 0.5
    finally:
        shutdown.request("publisher closing")
        thread.join(3)
        result_publisher_module.configure_result_publisher = original_configure
        context.term()
        writer.write(
            "closed",
            role="publisher",
            thread_alive=thread.is_alive(),
            queue_maximum=maximum_queue,
            metrics=metrics.snapshot().values,
        )


def run_subscriber(args: argparse.Namespace, writer: EventWriter) -> None:
    stopped = threading.Event()
    signal.signal(signal.SIGTERM, lambda _signum, _frame: stopped.set())
    signal.signal(signal.SIGINT, lambda _signum, _frame: stopped.set())
    context = zmq.Context()
    socket: zmq.Socket[bytes] = context.socket(zmq.SUB)
    socket.setsockopt(zmq.RCVHWM, 5)
    socket.setsockopt(zmq.LINGER, 0)
    socket.setsockopt(zmq.MAXMSGSIZE, TRANSPORT_MAX_MESSAGE_BYTES)
    socket.setsockopt(zmq.SUBSCRIBE, b"cv.result.gate_detection.front_camera")
    socket.connect(args.subscriber_endpoint)
    metrics = RuntimeMetrics()
    validator = ReceivedMultipartValidator(metrics)
    process_interval = 1.0 / args.consumer_rate_hz
    receive_interval = 1.0 / args.receive_rate_hz
    next_receive = time.monotonic()
    next_process = next_receive
    cycle_started = next_receive
    pending: dict[str, object] | None = None
    last_received_by_stream: dict[tuple[str, str], int] = {}
    writer.write(
        "socket_config",
        role="subscriber",
        socket_kind="SUB",
        rcvhwm=socket.getsockopt(zmq.RCVHWM),
        conflate=socket.getsockopt(zmq.CONFLATE),
    )
    writer.write(
        "ready",
        role="subscriber",
        rcvhwm=socket.getsockopt(zmq.RCVHWM),
        conflate=socket.getsockopt(zmq.CONFLATE),
        consumer_rate_hz=args.consumer_rate_hz,
        receive_rate_hz=args.receive_rate_hz,
    )
    try:
        while not stopped.is_set():
            now = time.monotonic()
            receive_paused = bool(args.receive_pause_seconds) and (
                (now - cycle_started) % 0.5 < args.receive_pause_seconds
            )
            if now >= next_receive and not receive_paused:
                next_receive = now + receive_interval
                drained = 0
                # Drain the transport pipe into exactly one replaceable
                # application slot. This prevents a second unbounded FIFO and
                # lets current data replace stale data after congestion.
                while drained < 256 and socket.poll(0, zmq.POLLIN):
                    frames = socket.recv_multipart()
                    drained += 1
                    result = validator.validate(frames)
                    if result.valid and result.envelope is not None:
                        envelope = result.envelope
                        session = envelope.publisher_session_id.hex()
                        stream_key = (session, envelope.source_id)
                        previous = last_received_by_stream.get(stream_key)
                        if previous is not None and envelope.sequence_number > previous + 1:
                            writer.write(
                                "sequence_gap",
                                publisher_session_id=session,
                                source_id=envelope.source_id,
                                previous_sequence_number=previous,
                                current_sequence_number=envelope.sequence_number,
                                missing_sequence_numbers=envelope.sequence_number - previous - 1,
                                observed_sequence_gaps=metrics.snapshot().values["observed_sequence_gaps"],
                            )
                        last_received_by_stream[stream_key] = envelope.sequence_number
                        pending = {
                            "publish_time_unix_ns": envelope.publish_time_unix_ns,
                            "sequence_number": envelope.sequence_number,
                            "publisher_session_id": session,
                        }
                    else:
                        writer.write("invalid", errors=[str(error) for error in result.errors])
            if now >= next_process:
                next_process = now + process_interval
                if pending is None:
                    stopped.wait(0.005)
                    continue
                processed_monotonic = time.monotonic()
                writer.write(
                    "result",
                    received_monotonic=processed_monotonic,
                    age_ms=(time.time_ns() - int(pending["publish_time_unix_ns"])) / 1_000_000.0,
                    sequence_number=pending["sequence_number"],
                    publisher_session_id=pending["publisher_session_id"],
                    observed_sequence_gaps=metrics.snapshot().values["observed_sequence_gaps"],
                )
                pending = None
            stopped.wait(0.002)
    finally:
        socket.close(linger=0)
        context.term()
        writer.write("closed", role="subscriber", metrics=metrics.snapshot().values)


def _get_status_request() -> control_pb2.CommandRequest:
    request = control_pb2.CommandRequest(
        command_id=uuid4().bytes,
        target_id="gate_detection",
        issued_time_unix_ns=time.time_ns(),
    )
    request.get_status.SetInParent()
    return request


def run_control(args: argparse.Namespace, writer: EventWriter) -> None:
    stopped = threading.Event()
    signal.signal(signal.SIGTERM, lambda _signum, _frame: stopped.set())
    signal.signal(signal.SIGINT, lambda _signum, _frame: stopped.set())
    client = ControlClient(args.client_endpoint, acknowledgement_timeout_seconds=0.5)
    writer.write("ready", role="control")
    try:
        while not stopped.is_set():
            request = _get_status_request()
            started = time.monotonic()
            response = client.send_command(request)
            ended = time.monotonic()
            validation = validate_command_response(response)
            correlated = response.command_id == request.command_id and response.target_id == request.target_id
            writer.write(
                "control",
                started_monotonic=started,
                ended_monotonic=ended,
                latency_ms=(ended - started) * 1000.0,
                structurally_valid=validation.valid,
                correlated=correlated,
                request_command_id=request.command_id.hex(),
                response_command_id=response.command_id.hex(),
                request_target_id=request.target_id,
                response_target_id=response.target_id,
                status=int(response.status),
                error_code=response.error_code,
            )
            stopped.wait(0.2)
    finally:
        client.close()
        writer.write("closed", role="control")


def run_video(args: argparse.Namespace, writer: EventWriter) -> None:
    original_configure = video_subscriber_module.configure_frame_index_subscriber

    def observe_subscriber_socket(socket: zmq.Socket[bytes], camera_id: str) -> None:
        original_configure(socket, camera_id)
        writer.write(
            "socket_config",
            role="video",
            socket_kind="FrameIndex SUB",
            rcvhwm=socket.getsockopt(zmq.RCVHWM),
            conflate=socket.getsockopt(zmq.CONFLATE),
        )

    video_subscriber_module.configure_frame_index_subscriber = observe_subscriber_socket
    service = VideoReceiverService(
        "front_camera",
        _camera(args.stream_index),
        args.subscriber_endpoint,
        args.publisher_endpoint,
        health_interval_ms=500,
        install_signals=True,
    )
    previous_state = None
    previous_backend = False
    previous_decoded = 0
    previous_hits = 0
    try:
        service.initialize()
        writer.write("ready", role="video", rtp_port=service.allocation.rtp_port)
        while not service.shutdown.token.is_requested:
            service.step()
            now_state = service.state_machine.state.value
            backend = service.backend is not None
            snapshot = service.metrics.snapshot().values
            decoded = int(snapshot["decoded_frames"])
            exact_hits = int(snapshot["frame_index_hits"])
            if now_state != previous_state:
                writer.write("video_state", state=now_state, decoded_frames=decoded)
                previous_state = now_state
            if backend != previous_backend:
                writer.write(
                    "video_backend",
                    present=backend,
                    stream_restarts=service.metrics.snapshot().values["stream_restarts"],
                )
                previous_backend = backend
            if decoded != previous_decoded:
                writer.write(
                    "video_frame",
                    decoded_frames=decoded,
                    state=now_state,
                    metrics=snapshot,
                )
                previous_decoded = decoded
            if exact_hits != previous_hits:
                writer.write(
                    "video_correlation",
                    frame_index_hits=exact_hits,
                    frame_index_misses=int(snapshot["frame_index_misses"]),
                )
                previous_hits = exact_hits
    finally:
        result = service.close()
        video_subscriber_module.configure_frame_index_subscriber = original_configure
        writer.write("closed", role="video", completed=result.completed, failures=len(result.failures))


def run_sender(args: argparse.Namespace, writer: EventWriter) -> None:
    stopped = threading.Event()
    signal.signal(signal.SIGTERM, lambda _signum, _frame: stopped.set())
    signal.signal(signal.SIGINT, lambda _signum, _frame: stopped.set())
    original_configure = video_sender_module.configure_result_publisher

    def observe_publisher_socket(socket: zmq.Socket[bytes]) -> None:
        original_configure(socket)
        writer.write(
            "socket_config",
            role="sender",
            socket_kind="FrameIndex PUB",
            sndhwm=socket.getsockopt(zmq.SNDHWM),
            send_timeout_ms=socket.getsockopt(zmq.SNDTIMEO),
        )

    video_sender_module.configure_result_publisher = observe_publisher_socket
    publisher_shutdown = ShutdownToken()
    publisher = FrameIndexPublisher(
        args.publisher_endpoint,
        "front_camera",
        metrics=RuntimeMetrics(),
        shutdown=publisher_shutdown,
    )
    publisher_thread = threading.Thread(target=publisher.run, name="production-frame-index-publisher")
    publisher_thread.start()
    if not publisher.ready.wait(2):
        raise RuntimeError("FrameIndex publisher did not become ready")
    sender = GStreamerRtpSender(
        "front_camera",
        uuid4().bytes,
        args.video_address,
        5000 + 2 * args.stream_index,
        96 + args.stream_index,
        width=160,
        height=120,
        frame_rate=20,
        mtu=300,
        on_frame_index=publisher.publish,
    )
    try:
        sender.start()
        writer.write("ready", role="sender")
        while not stopped.wait(0.5):
            writer.write("sender_sample", frame_index_metrics=publisher.metrics.snapshot().values)
    finally:
        sender.stop()
        publisher_shutdown.request("sender closing")
        publisher_thread.join(3)
        video_sender_module.configure_result_publisher = original_configure
        writer.write("closed", role="sender", publisher_thread_alive=publisher_thread.is_alive())


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser()
    result.add_argument(
        "role", choices=("broker", "router", "module", "publisher", "subscriber", "control", "video", "sender")
    )
    result.add_argument("--events", required=True)
    result.add_argument("--publisher-endpoint", default="")
    result.add_argument("--subscriber-endpoint", default="")
    result.add_argument("--client-endpoint", default="")
    result.add_argument("--module-endpoint", default="")
    result.add_argument("--video-address", default="")
    result.add_argument("--rate-hz", type=float, default=30.0)
    result.add_argument("--consumer-rate-hz", type=float, default=30.0)
    result.add_argument("--receive-rate-hz", type=float, default=30.0)
    result.add_argument("--receive-pause-seconds", type=float, default=0.0)
    result.add_argument("--payload-bytes", type=int, default=400_000)
    result.add_argument("--stream-index", type=int, default=27)
    return result


def main() -> None:
    args = parser().parse_args()
    writer = EventWriter(args.events)
    writer.write("process_started", role=args.role)
    try:
        globals()[f"run_{args.role}"](args, writer)
    except BaseException as error:
        writer.write("fatal", role=args.role, exception_type=type(error).__name__, message=str(error))
        raise


if __name__ == "__main__":
    main()
