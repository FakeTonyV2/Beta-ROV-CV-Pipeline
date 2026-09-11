"""Process boundaries used by the Phase 9 launch fixture."""

from __future__ import annotations

import argparse
import signal
from pathlib import Path
from threading import Event

import zmq
from purdue_rov.cv.v1 import bounding_box_pb2

from purdue_rov_cv.camera.entrypoints import camera_entrypoint
from purdue_rov_cv.config.loader import load_config
from purdue_rov_cv.messaging.entrypoints import broker_entrypoint
from purdue_rov_cv.messaging.router import ControlRouterService
from purdue_rov_cv.module_runner.entrypoints import module_runner_entrypoint
from purdue_rov_cv.recording.disk import GIB, DiskSpaceGuard
from purdue_rov_cv.recording.service import RecorderService
from purdue_rov_cv.runtime.envelope import ReceivedMultipartValidator
from purdue_rov_cv.runtime.json_logging import configure_json_logger
from purdue_rov_cv.runtime.metrics import RuntimeMetrics
from purdue_rov_cv.video.entrypoints import video_receiver_entrypoint


def _subscriber(endpoint: str, topic: str, marker: Path, delay_seconds: float) -> int:
    shutdown = Event()

    def stop(_signum: int, _frame: object) -> None:
        shutdown.set()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    context = zmq.Context()
    socket: zmq.Socket[bytes] = context.socket(zmq.SUB)
    socket.setsockopt(zmq.LINGER, 0)
    socket.setsockopt(zmq.RCVHWM, 5)
    socket.setsockopt(zmq.SUBSCRIBE, topic.encode("utf-8"))
    socket.connect(endpoint)
    validator = ReceivedMultipartValidator(RuntimeMetrics())
    try:
        while not shutdown.is_set():
            if not socket.poll(100, zmq.POLLIN):
                continue
            frames = socket.recv_multipart()
            result = validator.validate(frames)
            if not result.valid or result.envelope is None:
                continue
            payload = bounding_box_pb2.BoundingBoxResult.FromString(result.envelope.payload)
            temporary = marker.with_suffix(f"{marker.suffix}.tmp")
            temporary.write_text(
                (
                    f"frame_number={payload.frame_number}\n"
                    f"camera_id={payload.camera_id}\n"
                    f"detections={len(payload.detections)}\n"
                    f"payload_type={result.envelope.payload_type}\n"
                    f"capture_time_unix_ns={result.envelope.capture_time_unix_ns}\n"
                    f"publish_time_unix_ns={result.envelope.publish_time_unix_ns}\n"
                    f"source_monotonic_ns={result.envelope.source_monotonic_ns}\n"
                ),
                encoding="utf-8",
            )
            temporary.replace(marker)
            if delay_seconds:
                shutdown.wait(delay_seconds)
    finally:
        socket.close(linger=0)
        context.term()
    return 0


def _simulated_recorder(config_path: Path, session: str) -> int:
    """Run the real recorder with only its external disk probe simulated."""
    config = load_config(config_path, environ={})
    service = RecorderService.from_config(
        config,
        session,
        disk_guard=DiskSpaceGuard(lambda _path: 20 * GIB),
        install_signals=True,
    )
    service.run()
    return 0


class _SimulatedStartAuthorizer:
    """Explicit authorization boundary for the Phase 9 simulation process."""

    def authorize(self, *, startup_dependencies_satisfied: bool) -> tuple[bool, str]:
        if not startup_dependencies_satisfied:
            return False, "simulated startup dependencies are not satisfied"
        return True, "authorized by the explicit Phase 9 simulation boundary"


def _simulated_router(config_path: Path) -> int:
    """Run a real router while keeping production authorization fail-closed."""

    config = load_config(config_path, environ={})
    logger = configure_json_logger(
        device_id=config.device.device_id,
        process_name="phase9-simulated-control-router",
        source_id="control-router",
        publisher_session_id=None,
    )
    service = ControlRouterService.from_config(
        config,
        logger=logger,
        install_signals=True,
        start_authorizer=_SimulatedStartAuthorizer(),
    )
    service.run()
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="phase9-process-role")
    commands = parser.add_subparsers(dest="role", required=True)
    for role in ("broker", "router", "camera", "module", "video-receiver", "recorder"):
        child = commands.add_parser(role)
        child.add_argument("--config", required=True)
    commands.choices["camera"].add_argument("--camera", required=True)
    commands.choices["camera"].add_argument("--disconnect-after-frames", type=int)
    commands.choices["module"].add_argument("--task", required=True)
    commands.choices["video-receiver"].add_argument("--camera", required=True)
    commands.choices["recorder"].add_argument("--session", required=True)
    subscriber = commands.add_parser("subscriber")
    subscriber.add_argument("--endpoint", required=True)
    subscriber.add_argument("--topic", required=True)
    subscriber.add_argument("--marker", type=Path, required=True)
    subscriber.add_argument("--delay-seconds", type=float, default=0.0)
    args = parser.parse_args(argv)
    if args.role == "broker":
        return broker_entrypoint(["--config", args.config])
    if args.role == "router":
        return _simulated_router(Path(args.config))
    if args.role == "camera":
        camera_args = ["--config", args.config, "--camera", args.camera, "--simulate-gstreamer"]
        if args.disconnect_after_frames is not None:
            camera_args.extend(["--disconnect-after-frames", str(args.disconnect_after_frames)])
        return camera_entrypoint(camera_args)
    if args.role == "module":
        return module_runner_entrypoint(["--config", args.config, "--task", args.task])
    if args.role == "video-receiver":
        return video_receiver_entrypoint(["--config", args.config, "--camera", args.camera])
    if args.role == "recorder":
        return _simulated_recorder(Path(args.config), args.session)
    return _subscriber(args.endpoint, args.topic, args.marker, args.delay_seconds)


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["main"]
