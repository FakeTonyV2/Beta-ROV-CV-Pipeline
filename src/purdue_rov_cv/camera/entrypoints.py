"""Installed camera-service boundary and canonical exit translation."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Never
from uuid import uuid4

from purdue_rov_cv.config.issues import ConfigurationError
from purdue_rov_cv.config.loader import load_config
from purdue_rov_cv.config.models import CameraAdapter
from purdue_rov_cv.config.ports import derive_stream_allocation
from purdue_rov_cv.frame_buffer import LiveOwnerError, SharedMemoryInvalid, UnsafeStaleSegmentError
from purdue_rov_cv.runtime.exit_codes import ExitCode
from purdue_rov_cv.runtime.json_logging import configure_json_logger
from purdue_rov_cv.runtime.metrics import RuntimeMetrics
from purdue_rov_cv.runtime.shutdown import ShutdownToken
from purdue_rov_cv.video.mapping import RtpFrameIndexMapper
from purdue_rov_cv.video.sender import FrameIndexPublisher
from purdue_rov_cv.wire.errors import ErrorCode

from .backend import (
    CaptureBackend,
    DisconnectAfterFramesBackend,
    GStreamerCaptureBackend,
    SurfaceRtpStream,
    SyntheticCaptureBackend,
    V4L2CaptureBackend,
)
from .health import CameraHealthPublisher
from .service import CameraService
from .v4l2 import V4L2ConfigurationError, V4L2ModeUnsupported


class _CameraArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> Never:
        self.print_usage(sys.stderr)
        self.exit(ExitCode.INVALID_ARGUMENTS, f"{self.prog}: error: {message}\n")


def _parser() -> _CameraArgumentParser:
    parser = _CameraArgumentParser(prog="purdue-cv-camera")
    parser.add_argument("--camera", required=True, help="configured camera ID")
    parser.add_argument("--config", type=Path, help="mission YAML")
    parser.add_argument("--simulate", action="store_true", help="use the deterministic synthetic capture backend")
    parser.add_argument(
        "--simulate-gstreamer",
        action="store_true",
        help="use the production GStreamer/FrameIndex path with its deterministic video test source",
    )
    parser.add_argument(
        "--disconnect-after-frames",
        type=int,
        help="simulation-only camera disconnect hook; the service then exercises normal recovery",
    )
    return parser


def camera_main(argv: list[str] | None = None) -> ExitCode:
    args = _parser().parse_args(argv)
    config = load_config(args.config)
    if args.camera not in config.cameras:
        raise ValueError(f"unknown configured camera: {args.camera}")
    camera = config.cameras[args.camera]
    session = uuid4()
    logger = configure_json_logger(
        device_id=config.device.device_id,
        process_name="purdue-cv-camera",
        source_id=args.camera,
        publisher_session_id=session,
    )
    metrics = RuntimeMetrics()
    backend_factory: Callable[[], CaptureBackend]
    if args.simulate and args.simulate_gstreamer:
        raise ValueError("choose only one simulated camera backend")
    if args.disconnect_after_frames is not None and not (args.simulate or args.simulate_gstreamer):
        raise ValueError("--disconnect-after-frames requires a simulation mode")
    if args.simulate:

        def simulated_backend_factory() -> SyntheticCaptureBackend:
            return SyntheticCaptureBackend(
                camera.width,
                camera.height,
                camera.frame_rate,
                disconnect_after_frames=args.disconnect_after_frames,
            )

        backend_factory = simulated_backend_factory
        publisher = None
        mapper = None
    elif args.simulate_gstreamer:
        publisher = None
        mapper = None
    else:
        publisher = None
        mapper = None
        if camera.adapter is not CameraAdapter.V4L2:
            raise ValueError(f"camera backend {camera.adapter.value!r} is not implemented by Phase 10")
    if not args.simulate and camera.stream_to_surface:
        allocation = derive_stream_allocation(args.camera, camera.stream_index)
        publisher = FrameIndexPublisher(
            config.messaging.broker.publisher_endpoint,
            args.camera,
            metrics=metrics,
            shutdown=ShutdownToken(),
        )
        mapper = RtpFrameIndexMapper(args.camera, session.bytes)

        inject_disconnect = args.disconnect_after_frames is not None

        def streaming_backend_factory() -> CaptureBackend:
            nonlocal inject_disconnect
            assert publisher is not None and mapper is not None
            surface = SurfaceRtpStream(
                args.camera,
                session.bytes,
                str(config.network.surface_ip),
                allocation.rtp_port,
                allocation.rtp_payload_type,
                int.from_bytes(session.bytes[:4], byteorder="big"),
                publisher.publish,
                mapper=mapper,
            )
            if args.simulate_gstreamer:
                backend: CaptureBackend = GStreamerCaptureBackend(
                    camera.width,
                    camera.height,
                    camera.frame_rate,
                    surface_stream=surface,
                )
            else:
                backend = V4L2CaptureBackend(args.camera, camera, surface_stream=surface)
            if inject_disconnect:
                inject_disconnect = False
                assert args.disconnect_after_frames is not None
                return DisconnectAfterFramesBackend(backend, args.disconnect_after_frames)
            return backend

        backend_factory = streaming_backend_factory

    elif not args.simulate:
        inject_disconnect = args.disconnect_after_frames is not None

        def local_backend_factory() -> CaptureBackend:
            nonlocal inject_disconnect
            backend: CaptureBackend
            if args.simulate_gstreamer:
                backend = GStreamerCaptureBackend(camera.width, camera.height, camera.frame_rate)
            else:
                backend = V4L2CaptureBackend(args.camera, camera)
            if inject_disconnect:
                inject_disconnect = False
                assert args.disconnect_after_frames is not None
                return DisconnectAfterFramesBackend(backend, args.disconnect_after_frames)
            return backend

        backend_factory = local_backend_factory

    service = CameraService(
        args.camera,
        camera,
        backend_factory,
        session_uuid=session,
        metrics=metrics,
        logger=logger,
        install_signals=True,
        frame_index_publisher=publisher,
    )
    service.attach_health_publisher(
        CameraHealthPublisher(
            config.messaging.broker.publisher_endpoint,
            args.camera,
            interval_ms=config.diagnostics.publish_interval_ms,
            metrics=metrics,
            state_machine=service.state_machine,
            shutdown=service.shutdown.token,
        )
    )
    service.run()
    return ExitCode.CLEAN_SHUTDOWN


def camera_entrypoint(argv: list[str] | None = None) -> int:
    try:
        return int(camera_main(argv))
    except SystemExit:
        raise
    except ConfigurationError as error:
        for issue in error.issues:
            print(f"{error.error_code} {issue.code} {issue.path}: {issue.message}", file=sys.stderr)
        return int(ExitCode.INVALID_CONFIGURATION)
    except (LiveOwnerError, UnsafeStaleSegmentError, SharedMemoryInvalid, ValueError) as error:
        print(f"{ErrorCode.CONFIG_INVALID} <camera>: {type(error).__name__}: {error}", file=sys.stderr)
        return int(ExitCode.INVALID_CONFIGURATION)
    except V4L2ModeUnsupported as error:
        print(f"{ErrorCode.CAMERA_MODE_UNSUPPORTED} <camera>: {error}", file=sys.stderr)
        return int(ExitCode.INVALID_CONFIGURATION)
    except V4L2ConfigurationError as error:
        print(f"{ErrorCode.CONFIG_INVALID} <camera>: {error}", file=sys.stderr)
        return int(ExitCode.INVALID_CONFIGURATION)
    except OSError as error:
        print(f"{ErrorCode.INTERNAL_ERROR} <camera>: {type(error).__name__}: {error}", file=sys.stderr)
        return int(ExitCode.IO_FAILURE)
    except Exception as error:
        print(f"{ErrorCode.INTERNAL_ERROR} <camera>: {type(error).__name__}: {error}", file=sys.stderr)
        return int(ExitCode.INTERNAL_SOFTWARE_FAILURE)


if __name__ == "__main__":
    raise SystemExit(camera_entrypoint())


__all__ = ["camera_entrypoint", "camera_main"]
