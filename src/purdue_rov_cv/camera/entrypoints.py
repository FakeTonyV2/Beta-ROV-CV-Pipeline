"""Installed camera-service boundary and canonical exit translation."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Never
from uuid import uuid4

from purdue_rov_cv.config.issues import ConfigurationError
from purdue_rov_cv.config.loader import load_config
from purdue_rov_cv.config.ports import derive_stream_allocation
from purdue_rov_cv.frame_buffer import LiveOwnerError, SharedMemoryInvalid, UnsafeStaleSegmentError
from purdue_rov_cv.runtime.exit_codes import ExitCode
from purdue_rov_cv.runtime.json_logging import configure_json_logger
from purdue_rov_cv.runtime.metrics import RuntimeMetrics
from purdue_rov_cv.runtime.shutdown import ShutdownToken
from purdue_rov_cv.video.mapping import RtpFrameIndexMapper
from purdue_rov_cv.video.sender import FrameIndexPublisher
from purdue_rov_cv.wire.errors import ErrorCode

from .backend import GStreamerCaptureBackend, SurfaceRtpStream
from .service import CameraService


class _CameraArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> Never:
        self.print_usage(sys.stderr)
        self.exit(ExitCode.INVALID_ARGUMENTS, f"{self.prog}: error: {message}\n")


def _parser() -> _CameraArgumentParser:
    parser = _CameraArgumentParser(prog="purdue-cv-camera")
    parser.add_argument("--camera", required=True, help="configured camera ID")
    parser.add_argument("--config", type=Path, help="mission YAML")
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
    publisher = None
    mapper = None
    if camera.stream_to_surface:
        allocation = derive_stream_allocation(args.camera, camera.stream_index)
        publisher = FrameIndexPublisher(
            config.messaging.broker.publisher_endpoint,
            args.camera,
            metrics=metrics,
            shutdown=ShutdownToken(),
        )
        mapper = RtpFrameIndexMapper(args.camera, session.bytes)

        def streaming_backend_factory() -> GStreamerCaptureBackend:
            assert publisher is not None and mapper is not None
            return GStreamerCaptureBackend(
                camera.width,
                camera.height,
                camera.frame_rate,
                surface_stream=SurfaceRtpStream(
                    args.camera,
                    session.bytes,
                    str(config.network.surface_ip),
                    allocation.rtp_port,
                    allocation.rtp_payload_type,
                    int.from_bytes(session.bytes[:4], byteorder="big"),
                    publisher.publish,
                    mapper=mapper,
                ),
            )

        backend_factory = streaming_backend_factory

    else:

        def local_backend_factory() -> GStreamerCaptureBackend:
            return GStreamerCaptureBackend(camera.width, camera.height, camera.frame_rate)

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
    except OSError as error:
        print(f"{ErrorCode.INTERNAL_ERROR} <camera>: {type(error).__name__}: {error}", file=sys.stderr)
        return int(ExitCode.IO_FAILURE)
    except Exception as error:
        print(f"{ErrorCode.INTERNAL_ERROR} <camera>: {type(error).__name__}: {error}", file=sys.stderr)
        return int(ExitCode.INTERNAL_SOFTWARE_FAILURE)


if __name__ == "__main__":
    raise SystemExit(camera_entrypoint())


__all__ = ["camera_entrypoint", "camera_main"]
