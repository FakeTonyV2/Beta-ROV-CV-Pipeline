"""Installed per-camera surface video receiver entry point."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Never
from uuid import uuid4

from purdue_rov_cv.config.issues import ConfigurationError
from purdue_rov_cv.config.loader import load_config
from purdue_rov_cv.runtime.exit_codes import ExitCode
from purdue_rov_cv.runtime.json_logging import configure_json_logger
from purdue_rov_cv.wire.errors import ErrorCode

from .service import VideoReceiverService


class _ReceiverArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> Never:
        self.print_usage(sys.stderr)
        self.exit(ExitCode.INVALID_ARGUMENTS, f"{self.prog}: error: {message}\n")


def _parser() -> _ReceiverArgumentParser:
    parser = _ReceiverArgumentParser(prog="purdue-cv-video-receiver")
    parser.add_argument("--camera", required=True, help="configured surface-visible camera ID")
    parser.add_argument("--config", type=Path, help="mission YAML")
    parser.add_argument("--approximate-debug", action="store_true", help="label wrap-aware near-RTP matches")
    return parser


def video_receiver_main(argv: list[str] | None = None) -> ExitCode:
    args = _parser().parse_args(argv)
    config = load_config(args.config)
    session = uuid4()
    logger = configure_json_logger(
        device_id=config.device.device_id,
        process_name="purdue-cv-video-receiver",
        source_id=args.camera,
        publisher_session_id=session,
    )
    service = VideoReceiverService.from_config(
        args.camera,
        config,
        logger=logger,
        install_signals=True,
        approximate_debug=args.approximate_debug,
    )
    service.run()
    return ExitCode.CLEAN_SHUTDOWN


def video_receiver_entrypoint(argv: list[str] | None = None) -> int:
    try:
        return int(video_receiver_main(argv))
    except SystemExit:
        raise
    except ConfigurationError as error:
        for issue in error.issues:
            print(f"{error.error_code} {issue.code} {issue.path}: {issue.message}", file=sys.stderr)
        return int(ExitCode.INVALID_CONFIGURATION)
    except ValueError as error:
        print(f"{ErrorCode.CONFIG_INVALID} <video-receiver>: {error}", file=sys.stderr)
        return int(ExitCode.INVALID_CONFIGURATION)
    except OSError as error:
        print(f"{ErrorCode.INTERNAL_ERROR} <video-receiver>: {type(error).__name__}: {error}", file=sys.stderr)
        return int(ExitCode.IO_FAILURE)
    except Exception as error:
        print(f"{ErrorCode.INTERNAL_ERROR} <video-receiver>: {type(error).__name__}: {error}", file=sys.stderr)
        return int(ExitCode.INTERNAL_SOFTWARE_FAILURE)


if __name__ == "__main__":
    raise SystemExit(video_receiver_entrypoint())


__all__ = ["video_receiver_entrypoint", "video_receiver_main"]
