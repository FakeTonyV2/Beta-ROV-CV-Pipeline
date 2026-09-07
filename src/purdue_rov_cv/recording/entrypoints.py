"""Installed production recorder entry point."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Never

from purdue_rov_cv.config.issues import ConfigurationError
from purdue_rov_cv.config.loader import load_config
from purdue_rov_cv.runtime.exit_codes import ExitCode
from purdue_rov_cv.runtime.json_logging import configure_json_logger
from purdue_rov_cv.wire.errors import ErrorCode

from .service import RecorderService


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> Never:
        self.print_usage(sys.stderr)
        self.exit(ExitCode.INVALID_ARGUMENTS, f"{self.prog}: error: {message}\n")


def recorder_main(argv: list[str] | None = None) -> ExitCode:
    parser = _Parser(prog="purdue-cv-recorder")
    parser.add_argument("--config", type=Path)
    parser.add_argument("--session", required=True, help="safe recording session identifier")
    args = parser.parse_args(argv)
    config = load_config(args.config)
    logger = configure_json_logger(
        device_id=config.device.device_id,
        process_name="purdue-cv-recorder",
        source_id="recorder",
        publisher_session_id=None,
    )
    service = RecorderService.from_config(config, args.session, logger=logger, install_signals=True)
    service.run()
    return ExitCode.CLEAN_SHUTDOWN


def recorder_entrypoint(argv: list[str] | None = None) -> int:
    try:
        return int(recorder_main(argv))
    except SystemExit:
        raise
    except ConfigurationError as error:
        for issue in error.issues:
            print(f"{error.error_code} {issue.code} {issue.path}: {issue.message}", file=sys.stderr)
        return int(ExitCode.INVALID_CONFIGURATION)
    except ValueError as error:
        print(f"{ErrorCode.CONFIG_INVALID} <recorder>: {error}", file=sys.stderr)
        return int(ExitCode.INVALID_CONFIGURATION)
    except OSError as error:
        print(f"{ErrorCode.INTERNAL_ERROR} <recorder>: {type(error).__name__}: {error}", file=sys.stderr)
        return int(ExitCode.IO_FAILURE)
    except Exception as error:
        print(f"{ErrorCode.INTERNAL_ERROR} <recorder>: {type(error).__name__}: {error}", file=sys.stderr)
        return int(ExitCode.INTERNAL_SOFTWARE_FAILURE)


__all__ = ["recorder_entrypoint", "recorder_main"]
