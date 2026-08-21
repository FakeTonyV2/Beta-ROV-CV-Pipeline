"""Supervisor-facing broker and router process entry points."""

from __future__ import annotations

import argparse
import errno
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Never

import zmq

from purdue_rov_cv.config.issues import ConfigurationError
from purdue_rov_cv.config.loader import load_config
from purdue_rov_cv.runtime.exit_codes import ExitCode
from purdue_rov_cv.runtime.json_logging import configure_json_logger
from purdue_rov_cv.wire.errors import ErrorCode

from .broker import DataBrokerService
from .router import ControlRouterService

_ZMQ_IO_ERRNOS = frozenset({errno.EACCES, errno.ENOENT, errno.ENOSPC})
_ZMQ_INVALID_DEPLOYMENT_ERRNOS = frozenset({errno.EADDRNOTAVAIL, errno.ENODEV})
_ZMQ_TEMPORARY_ERRNOS = frozenset(
    {
        errno.EADDRINUSE,
        errno.EAGAIN,
        errno.EBUSY,
        errno.EINTR,
        errno.EMFILE,
        errno.ENFILE,
        errno.ENOMEM,
    }
)


class _ServiceArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> Never:
        self.print_usage(sys.stderr)
        self.exit(ExitCode.INVALID_ARGUMENTS, f"{self.prog}: error: {message}\n")


def _parser(prog: str) -> _ServiceArgumentParser:
    parser = _ServiceArgumentParser(prog=prog)
    parser.add_argument(
        "--config",
        type=Path,
        help="mission YAML; defaults to PURDUE_ROV_CV_CONFIG or /etc/purdue-rov-cv/mission.yaml",
    )
    return parser


def broker_main(argv: list[str] | None = None) -> ExitCode:
    args = _parser("purdue-cv-broker").parse_args(argv)
    config = load_config(args.config)
    logger = configure_json_logger(
        device_id=config.device.device_id,
        process_name="purdue-cv-broker",
        source_id="data-broker",
        publisher_session_id=None,
    )
    service = DataBrokerService.from_config(config, logger=logger, install_signals=True)
    service.run()
    return ExitCode.CLEAN_SHUTDOWN


def control_router_main(argv: list[str] | None = None) -> ExitCode:
    args = _parser("purdue-cv-control-router").parse_args(argv)
    config = load_config(args.config)
    logger = configure_json_logger(
        device_id=config.device.device_id,
        process_name="purdue-cv-control-router",
        source_id="control-router",
        publisher_session_id=None,
    )
    service = ControlRouterService.from_config(config, logger=logger, install_signals=True)
    service.run()
    return ExitCode.CLEAN_SHUTDOWN


def _translate_boundary(call: Callable[[list[str] | None], ExitCode], argv: list[str] | None) -> int:
    try:
        return int(call(argv))
    except SystemExit:
        raise
    except ConfigurationError as error:
        for issue in error.issues:
            print(f"{error.error_code} {issue.code} {issue.path}: {issue.message}", file=sys.stderr)
        if any(issue.code == "CONFIG_FILE_READ_ERROR" for issue in error.issues):
            return int(ExitCode.IO_FAILURE)
        return int(ExitCode.INVALID_CONFIGURATION)
    except zmq.ZMQError as error:
        print(f"{ErrorCode.INTERNAL_ERROR} <service>: ZMQError: {error}", file=sys.stderr)
        if error.errno in _ZMQ_IO_ERRNOS:
            return int(ExitCode.IO_FAILURE)
        if error.errno in _ZMQ_INVALID_DEPLOYMENT_ERRNOS:
            return int(ExitCode.INVALID_CONFIGURATION)
        if error.errno in _ZMQ_TEMPORARY_ERRNOS:
            return int(ExitCode.TEMPORARY_FAILURE)
        return int(ExitCode.INTERNAL_SOFTWARE_FAILURE)
    except OSError as error:
        print(f"{ErrorCode.INTERNAL_ERROR} <service>: {type(error).__name__}: {error}", file=sys.stderr)
        return int(ExitCode.IO_FAILURE)
    except Exception as error:
        print(f"{ErrorCode.INTERNAL_ERROR} <service>: {type(error).__name__}: {error}", file=sys.stderr)
        return int(ExitCode.INTERNAL_SOFTWARE_FAILURE)


def broker_entrypoint(argv: list[str] | None = None) -> int:
    return _translate_boundary(broker_main, argv)


def control_router_entrypoint(argv: list[str] | None = None) -> int:
    return _translate_boundary(control_router_main, argv)


__all__ = ["broker_entrypoint", "broker_main", "control_router_entrypoint", "control_router_main"]
