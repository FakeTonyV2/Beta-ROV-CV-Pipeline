"""Small automation-friendly command line for the configuration contract."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Never

from .config.issues import ConfigurationError
from .config.loader import load_config
from .config.probes import HardwareProbeUnavailable, create_default_hardware_probe, validate_hardware_config
from .runtime.exit_codes import ExitCode
from .wire.errors import ErrorCode


class _ExitCodeArgumentParser(argparse.ArgumentParser):
    """Map command-line syntax failures to the supervisor exit contract."""

    def error(self, message: str) -> Never:
        self.print_usage(sys.stderr)
        self.exit(ExitCode.INVALID_ARGUMENTS, f"{self.prog}: error: {message}\n")


def _validate_command(args: argparse.Namespace) -> ExitCode:
    try:
        config = load_config(Path(args.path) if args.path else None)
    except ConfigurationError as error:
        for issue in error.issues:
            print(f"{error.error_code} {issue.code} {issue.path}: {issue.message}", file=sys.stderr)
        if any(issue.code == "CONFIG_FILE_READ_ERROR" for issue in error.issues):
            return ExitCode.IO_FAILURE
        return ExitCode.INVALID_CONFIGURATION
    if args.probe_hardware:
        try:
            probe = create_default_hardware_probe()
        except HardwareProbeUnavailable as error:
            print(f"{ErrorCode.CONFIG_INVALID} HARDWARE_PROBE_UNAVAILABLE <probe>: {error}", file=sys.stderr)
            return ExitCode.INVALID_CONFIGURATION
        issues = validate_hardware_config(config, probe)
        if issues:
            for issue in issues:
                print(f"{ErrorCode.CONFIG_INVALID} {issue.code} {issue.path}: {issue.message}", file=sys.stderr)
            return ExitCode.INVALID_CONFIGURATION
        print(
            f"valid: schema_version={config.schema_version} cameras={len(config.cameras)} tasks={len(config.tasks)} hardware=ok"
        )
        return ExitCode.CLEAN_SHUTDOWN
    print(f"valid: schema_version={config.schema_version} cameras={len(config.cameras)} tasks={len(config.tasks)}")
    return ExitCode.CLEAN_SHUTDOWN


def main(argv: list[str] | None = None) -> ExitCode:
    parser = _ExitCodeArgumentParser(prog="rov-cv")
    commands = parser.add_subparsers(dest="command", required=True)
    config_parser = commands.add_parser("config")
    config_commands = config_parser.add_subparsers(dest="config_command", required=True)
    validate = config_commands.add_parser("validate")
    validate.add_argument(
        "path",
        nargs="?",
        help="mission YAML path; defaults to $PURDUE_ROV_CV_CONFIG or /etc/purdue-rov-cv/mission.yaml",
    )
    validate.add_argument("--probe-hardware", action="store_true")
    validate.set_defaults(handler=_validate_command)
    args = parser.parse_args(argv)
    return args.handler(args)


def entrypoint(argv: list[str] | None = None) -> int:
    """Own unexpected process failures at the installed CLI boundary."""
    try:
        return int(main(argv))
    except SystemExit:
        raise
    except Exception as error:
        print(
            f"{ErrorCode.INTERNAL_ERROR} <cli>: {type(error).__name__}: {error}",
            file=sys.stderr,
        )
        return int(ExitCode.INTERNAL_SOFTWARE_FAILURE)


if __name__ == "__main__":
    raise SystemExit(entrypoint())
