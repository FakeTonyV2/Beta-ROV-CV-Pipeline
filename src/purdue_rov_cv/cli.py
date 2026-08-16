"""Small automation-friendly command line for the configuration contract."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .config.issues import ConfigurationError
from .config.loader import load_config
from .config.probes import HardwareProbeUnavailable, create_default_hardware_probe, validate_hardware_config
from .wire.errors import ErrorCode


def _validate_command(args: argparse.Namespace) -> int:
    try:
        config = load_config(Path(args.path) if args.path else None)
    except ConfigurationError as error:
        for issue in error.issues:
            print(f"{error.error_code} {issue.code} {issue.path}: {issue.message}", file=sys.stderr)
        return 2
    if args.probe_hardware:
        try:
            probe = create_default_hardware_probe()
        except HardwareProbeUnavailable as error:
            print(f"{ErrorCode.CONFIG_INVALID} HARDWARE_PROBE_UNAVAILABLE <probe>: {error}", file=sys.stderr)
            return 3
        issues = validate_hardware_config(config, probe)
        if issues:
            for issue in issues:
                print(f"{ErrorCode.CONFIG_INVALID} {issue.code} {issue.path}: {issue.message}", file=sys.stderr)
            return 4
        print(
            f"valid: schema_version={config.schema_version} cameras={len(config.cameras)} tasks={len(config.tasks)} hardware=ok"
        )
        return 0
    print(f"valid: schema_version={config.schema_version} cameras={len(config.cameras)} tasks={len(config.tasks)}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="rov-cv")
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


if __name__ == "__main__":
    raise SystemExit(main())
