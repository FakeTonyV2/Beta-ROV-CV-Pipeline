"""Small automation-friendly command line for the configuration contract."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
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


def _camera_duration(value: str) -> float:
    try:
        duration = float(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("camera duration must be a number") from error
    if duration < 10.0:
        raise argparse.ArgumentTypeError("camera duration must be at least 10 seconds")
    return duration


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


def _preflight_command(args: argparse.Namespace) -> int:
    from .preflight.probes import BrokerRuntimeEvidenceProvider, LocalSystemPreflightProbe, SimulatedPreflightProbe
    from .preflight.runner import run_preflight

    probe = (
        SimulatedPreflightProbe(args.scenario)
        if args.simulate
        else LocalSystemPreflightProbe(runtime_evidence=BrokerRuntimeEvidenceProvider())
    )
    report = run_preflight(
        Path(args.path),
        probe,
        camera_duration_seconds=args.camera_duration,
    )
    if args.json_report:
        try:
            report.write_json(Path(args.json_report))
        except OSError as error:
            from .preflight.checks import make_report

            report = make_report(
                report.checks,
                execution_error=f"could not write JSON report: {error}",
                configuration_sha256=report.configuration_sha256,
            )
    print(report.human_summary())
    if args.print_json:
        print(report.to_json(indent=None))
    return report.exit_code


def _operator_command(args: argparse.Namespace) -> int:
    from .messaging.client import ControlClient
    from .preflight.operator import SurfaceOperator, read_preflight_observation

    try:
        config = load_config(Path(args.path), environ={})
    except ConfigurationError as error:
        print(str(error), file=sys.stderr)
        return int(ExitCode.INVALID_CONFIGURATION)
    if args.target not in config.tasks or not config.tasks[args.target].enabled:
        print(f"target is not an enabled configured module: {args.target}", file=sys.stderr)
        return int(ExitCode.INVALID_ARGUMENTS)
    with ControlClient(config.messaging.control.client_endpoint) as client:
        operator = SurfaceOperator(client)
        result = getattr(operator, args.operator_command)(args.target)
    preflight = read_preflight_observation(Path(args.preflight_report) if args.preflight_report else None)
    output = json.loads(result.to_json())
    output.update(
        {
            "mission_state": "UNOBSERVED",
            "preflight": {
                "available": preflight.available,
                "detail": preflight.detail,
                "exit_code": preflight.exit_code,
                "mission_eligible": preflight.mission_eligible,
                "result": preflight.result,
                "run_id": preflight.run_id,
            },
            "observed_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        }
    )
    print(json.dumps(output, sort_keys=True))
    return 0 if result.succeeded else 1


def main(argv: list[str] | None = None) -> int:
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
    preflight = commands.add_parser("preflight", help="run the complete Phase 9 preflight checklist")
    preflight.add_argument("path", help="mission YAML path")
    preflight.add_argument("--simulate", action="store_true", help="use deterministic simulated probes")
    preflight.add_argument(
        "--scenario",
        choices=(
            "success",
            "invalid_model_hash",
            "invalid_camera_mode",
            "unsynchronized_clock",
            "missing_component",
            "component_error",
        ),
        default="success",
        help="fault scenario used with --simulate",
    )
    preflight.add_argument("--camera-duration", type=_camera_duration, default=10.0)
    preflight.add_argument("--json-report", help="write the versioned JSON report to this path")
    preflight.add_argument("--print-json", action="store_true")
    preflight.set_defaults(handler=_preflight_command)

    operator = commands.add_parser("operator", help="surface control-plane client")
    operator.add_argument("operator_command", choices=("get_status", "start", "stop"))
    operator.add_argument("target", help="enabled configured module ID")
    operator.add_argument("--config", dest="path", required=True)
    operator.add_argument("--preflight-report", help="optional Phase 9 JSON report for preflight evidence")
    operator.set_defaults(handler=_operator_command)
    args = parser.parse_args(argv)
    if args.command == "preflight" and not args.simulate and args.scenario != "success":
        parser.error("--scenario requires --simulate")
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
