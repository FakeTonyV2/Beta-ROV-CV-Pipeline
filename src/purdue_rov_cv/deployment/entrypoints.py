"""Installed process entry points for Phase 11 deployment roles."""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
from datetime import datetime, timezone
from pathlib import Path
from threading import Event

import psutil

from purdue_rov_cv.config.issues import ConfigurationError
from purdue_rov_cv.config.loader import load_config
from purdue_rov_cv.preflight.clock import ChronyClockProbe, ClockMonitor
from purdue_rov_cv.runtime.exit_codes import ExitCode

from .environment import EnvironmentValidator, write_report


def environment_validator_entrypoint(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="purdue-cv-validate-environment")
    parser.add_argument("--role", choices=("pi", "surface"), required=True)
    parser.add_argument("--config", type=Path, default=Path("/etc/purdue-rov-cv/mission.yaml"))
    parser.add_argument("--json-report", type=Path)
    args = parser.parse_args(argv)
    report = EnvironmentValidator().validate(role=args.role, config_path=args.config)
    if args.json_report is not None:
        write_report(report, args.json_report)
    print(report.human_summary())
    return 0 if report.supported else 78


def _atomic_json(path: Path, value: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(json.dumps(value, sort_keys=True) + "\n", encoding="utf-8")
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def _health_main(role: str, argv: list[str] | None) -> int:
    parser = argparse.ArgumentParser(prog=f"purdue-cv-{role}-health")
    parser.add_argument("--config", type=Path, default=Path("/etc/purdue-rov-cv/mission.yaml"))
    parser.add_argument("--state-file", type=Path, default=Path(f"/run/purdue-rov-cv/{role}-health.json"))
    args = parser.parse_args(argv)
    config = load_config(args.config, environ={})
    shutdown = Event()

    def stop(_signum: int, _frame: object) -> None:
        shutdown.set()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    monitor = ClockMonitor(
        maximum_offset_ms=config.clock.maximum_offset_ms,
        invalidate_after_failures=config.clock.invalid_after_failures,
    )
    probe = ChronyClockProbe()
    while not shutdown.is_set():
        status = monitor.poll(probe)
        disk = psutil.disk_usage("/")
        temperatures: list[float] = []
        try:
            temperatures = [
                float(item.current)
                for values in psutil.sensors_temperatures().values()
                for item in values
                if item.current is not None
            ]
        except (AttributeError, OSError):
            pass
        value: dict[str, object] = {
            "schema_version": 1,
            "role": role,
            "health": "NORMAL" if status.synchronized else "DEGRADED",
            "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "clock": {
                "synchronized": status.synchronized,
                "cross_device_latency_valid": status.cross_device_latency_valid,
                "consecutive_failures": status.consecutive_failures,
                "offset_ms": status.sample.estimated_offset_ms if status.sample is not None else None,
                "reason": status.reason,
            },
            "resources": {
                "available_memory_bytes": int(psutil.virtual_memory().available),
                "root_free_bytes": int(disk.free),
                "cpu_percent": float(psutil.cpu_percent(interval=None)),
                "temperature_c": max(temperatures) if temperatures else None,
                "thermally_throttled": _thermally_throttled(),
                "thermal_mission_gate": False,
            },
        }
        _atomic_json(args.state_file, value)
        print(json.dumps(value, sort_keys=True), flush=True)
        shutdown.wait(config.clock.check_interval_seconds)
    return 0


def _thermally_throttled() -> bool | None:
    path = Path("/sys/devices/platform/soc/soc:firmware/get_throttled")
    try:
        return path.read_text(encoding="ascii").strip().lower() not in {"0", "0x0"}
    except OSError:
        return None


def system_health_entrypoint(argv: list[str] | None = None) -> int:
    return _health_entrypoint("system", argv)


def surface_health_entrypoint(argv: list[str] | None = None) -> int:
    return _health_entrypoint("surface", argv)


def _health_entrypoint(role: str, argv: list[str] | None) -> int:
    try:
        return _health_main(role, argv)
    except ConfigurationError as error:
        print(str(error), file=sys.stderr)
        return int(ExitCode.INVALID_CONFIGURATION)
    except ValueError as error:
        print(f"invalid {role} health configuration: {error}", file=sys.stderr)
        return int(ExitCode.INVALID_CONFIGURATION)
    except OSError as error:
        print(f"{role} health I/O failure: {error}", file=sys.stderr)
        return int(ExitCode.IO_FAILURE)
    except Exception as error:
        print(f"{role} health internal failure: {type(error).__name__}: {error}", file=sys.stderr)
        return int(ExitCode.INTERNAL_SOFTWARE_FAILURE)


__all__ = ["environment_validator_entrypoint", "surface_health_entrypoint", "system_health_entrypoint"]
