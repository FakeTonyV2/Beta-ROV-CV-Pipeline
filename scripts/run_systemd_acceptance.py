#!/usr/bin/env python3
"""Safely verify restart, exit-78, start-limit, and SIGTERM behavior."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from purdue_rov_cv.deployment.evidence import artifact_envelope, utc_now, write_artifact

ROOT = Path(__file__).parents[1]
FIXTURE = Path(__file__).with_name("systemd_fixture.py")
PREFIX = "purdue-cv-acceptance-"


def _show(unit: str) -> dict[str, str]:
    result = subprocess.run(
        [
            "systemctl",
            "show",
            unit,
            "--property=ActiveState,SubState,Result,ExecMainStatus,NRestarts",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    return dict(line.split("=", 1) for line in result.stdout.splitlines() if "=" in line)


def _run_unit(unit: str, mode: str, marker: Path | None = None) -> None:
    command = [
        "systemd-run",
        f"--unit={unit}",
        "--property=Type=simple",
        "--property=Restart=on-failure",
        "--property=RestartSec=2",
        "--property=TimeoutStopSec=5",
        "--property=KillSignal=SIGTERM",
        "--property=RestartPreventExitStatus=78",
        "--property=StartLimitIntervalSec=60",
        "--property=StartLimitBurst=5",
        sys.executable,
        str(FIXTURE),
        mode,
    ]
    if marker is not None:
        command.extend(["--marker", str(marker)])
    subprocess.run(command, check=True)


def _cleanup(units: list[str]) -> None:
    for unit in units:
        subprocess.run(["systemctl", "stop", unit], capture_output=True, check=False)
        subprocess.run(["systemctl", "reset-failed", unit], capture_output=True, check=False)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--allow-systemd-test", action="store_true")
    parser.add_argument("--config", type=Path, default=Path("/etc/purdue-rov-cv/mission.yaml"))
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not args.allow_systemd_test:
        raise SystemExit("systemd acceptance requires --allow-systemd-test")
    if os.name != "posix" or os.geteuid() != 0:
        raise SystemExit("systemd acceptance requires root on the deployed Linux host")
    started = utc_now()
    units = [f"{PREFIX}{name}.service" for name in ("restart", "exit78", "limit", "sigterm")]
    events: list[dict[str, Any]] = []
    results: dict[str, Any] = {}
    marker_root = Path("/run/purdue-rov-cv")
    marker_root.mkdir(parents=True, exist_ok=True)
    restart_marker = marker_root / "systemd-restart-fixture"
    term_marker = marker_root / "systemd-sigterm-fixture"
    restart_marker.unlink(missing_ok=True)
    term_marker.unlink(missing_ok=True)
    try:
        before = time.monotonic()
        _run_unit(units[0], "fail-once", restart_marker)
        deadline = before + 8.0
        restart_state: dict[str, str] = {}
        while time.monotonic() < deadline:
            restart_state = _show(units[0])
            if restart_state.get("ActiveState") == "active" and int(restart_state.get("NRestarts", "0")) >= 1:
                break
            time.sleep(0.1)
        restart_delay = time.monotonic() - before
        results["restart_on_failure"] = {
            "passed": restart_state.get("ActiveState") == "active" and restart_delay >= 1.8,
            "observed_delay_seconds": restart_delay,
            "systemd": restart_state,
        }

        _run_unit(units[1], "exit78")
        time.sleep(3.0)
        exit_state = _show(units[1])
        results["exit_78"] = {
            "passed": exit_state.get("ExecMainStatus") == "78" and exit_state.get("NRestarts") == "0",
            "systemd": exit_state,
        }

        _run_unit(units[2], "fail")
        time.sleep(13.0)
        limit_state = _show(units[2])
        results["start_limit"] = {
            "passed": limit_state.get("ActiveState") == "failed" and int(limit_state.get("NRestarts", "0")) >= 4,
            "systemd": limit_state,
        }

        _run_unit(units[3], "run", term_marker)
        time.sleep(0.5)
        before_stop = time.monotonic()
        subprocess.run(["systemctl", "stop", units[3]], check=True)
        stop_duration = time.monotonic() - before_stop
        results["sigterm"] = {
            "passed": stop_duration <= 5.0 and term_marker.read_text(encoding="ascii").strip() == "SIGTERM",
            "stop_duration_seconds": stop_duration,
            "systemd": _show(units[3]),
        }
    finally:
        _cleanup(units)
        restart_marker.unlink(missing_ok=True)
        term_marker.unlink(missing_ok=True)
        events.append({"event": "all controlled transient fixtures cleaned up", "timestamp": utc_now()})
    passed = all(bool(value.get("passed")) for value in results.values()) and len(results) == 4
    artifact = artifact_envelope(
        kind="systemd-supervision-acceptance",
        root=ROOT,
        config_path=args.config,
        started_at=started,
        ended_at=utc_now(),
        measurements=results,
        events=events,
        result="PASS" if passed else "FAIL",
        normative=True,
    )
    write_artifact(args.output, artifact)
    print(args.output)
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
