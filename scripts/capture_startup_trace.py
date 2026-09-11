#!/usr/bin/env python3
"""Capture systemd and application readiness for all ten startup stages."""

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path
from typing import Any

from purdue_rov_cv.config.loader import load_config
from purdue_rov_cv.deployment.evidence import artifact_envelope, utc_now, write_artifact

ROOT = Path(__file__).parents[1]

STAGE_NAMES = (
    "network",
    "chrony",
    "broker",
    "control-router",
    "cameras",
    "modules",
    "video-receivers",
    "recorder-operator",
    "preflight",
    "mission-enable",
)


def _unit(unit: str) -> dict[str, Any]:
    result = subprocess.run(
        [
            "systemctl",
            "show",
            unit,
            "--property=Id,ActiveState,SubState,ActiveEnterTimestampMonotonic,ExecMainPID,ExecMainStatus",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    values = dict(line.split("=", 1) for line in result.stdout.splitlines() if "=" in line)
    values["query_returncode"] = result.returncode
    return values


def _json_or_unverified(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {"state": "UNVERIFIED", "path": str(path) if path else None}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        return {"state": "UNVERIFIED", "path": str(path), "error": str(error)}
    return {"state": "OBSERVED", "path": str(path), "value": value}


def _application_ready(stage_name: str, application: Any) -> bool:
    if not isinstance(application, dict) or application.get("state") != "OBSERVED":
        return False
    value = application.get("value")
    if not isinstance(value, dict):
        return False
    if stage_name == "preflight":
        checks = value.get("checks")
        acceptable = (
            [
                item
                for item in checks
                if isinstance(item, dict)
                and (
                    item.get("status") == "PASS"
                    or (item.get("status") in {"WARNING", "UNAVAILABLE"} and item.get("fatal") is False)
                )
            ]
            if isinstance(checks, list)
            else []
        )
        return (
            value.get("overall_result") == "PASS"
            and value.get("exit_code") == 0
            and value.get("mission_enable_decision") is True
            and isinstance(checks, list)
            and len(checks) == 20
            and len(acceptable) == 20
        )
    if stage_name == "mission-enable":
        return value.get("mission_enabled") is True and value.get("mission_state") == "ENABLED"
    return value.get("result") == "PASS" or value.get("ready") is True


def _stage_ready(stage: dict[str, Any]) -> bool:
    def systemd_active(value: Any) -> bool:
        if not isinstance(value, dict):
            return True
        if "ActiveState" in value:
            return value["ActiveState"] == "active" and value.get("query_returncode") == 0
        return all(systemd_active(item) for item in value.values())

    name = stage.get("stage")
    if name in {"video-receivers", "recorder-operator"} and "systemd" not in stage:
        return stage.get("state") in {"PASS", "READY", "RUNNING"}
    if not systemd_active(stage.get("systemd", {})):
        return False
    if name in {"cameras", "modules", "preflight", "mission-enable"}:
        return isinstance(name, str) and _application_ready(name, stage.get("application"))
    return stage.get("state") != "UNVERIFIED"


def capture(
    *,
    config_path: Path,
    preflight_report: Path | None,
    mission_state: Path | None,
    surface_trace: Path | None,
    readiness_report: Path | None,
) -> list[dict[str, Any]]:
    config = load_config(config_path, environ={})
    surface = _json_or_unverified(surface_trace)
    surface_stages = (
        surface.get("value", {}).get("measurements", {}).get("stages", []) if surface["state"] == "OBSERVED" else []
    )
    by_stage = {item.get("stage"): item for item in surface_stages if isinstance(item, dict)}
    stages: list[dict[str, Any]] = [
        {
            "stage": STAGE_NAMES[0],
            "service_process": "network-online.target",
            "systemd": _unit("network-online.target"),
        },
        {"stage": STAGE_NAMES[1], "service_process": "chrony.service", "systemd": _unit("chrony.service")},
        {"stage": STAGE_NAMES[2], "service_process": "purdue-cv-broker", "systemd": _unit("purdue-cv-broker.service")},
        {
            "stage": STAGE_NAMES[3],
            "service_process": "purdue-cv-control-router",
            "systemd": _unit("purdue-cv-control-router.service"),
        },
        {
            "stage": STAGE_NAMES[4],
            "service_process": "camera instances",
            "systemd": {camera_id: _unit(f"purdue-cv-camera@{camera_id}.service") for camera_id in config.cameras},
            "application": _json_or_unverified(readiness_report),
        },
        {
            "stage": STAGE_NAMES[5],
            "service_process": "module instances",
            "systemd": {
                task_id: _unit(f"purdue-cv-module@{task_id}.service")
                for task_id, task in config.tasks.items()
                if task.enabled
            },
            "application": _json_or_unverified(readiness_report),
        },
        by_stage.get(
            STAGE_NAMES[6], {"stage": STAGE_NAMES[6], "state": "UNVERIFIED", "reason": "surface trace absent"}
        ),
        by_stage.get(
            STAGE_NAMES[7], {"stage": STAGE_NAMES[7], "state": "UNVERIFIED", "reason": "surface trace absent"}
        ),
        {
            "stage": STAGE_NAMES[8],
            "service_process": "production preflight",
            "application": _json_or_unverified(preflight_report),
        },
        {
            "stage": STAGE_NAMES[9],
            "service_process": "authoritative mission gate",
            "application": _json_or_unverified(mission_state),
        },
    ]
    return stages


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, default=Path("/etc/purdue-rov-cv/mission.yaml"))
    parser.add_argument("--preflight-report", type=Path)
    parser.add_argument("--mission-state", type=Path)
    parser.add_argument("--surface-trace", type=Path)
    parser.add_argument("--readiness-report", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    started = utc_now()
    stages = capture(
        config_path=args.config,
        preflight_report=args.preflight_report,
        mission_state=args.mission_state,
        surface_trace=args.surface_trace,
        readiness_report=args.readiness_report,
    )

    complete = all(_stage_ready(stage) for stage in stages)
    artifact = artifact_envelope(
        kind="startup-trace",
        root=ROOT,
        config_path=args.config,
        started_at=started,
        ended_at=utc_now(),
        measurements={"stages": stages},
        events=[],
        result="PASS" if complete else "UNVERIFIED",
        normative=True,
    )
    write_artifact(args.output, artifact)
    print(args.output)
    return 0 if complete else 1


if __name__ == "__main__":
    raise SystemExit(main())
