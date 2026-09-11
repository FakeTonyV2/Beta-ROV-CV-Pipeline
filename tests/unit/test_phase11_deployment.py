"""Phase 11 environment, topology, unit-policy, and evidence contracts."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from argparse import Namespace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from purdue_rov_cv.camera.health import CameraHealthPublisher
from purdue_rov_cv.config.loader import config_hash, load_config
from purdue_rov_cv.deployment.entrypoints import system_health_entrypoint
from purdue_rov_cv.deployment.environment import EnvironmentStatus, EnvironmentValidator, write_report
from purdue_rov_cv.deployment.evidence import (
    analyze_memory,
    artifact_envelope,
    configuration_hash,
    write_artifact,
)
from purdue_rov_cv.preflight.authorization import PreflightStartAuthorizer
from purdue_rov_cv.preflight.checks import CameraMeasurement, CorrelationMeasurement
from purdue_rov_cv.preflight.clock import ClockStatus
from purdue_rov_cv.preflight.health import ComponentHealth, MissionState
from purdue_rov_cv.runtime.metrics import RuntimeMetrics
from purdue_rov_cv.runtime.shutdown import ShutdownToken
from purdue_rov_cv.runtime.state import ComponentState, ComponentStateMachine
from scripts.capture_startup_trace import STAGE_NAMES, _application_ready
from scripts.configure_deployment import configure
from scripts.run_phase11_hil import _camera_hub, _runtime_component_results, _stability_merge
from scripts.validate_systemd import validate_units

ROOT = Path(__file__).parents[2]
CONFIG = ROOT / "config" / "mission.yaml"


def _supported_root(tmp_path: Path) -> Path:
    for path in (
        "etc",
        "run/systemd/system",
        "run/purdue-rov-cv",
        "var/lib/purdue-rov-cv",
        "sys/class/net/eth0",
        "sys/firmware/devicetree/base",
        "dev/v4l/by-id",
        "opt/purdue-rov-cv/models",
    ):
        (tmp_path / path).mkdir(parents=True, exist_ok=True)
    (tmp_path / "etc/os-release").write_text('ID=ubuntu\nVERSION_ID="24.04"\n', encoding="utf-8")
    (tmp_path / "sys/class/net/eth0/operstate").write_text("up\n", encoding="ascii")
    (tmp_path / "sys/firmware/devicetree/base/model").write_text("Raspberry Pi 5 Model B Rev 1.0\x00", encoding="ascii")
    (tmp_path / "dev/v4l/by-id/usb-purdue-rov-front-camera").write_text("device", encoding="ascii")
    (tmp_path / "opt/purdue-rov-cv/models/gate_detector.onnx").write_bytes(b"model")
    return tmp_path


def test_environment_validator_reports_exact_categories(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    root = _supported_root(tmp_path)
    monkeypatch.setattr(
        "purdue_rov_cv.deployment.environment.psutil.virtual_memory", lambda: type("M", (), {"available": 1024**3})()
    )
    monkeypatch.setattr("purdue_rov_cv.deployment.environment.psutil.sensors_temperatures", lambda: {})
    monkeypatch.setattr(shutil, "disk_usage", lambda _path: type("D", (), {"free": 4 * 1024**3})())

    def run(command, **_kwargs):  # type: ignore[no-untyped-def]
        if command[:2] == ["ip", "-json"]:
            return subprocess.CompletedProcess(
                command, 0, '[{"addr_info":[{"family":"inet","local":"192.168.50.2"}]}]', ""
            )
        if command[0] == "ping":
            return subprocess.CompletedProcess(command, 0, "", "")
        return subprocess.CompletedProcess(command, 0, "gst-launch-1.0 version 1.24.2\n", "")

    validator = EnvironmentValidator(
        machine=lambda: "aarch64", which=lambda name: f"/usr/bin/{name}", run=run, root=root
    )
    report = validator.validate(role="pi", config_path=CONFIG)
    assert report.supported
    assert report.overall_status is EnvironmentStatus.UNVERIFIED
    thermal = next(check for check in report.checks if check.check_id == "ENV-014")
    assert thermal.status is EnvironmentStatus.UNVERIFIED and not thermal.hard_requirement


def test_environment_validator_never_hides_hard_mismatch(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    root = _supported_root(tmp_path)
    monkeypatch.setattr(
        "purdue_rov_cv.deployment.environment.psutil.virtual_memory", lambda: type("M", (), {"available": 1024**3})()
    )
    monkeypatch.setattr("purdue_rov_cv.deployment.environment.psutil.sensors_temperatures", lambda: {})
    validator = EnvironmentValidator(machine=lambda: "x86_64", which=lambda _name: None, root=root)
    report = validator.validate(role="pi", config_path=CONFIG)
    assert not report.supported
    architecture = next(check for check in report.checks if check.check_id == "ENV-001")
    assert architecture.status is EnvironmentStatus.UNSUPPORTED
    assert architecture.detected == "x86_64"


def test_environment_report_and_artifact_are_machine_readable(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    root = _supported_root(tmp_path)
    monkeypatch.setattr(
        "purdue_rov_cv.deployment.environment.psutil.virtual_memory", lambda: type("M", (), {"available": 1024**3})()
    )
    monkeypatch.setattr("purdue_rov_cv.deployment.environment.psutil.sensors_temperatures", lambda: {})
    monkeypatch.setattr(shutil, "disk_usage", lambda _path: type("D", (), {"free": 4 * 1024**3})())

    def run(command, **_kwargs):  # type: ignore[no-untyped-def]
        if command[:2] == ["ip", "-json"]:
            return subprocess.CompletedProcess(
                command, 0, '[{"addr_info":[{"family":"inet","local":"192.168.50.2"}]}]', ""
            )
        if command[0] == "ping":
            return subprocess.CompletedProcess(command, 0, "", "")
        return subprocess.CompletedProcess(command, 0, "gst-launch-1.0 version 1.24.2\n", "")

    report = EnvironmentValidator(
        machine=lambda: "aarch64", which=lambda name: f"/usr/bin/{name}", run=run, root=root
    ).validate(role="pi", config_path=CONFIG)
    environment_path = tmp_path / "evidence/environment.json"
    write_report(report, environment_path)
    assert json.loads(environment_path.read_text(encoding="utf-8"))["role"] == "pi"
    assert "Deployment environment (pi)" in report.human_summary()

    artifact = artifact_envelope(
        kind="unit-evidence",
        root=ROOT,
        config_path=CONFIG,
        started_at="2026-09-10T00:00:00Z",
        ended_at="2026-09-10T00:00:01Z",
        measurements={"value": 1},
        events=[{"event": "sample"}],
        result="PASS",
        normative=True,
    )
    artifact_path = tmp_path / "evidence/artifact.json"
    write_artifact(artifact_path, artifact)
    decoded = json.loads(artifact_path.read_text(encoding="utf-8"))
    assert decoded["repository"]["revision"]
    assert decoded["configuration_sha256"]
    assert decoded["normative_duration_completed"] is True


def test_environment_missing_configuration_is_explicitly_unsupported(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    root = _supported_root(tmp_path)
    monkeypatch.setattr(
        "purdue_rov_cv.deployment.environment.psutil.virtual_memory", lambda: type("M", (), {"available": 1024**3})()
    )
    monkeypatch.setattr("purdue_rov_cv.deployment.environment.psutil.sensors_temperatures", lambda: {})
    report = EnvironmentValidator(machine=lambda: "aarch64", which=lambda _name: None, root=root).validate(
        role="pi", config_path=tmp_path / "missing.yaml"
    )
    config_check = next(check for check in report.checks if check.check_id == "ENV-007")
    assert config_check.status is EnvironmentStatus.UNSUPPORTED
    assert not report.supported


def test_all_systemd_services_implement_phase11_policy() -> None:
    assert validate_units(ROOT / "systemd") == []


def test_configured_topology_has_one_instance_per_enabled_item(tmp_path: Path) -> None:
    unit_root = tmp_path / "systemd"
    for path in (ROOT / "systemd").iterdir():
        if path.is_file():
            (unit_root / path.name).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, unit_root / path.name)
    configure(CONFIG, unit_root, "pi")
    configure(CONFIG, unit_root, "surface")
    assert (unit_root / "purdue-cv-onboard.target.wants/purdue-cv-camera@front_camera.service").is_symlink()
    assert (unit_root / "purdue-cv-onboard.target.wants/purdue-cv-module@gate_detection.service").is_symlink()
    assert (unit_root / "purdue-cv-surface.target.wants/purdue-cv-video-receiver@front_camera.service").is_symlink()
    drop_in = unit_root / "purdue-cv-module@gate_detection.service.d/10-camera-readiness.conf"
    assert "After=purdue-cv-camera@front_camera.service" in drop_in.read_text(encoding="utf-8")

    before = {
        path.relative_to(unit_root): (path.is_symlink(), os.readlink(path) if path.is_symlink() else path.read_bytes())
        for path in unit_root.rglob("*")
        if path.is_file() or path.is_symlink()
    }
    configure(CONFIG, unit_root, "pi")
    configure(CONFIG, unit_root, "surface")
    after = {
        path.relative_to(unit_root): (path.is_symlink(), os.readlink(path) if path.is_symlink() else path.read_bytes())
        for path in unit_root.rglob("*")
        if path.is_file() or path.is_symlink()
    }
    assert after == before


def test_topology_reconciliation_removes_only_stale_managed_entries(tmp_path: Path) -> None:
    unit_root = tmp_path / "systemd"
    wants = unit_root / "purdue-cv-onboard.target.wants"
    wants.mkdir(parents=True)
    stale = wants / "purdue-cv-camera@removed_camera.service"
    stale.symlink_to("../purdue-cv-camera@.service")
    unmanaged = wants / "local-monitor.service"
    unmanaged.symlink_to("../local-monitor.service")
    drop_in = unit_root / "purdue-cv-module@removed_task.service.d/10-camera-readiness.conf"
    drop_in.parent.mkdir(parents=True)
    drop_in.write_text(
        "# Generated by scripts/configure_deployment.py\n[Unit]\nAfter=old.service\n",
        encoding="utf-8",
    )
    configure(CONFIG, unit_root, "pi")
    assert not stale.exists() and not stale.is_symlink()
    assert unmanaged.is_symlink()
    assert not drop_in.exists()


def test_memory_boundedness_uses_plateau_not_final_le_initial() -> None:
    decision = analyze_memory([(0.0, 100), (10.0, 110), (20.0, 108)], growth_tolerance_bytes=32)
    assert decision.plateau
    assert decision.final_bytes > decision.baseline_bytes
    growing = analyze_memory([(0.0, 100), (10.0, 200), (20.0, 300)], growth_tolerance_bytes=32)
    assert not growing.plateau and growing.slope_bytes_per_second > 0


def test_acceptance_matrix_is_complete_and_conservative() -> None:
    matrix = json.loads((ROOT / "config/phase11-acceptance-matrix.json").read_text(encoding="utf-8"))
    criteria = matrix["criteria"]
    assert [item["id"] for item in criteria] == list(range(1, 21))
    required = {
        "criterion",
        "owner_phase",
        "implementation",
        "verification",
        "environment",
        "evidence_artifact",
        "result",
        "notes",
    }
    assert all(required <= item.keys() for item in criteria)
    assert all(item["result"] in matrix["allowed_results"] for item in criteria)
    assert all(item["result"] == "UNVERIFIED" for item in criteria)


def test_startup_trace_has_all_ten_authoritative_stages() -> None:
    assert STAGE_NAMES == (
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


def test_startup_trace_does_not_treat_observation_as_readiness() -> None:
    assert not _application_ready(
        "preflight",
        {"state": "OBSERVED", "value": {"overall_result": "FAIL", "exit_code": 1}},
    )
    assert not _application_ready(
        "mission-enable",
        {"state": "OBSERVED", "value": {"mission_enabled": False, "mission_state": "DISABLED"}},
    )


def test_production_start_authorizer_accepts_only_fresh_matching_report(tmp_path: Path) -> None:
    config = load_config(CONFIG, environ={})
    now = datetime(2026, 9, 11, 12, 0, tzinfo=timezone.utc)
    report_path = tmp_path / "preflight.json"
    state_path = tmp_path / "mission-state.json"
    report = {
        "schema_version": 1,
        "run_id": "run-1",
        "timestamp": now.isoformat().replace("+00:00", "Z"),
        "overall_result": "PASS",
        "exit_code": 0,
        "mission_enable_decision": True,
        "configuration_sha256": config_hash(config),
        "checks": [
            {
                "check_id": f"PFL-{index:03d}",
                "status": "PASS",
                "fatal": index not in {16, 17},
            }
            for index in range(1, 21)
        ],
    }
    report_path.write_text(json.dumps(report), encoding="utf-8")

    def live():  # type: ignore[no-untyped-def]
        return (
            (ComponentHealth("live", "test", ComponentState.RUNNING),),
            ClockStatus(True, True, 0, None, None, ""),
        )

    authorizer = PreflightStartAuthorizer(
        config,
        report_path=report_path,
        state_path=state_path,
        live_health_probe=live,
        now=lambda: now,
    )
    accepted, reason = authorizer.authorize(startup_dependencies_satisfied=True)
    assert accepted and reason == "mission START authorized"
    assert json.loads(state_path.read_text(encoding="utf-8"))["mission_state"] == "ENABLED"

    stale_authorizer = PreflightStartAuthorizer(
        config,
        report_path=report_path,
        state_path=state_path,
        live_health_probe=live,
        now=lambda: now + timedelta(seconds=31),
    )
    accepted, reason = stale_authorizer.authorize(startup_dependencies_satisfied=True)
    assert not accepted and "report is fresh" in reason

    report["checks"][-1]["check_id"] = "PFL-019"
    report_path.write_text(json.dumps(report), encoding="utf-8")
    duplicate_authorizer = PreflightStartAuthorizer(
        config,
        report_path=report_path,
        state_path=state_path,
        live_health_probe=live,
        now=lambda: now,
    )
    accepted, reason = duplicate_authorizer.authorize(startup_dependencies_satisfied=True)
    assert not accepted and "canonical PFL-001 through PFL-020" in reason


def test_production_start_authorizer_rolls_back_if_state_cannot_be_persisted(tmp_path: Path) -> None:
    config = load_config(CONFIG, environ={})
    now = datetime(2026, 9, 11, 12, 0, tzinfo=timezone.utc)
    report_path = tmp_path / "preflight.json"
    report_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "run_id": "run-1",
                "timestamp": now.isoformat().replace("+00:00", "Z"),
                "overall_result": "PASS",
                "exit_code": 0,
                "mission_enable_decision": True,
                "configuration_sha256": config_hash(config),
                "checks": [
                    {
                        "check_id": f"PFL-{index:03d}",
                        "status": "PASS",
                        "fatal": index not in {16, 17},
                    }
                    for index in range(1, 21)
                ],
            }
        ),
        encoding="utf-8",
    )
    blocked_parent = tmp_path / "not-a-directory"
    blocked_parent.write_text("occupied", encoding="utf-8")

    authorizer = PreflightStartAuthorizer(
        config,
        report_path=report_path,
        state_path=blocked_parent / "mission-state.json",
        live_health_probe=lambda: (
            (ComponentHealth("live", "test", ComponentState.RUNNING),),
            ClockStatus(True, True, 0, None, None, ""),
        ),
        now=lambda: now,
    )
    accepted, reason = authorizer.authorize(startup_dependencies_satisfied=True)
    assert not accepted
    assert "could not be persisted" in reason
    assert authorizer.state is MissionState.DISABLED


def test_camera_hub_uses_measured_not_requested_duration(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    evidence = {
        "actual_duration_seconds": 10.0,
        "cameras": {"front_camera": CameraMeasurement(30, 30.0, 10.0, True, 1800.0)},
        "camera_metrics": {
            "front_camera": {
                "frames_received": 300,
                "frame_timeouts": 0,
                "pipeline_restarts": 0,
                "pipeline_restart_observed": False,
                "usb_device_present": True,
                "usb_disconnect_observed": False,
                "shared_memory_write_count": 300,
            }
        },
        "rtp_streams": {"front_camera": True},
        "correlations": {"front_camera": CorrelationMeasurement(300, 300)},
        "events": [],
    }
    monkeypatch.setattr(
        "scripts.run_phase11_hil.BrokerRuntimeEvidenceProvider",
        lambda: lambda _config, _duration: evidence,
    )
    monkeypatch.setattr(
        "scripts.run_phase11_hil._unit_status",
        lambda _unit: {"active": True, "main_pid": 10, "restart_count": 0},
    )
    output = tmp_path / "camera-hub.json"
    code = _camera_hub(Namespace(config=CONFIG, duration=1800.0, smoke=False, output=output))
    artifact = json.loads(output.read_text(encoding="utf-8"))
    assert code == 1
    assert artifact["result"] == "UNVERIFIED"
    assert artifact["measurements"]["actual_duration_seconds"] == 10.0


def test_stability_merge_rejects_nonconcurrent_host_runs(tmp_path: Path) -> None:
    config_digest = configuration_hash(CONFIG)

    def host(role: str, start: str, end: str) -> dict[str, object]:
        return {
            "kind": "host-stability",
            "start": start,
            "end": end,
            "configuration_sha256": config_digest,
            "repository": {"revision": "abc"},
            "measurements": {
                "role": role,
                "host_result": "PASS",
                "actual_duration_seconds": 3660.0,
            },
        }

    pi_path = tmp_path / "pi.json"
    surface_path = tmp_path / "surface.json"
    pi_path.write_text(
        json.dumps(host("pi", "2026-09-11T00:00:00Z", "2026-09-11T01:01:00Z")),
        encoding="utf-8",
    )
    surface_path.write_text(
        json.dumps(host("surface", "2026-09-11T02:00:00Z", "2026-09-11T03:01:00Z")),
        encoding="utf-8",
    )
    output = tmp_path / "merged.json"
    code = _stability_merge(
        Namespace(
            config=CONFIG,
            pi_artifact=pi_path,
            surface_artifact=surface_path,
            output=output,
        )
    )
    artifact = json.loads(output.read_text(encoding="utf-8"))
    assert code == 1
    assert artifact["result"] == "FAIL"
    assert "below 3600" in " ".join(artifact["measurements"]["reasons"])


def test_stability_requires_every_fresh_runtime_component_to_be_running() -> None:
    config = load_config(CONFIG, environ={})
    states = {
        "camera:front_camera": ComponentState.RUNNING,
        "module:gate_detection": ComponentState.RUNNING,
        "video_receiver:front_camera": ComponentState.RUNNING,
        "recorder": ComponentState.RUNNING,
    }
    assert all(_runtime_component_results(config, {"component_states": states}).values())
    states["module:gate_detection"] = ComponentState.ERROR
    results = _runtime_component_results(config, {"component_states": states})
    assert not results["module:gate_detection"]
    del states["recorder"]
    results = _runtime_component_results(config, {"component_states": states})
    assert not results["recorder"]


def test_camera_health_exposes_camera_hub_counters() -> None:
    metrics = RuntimeMetrics(monotonic=lambda: 0.0)
    metrics.increment("frames_received", 300)
    metrics.increment("frame_timeouts", 2)
    metrics.increment("pipeline_restarts", 1)
    metrics.increment("shared_memory_write_count", 299)
    metrics.set_gauge("frames_per_second", 29.8)
    metrics.set_gauge("current_width", 1920)
    metrics.set_gauge("current_height", 1080)
    metrics.set_gauge("usb_device_present", True)
    metrics.set_metadata("current_pixel_format", "BGR8")
    publisher = CameraHealthPublisher(
        "inproc://unused",
        "front_camera",
        interval_ms=1000,
        metrics=metrics,
        state_machine=ComponentStateMachine(),
        shutdown=ShutdownToken(),
    )
    health = publisher.health()
    assert health.source_id == "front_camera"
    assert health.camera.frames_received == 300
    assert health.camera.frames_per_second == pytest.approx(29.8)
    assert health.camera.frame_timeouts == 2
    assert health.camera.pipeline_restarts == 1
    assert health.camera.shared_memory_write_count == 299
    assert health.camera.usb_device_present


def test_health_service_invalid_configuration_uses_exit_78(tmp_path: Path) -> None:
    assert system_health_entrypoint(["--config", str(tmp_path / "missing.yaml")]) == 78
