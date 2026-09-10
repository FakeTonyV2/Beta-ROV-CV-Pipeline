"""Phase 9 clock, health, checklist, reporting, and fault coverage."""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pytest
from purdue_rov.cv.v1 import control_pb2, diagnostics_pb2

from purdue_rov_cv.camera import CaptureBackendError, SyntheticCaptureBackend
from purdue_rov_cv.cli import main as cli_main
from purdue_rov_cv.config.loader import load_config
from purdue_rov_cv.config.models import AppConfig, CameraConfig
from purdue_rov_cv.config.probes import CameraProbeResult
from purdue_rov_cv.messaging.client import ControlClient
from purdue_rov_cv.preflight import (
    CHECK_SPECS,
    BrokerRuntimeEvidenceProvider,
    CameraMeasurement,
    CheckResult,
    CheckStatus,
    ClockMonitor,
    ClockSample,
    ClockStatusService,
    ComponentHealth,
    CorrelationMeasurement,
    EvidenceKind,
    LocalSystemPreflightProbe,
    MissionEnableGate,
    MissionState,
    PreflightExitCode,
    ProbeSnapshot,
    SimulatedClockProbe,
    SimulatedPreflightProbe,
    SystemHealthAggregator,
    evaluate_checks,
    make_report,
    read_preflight_observation,
    run_preflight,
)
from purdue_rov_cv.preflight.harness import ManagedProcess, Phase9ProcessHarness, _active_interface
from purdue_rov_cv.recording.disk import GIB, DiskSpaceGuard
from purdue_rov_cv.runtime.exit_codes import ExitCode
from purdue_rov_cv.runtime.state import ComponentState

ROOT = Path(__file__).parents[2]
CONFIG = ROOT / "config" / "mission.yaml"


def _evaluated() -> tuple[AppConfig, ProbeSnapshot]:
    config = load_config(CONFIG, environ={})
    snapshot = SimulatedPreflightProbe(monotonic=lambda: 100.0, wait=lambda _seconds: None).collect(
        config, camera_duration_seconds=10.0
    )
    return config, snapshot


def _by_id(results: tuple[CheckResult, ...]) -> dict[str, CheckResult]:
    return {item.check_id: item for item in results}


def test_harness_selects_only_schema_valid_active_interface_names(tmp_path: Path) -> None:
    for name, state in {
        "br-ci-network": "up",
        "eth.100": "up",
        "eth0": "down",
        "lo": "up",
    }.items():
        candidate = tmp_path / name
        candidate.mkdir()
        (candidate / "operstate").write_text(state, encoding="ascii")
    assert _active_interface(tmp_path) == "lo"

    (tmp_path / "eth0" / "operstate").write_text("up", encoding="ascii")
    assert _active_interface(tmp_path) == "eth0"


def test_harness_readiness_wait_fails_fast_when_child_exits(tmp_path: Path) -> None:
    log_path = tmp_path / "failed.log"
    log_file = log_path.open("w+", encoding="utf-8")
    try:
        import subprocess
        import sys

        process = subprocess.Popen([sys.executable, "-c", "raise SystemExit(64)"])
        process.wait(timeout=2.0)
        managed = ManagedProcess("failed", process, log_path, log_file)
        with pytest.raises(RuntimeError, match="exited before readiness"):
            Phase9ProcessHarness._wait_until(lambda: False, "readiness", timeout=5.0, process=managed)
    finally:
        log_file.close()


def test_check_matrix_is_complete_stable_and_explanatory() -> None:
    assert [item.check_id for item in CHECK_SPECS] == [f"PFL-{number:03d}" for number in range(1, 21)]
    assert all(item.name and item.probe and item.inputs and item.calculation and item.fatal for item in CHECK_SPECS)


def test_nominal_check_results_and_report_representations_are_deterministic() -> None:
    config, snapshot = _evaluated()
    checks = evaluate_checks(config, snapshot)
    report = make_report(checks, now=datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc))
    assert len(checks) == 20
    assert all(check.passed for check in checks)
    assert report.exit_code == PreflightExitCode.PASS
    assert report.mission_enable
    assert report.timestamp == "2026-01-02T03:04:05Z"
    assert report.to_json() == report.to_json()
    assert '"schema_version": 1' in report.to_json()
    assert "[PASS] PFL-020" in report.human_summary()
    assert "Mission enable decision: ELIGIBLE" in report.human_summary()
    assert "measured:" in report.human_summary()
    assert "threshold:" in report.human_summary()
    assert all(check.status is CheckStatus.PASS for check in report.checks)
    assert _by_id(report.checks)["PFL-008"].evidence is EvidenceKind.SIMULATED


@pytest.mark.parametrize(
    ("scenario", "failed_id"),
    [
        ("invalid_model_hash", "PFL-002"),
        ("invalid_camera_mode", "PFL-009"),
        ("unsynchronized_clock", "PFL-007"),
        ("missing_component", "PFL-020"),
        ("component_error", "PFL-020"),
    ],
)
def test_explicit_fault_scenarios_disable_mission(scenario: str, failed_id: str) -> None:
    report = run_preflight(
        CONFIG,
        SimulatedPreflightProbe(scenario, monotonic=lambda: 100.0, wait=lambda _seconds: None),
        now=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    assert report.exit_code == PreflightExitCode.REQUIRED_CHECK_FAILED
    assert not report.mission_enable
    assert not _by_id(report.checks)[failed_id].passed
    assert _by_id(report.checks)[failed_id].failure_reason


def test_probe_execution_failure_is_exit_two_and_disables_mission() -> None:
    class Broken:
        def collect(self, config, *, camera_duration_seconds):  # type: ignore[no-untyped-def]
            raise RuntimeError("injected unavailable probe")

    report = run_preflight(CONFIG, Broken())
    assert report.exit_code == PreflightExitCode.COULD_NOT_EXECUTE
    assert not report.mission_enable
    assert "injected unavailable probe" in report.execution_error


def test_missing_configuration_is_exit_two(tmp_path: Path) -> None:
    report = run_preflight(
        tmp_path / "missing.yaml",
        SimulatedPreflightProbe(wait=lambda _seconds: None),
    )
    assert report.exit_code == 2
    assert not report.mission_enable
    assert "CONFIG_FILE_NOT_FOUND" in report.execution_error


def test_invalid_strict_configuration_is_a_required_check_failure(tmp_path: Path) -> None:
    invalid = tmp_path / "invalid.yaml"
    invalid.write_text(CONFIG.read_text(encoding="utf-8") + "\nunknown_phase9_field: true\n", encoding="utf-8")
    report = run_preflight(invalid, SimulatedPreflightProbe(wait=lambda _seconds: None))
    assert report.exit_code == 1
    assert report.checks[0].check_id == "PFL-001"
    assert not report.mission_enable


def test_clock_offset_boundary_freshness_and_three_failure_rule() -> None:
    now = [100.0]
    monitor = ClockMonitor(monotonic=lambda: now[0])
    valid = monitor.observe(ClockSample(True, True, 9.999, 100.0))
    assert valid.synchronized and valid.cross_device_latency_valid
    first = monitor.observe(ClockSample(False, True, 0.0, 105.0))
    second = monitor.observe(ClockSample(False, True, 0.0, 110.0))
    third = monitor.observe(ClockSample(False, True, 0.0, 115.0))
    assert not first.synchronized and first.cross_device_latency_valid
    assert not second.synchronized and second.cross_device_latency_valid
    assert not third.cross_device_latency_valid
    monitor = ClockMonitor(monotonic=lambda: now[0])
    boundary = monitor.observe(ClockSample(True, True, 10.0, 100.0))
    assert not boundary.synchronized
    assert not ClockMonitor(monotonic=lambda: 100.0).observe(ClockSample(True, True, 10.001, 100.0)).synchronized
    assert ClockMonitor(monotonic=lambda: 100.0).observe(ClockSample(True, True, -9.999, 100.0)).synchronized
    assert not ClockMonitor(monotonic=lambda: 100.0).observe(ClockSample(True, True, -10.0, 100.0)).synchronized
    monitor = ClockMonitor(monotonic=lambda: now[0])
    monitor.observe(ClockSample(True, True, -9.0, 100.0))
    now[0] = 115.0
    assert monitor.status().synchronized
    now[0] = 115.001
    assert not monitor.status().synchronized
    assert not monitor.status().cross_device_latency_valid

    now[0] = 200.0
    monitor = ClockMonitor(monotonic=lambda: now[0])
    monitor.observe(ClockSample(True, True, 0.0, 200.0))
    now[0] = 201.0
    monitor.observe(ClockSample(False, True, 0.0, 201.0))
    now[0] = 202.0
    monitor.observe(ClockSample(False, True, 0.0, 202.0))
    now[0] = 203.0
    reset = monitor.observe(ClockSample(True, True, 0.0, 203.0))
    assert reset.consecutive_failures == 0 and reset.cross_device_latency_valid


def test_clock_service_uses_five_second_monotonic_cadence() -> None:
    now = [0.0]
    probe = SimulatedClockProbe(
        [
            ClockSample(True, True, 1.0, 0.0),
            ClockSample(False, True, 1.0, 5.0),
        ]
    )
    monitor = ClockMonitor(monotonic=lambda: now[0])
    service = ClockStatusService(probe, monitor, monotonic=lambda: now[0])
    assert service.step().synchronized
    now[0] = 4.999
    assert service.step().consecutive_failures == 0
    now[0] = 5.0
    assert service.step().consecutive_failures == 1


def test_numeric_thresholds_are_exact() -> None:
    config, snapshot = _evaluated()
    camera = CameraMeasurement(100.0, 95.0, 500.0, True, 10.0)
    snapshot = replace(
        snapshot,
        cameras={"front_camera": camera},
        correlations={"front_camera": CorrelationMeasurement(100, 95)},
        average_cpu_percent=84.999,
        maximum_temperature_c=79.999,
        recording_free_bytes=10 * GIB,
        module_processed_frames={"gate_detection": 10},
    )
    checks = _by_id(evaluate_checks(config, snapshot))
    for check_id in ("PFL-011", "PFL-012", "PFL-014", "PFL-015", "PFL-016", "PFL-018", "PFL-019"):
        assert checks[check_id].passed
    failed = replace(
        snapshot,
        cameras={"front_camera": replace(camera, achieved_fps=94.999, maximum_gap_ms=500.001)},
        correlations={"front_camera": CorrelationMeasurement(100, 94)},
        average_cpu_percent=85.0,
        maximum_temperature_c=80.0,
        recording_free_bytes=10 * GIB - 1,
        module_processed_frames={"gate_detection": 9},
    )
    checks = _by_id(evaluate_checks(config, failed))
    for check_id in ("PFL-011", "PFL-012", "PFL-014", "PFL-015", "PFL-016", "PFL-018", "PFL-019"):
        assert not checks[check_id].passed


def test_mission_gate_requires_full_success_and_latches_degraded_after_clock_loss() -> None:
    clock = ClockMonitor(monotonic=lambda: 100.0).observe(ClockSample(True, True, 1.0, 100.0))
    aggregator = SystemHealthAggregator(monotonic=lambda: 100.0)
    aggregator.update(ComponentHealth("broker", "broker", ComponentState.RUNNING))
    health = aggregator.snapshot(
        clock=clock,
        preflight_completed=True,
        preflight_passed=True,
        preflight_exit_code=0,
        preflight_run_id="run-1",
    )
    gate = MissionEnableGate()
    assert gate.authorize(health, startup_dependencies_satisfied=True)
    assert gate.enabled and gate.state is MissionState.ENABLED
    lost = replace(clock, synchronized=False, cross_device_latency_valid=False, reason="clock lost")
    degraded_health = aggregator.snapshot(
        clock=lost,
        preflight_completed=True,
        preflight_passed=True,
        preflight_exit_code=0,
    )
    gate.observe_runtime(degraded_health)
    assert gate.enabled and gate.state is MissionState.DEGRADED
    assert not degraded_health.cross_device_latency_valid
    # Runtime observation never silently re-enables the latch.
    gate.observe_runtime(health)
    assert gate.enabled and gate.state is MissionState.DEGRADED


def test_required_component_error_blocks_gate() -> None:
    clock = ClockMonitor(monotonic=lambda: 1.0).observe(ClockSample(True, True, 0.0, 1.0))
    aggregator = SystemHealthAggregator(monotonic=lambda: 1.0)
    aggregator.update(ComponentHealth("camera", "camera", ComponentState.ERROR))
    health = aggregator.snapshot(
        clock=clock,
        preflight_completed=True,
        preflight_passed=True,
        preflight_exit_code=0,
    )
    gate = MissionEnableGate()
    assert not gate.authorize(health, startup_dependencies_satisfied=True)
    assert "ERROR" in gate.reason


@pytest.mark.parametrize(
    ("mutation", "startup_ready"),
    [
        ({"preflight_completed": False}, True),
        ({"preflight_passed": False, "preflight_exit_code": 1}, True),
        ({"preflight_passed": False, "preflight_exit_code": 2}, True),
        ({"preflight_run_id": None}, True),
        ({"missing_required_components": ("camera",)}, True),
        ({"stale_required_components": ("camera",)}, True),
        ({}, False),
    ],
)
def test_mission_gate_independently_requires_every_enable_prerequisite(
    mutation: dict[str, object], startup_ready: bool
) -> None:
    clock = ClockMonitor(monotonic=lambda: 1.0).observe(ClockSample(True, True, 0.0, 1.0))
    aggregator = SystemHealthAggregator(monotonic=lambda: 1.0)
    base = aggregator.snapshot(
        clock=clock,
        preflight_completed=True,
        preflight_passed=True,
        preflight_exit_code=0,
        preflight_run_id="run-1",
    )
    gate = MissionEnableGate()
    assert not gate.authorize(replace(base, **mutation), startup_dependencies_satisfied=startup_ready)
    assert not gate.enabled and gate.state is MissionState.DISABLED


@pytest.mark.parametrize("exit_code", [1, 2])
def test_failed_preflight_rerun_disables_and_invalidates_prior_success(exit_code: int) -> None:
    clock = ClockMonitor(monotonic=lambda: 1.0).observe(ClockSample(True, True, 0.0, 1.0))
    aggregator = SystemHealthAggregator(monotonic=lambda: 1.0)
    passed = aggregator.snapshot(
        clock=clock,
        preflight_completed=True,
        preflight_passed=True,
        preflight_exit_code=0,
        preflight_run_id="run-1",
    )
    gate = MissionEnableGate()
    assert gate.authorize(passed, startup_dependencies_satisfied=True)
    failed = replace(
        passed,
        preflight_passed=False,
        preflight_exit_code=exit_code,
        preflight_run_id="run-2",
    )
    assert not gate.authorize(failed, startup_dependencies_satisfied=True)
    assert not gate.enabled and gate.state is MissionState.DISABLED
    assert not gate.authorize(passed, startup_dependencies_satisfied=True)


def test_invalid_clock_independently_blocks_initial_mission_enable() -> None:
    clock = ClockMonitor(monotonic=lambda: 1.0).observe(ClockSample(True, True, 0.0, 1.0))
    health = SystemHealthAggregator(monotonic=lambda: 1.0).snapshot(
        clock=replace(clock, synchronized=False, cross_device_latency_valid=False, reason="clock invalid"),
        preflight_completed=True,
        preflight_passed=True,
        preflight_exit_code=0,
        preflight_run_id="run-1",
    )
    gate = MissionEnableGate()
    assert not gate.authorize(health, startup_dependencies_satisfied=True)
    assert not gate.enabled and gate.state is MissionState.DISABLED


def test_control_timeout_uses_canonical_outcome_unknown_semantics() -> None:
    client = ControlClient(
        "tcp://127.0.0.1:1",
        acknowledgement_timeout_seconds=0.01,
    )
    request = control_pb2.CommandRequest(
        command_id=uuid4().bytes,
        target_id="echo",
        issued_time_unix_ns=1,
    )
    request.get_status.SetInParent()
    try:
        response = client.send_command(request)
    finally:
        client.close()
    assert response.status == control_pb2.COMMAND_STATUS_OUTCOME_UNKNOWN
    assert response.error_code == "COMMAND_OUTCOME_UNKNOWN"
    assert client.send_attempts == 1


def test_synthetic_camera_disconnect_hook_preserves_service_owned_numbering() -> None:
    backend = SyntheticCaptureBackend(
        2,
        2,
        10,
        disconnect_after_frames=1,
        monotonic=lambda: 0.0,
        monotonic_ns=lambda: 1,
        time_ns=lambda: 2,
        sleep=lambda _seconds: None,
    )
    backend.start()
    frame = backend.poll(0.1)
    assert frame is not None and frame.frame_number is None
    with pytest.raises(CaptureBackendError, match="injected camera disconnect"):
        backend.poll(0.1)


def test_unavailable_probe_keeps_independent_results_and_returns_exit_two() -> None:
    config, snapshot = _evaluated()
    snapshot = replace(snapshot, unavailable_checks={"PFL-005": "broker probe infrastructure unavailable"})
    report = make_report(evaluate_checks(config, snapshot))
    checks = _by_id(report.checks)
    assert report.exit_code == PreflightExitCode.COULD_NOT_EXECUTE
    assert checks["PFL-005"].status is CheckStatus.UNAVAILABLE
    assert checks["PFL-020"].status is CheckStatus.PASS
    assert "[UNAVAILABLE] PFL-005" in report.human_summary()


def test_health_aggregator_rejects_missing_and_stale_required_components() -> None:
    now = [10.0]
    clock = ClockMonitor(monotonic=lambda: now[0]).observe(ClockSample(True, True, 0.0, 10.0))
    aggregator = SystemHealthAggregator(required_components={"broker", "camera"}, monotonic=lambda: now[0])
    aggregator.update(ComponentHealth("broker", "broker", ComponentState.RUNNING))
    missing = aggregator.snapshot(
        clock=clock,
        preflight_completed=True,
        preflight_passed=True,
        preflight_exit_code=0,
    )
    assert missing.missing_required_components == ("camera",)
    now[0] = 13.001
    stale = aggregator.snapshot(
        clock=clock,
        preflight_completed=True,
        preflight_passed=True,
        preflight_exit_code=0,
    )
    assert stale.stale_required_components == ("broker",)


def test_stale_success_cannot_reauthorize_after_runtime_failure() -> None:
    now = [20.0]
    valid = ClockMonitor(monotonic=lambda: now[0]).observe(ClockSample(True, True, 0.0, 20.0))
    aggregator = SystemHealthAggregator(required_components={"broker"}, monotonic=lambda: now[0])
    aggregator.update(ComponentHealth("broker", "broker", ComponentState.RUNNING))
    health = aggregator.snapshot(
        clock=valid,
        preflight_completed=True,
        preflight_passed=True,
        preflight_exit_code=0,
        preflight_run_id="run-1",
    )
    gate = MissionEnableGate()
    assert gate.authorize(health, startup_dependencies_satisfied=True)
    lost = replace(valid, synchronized=False, cross_device_latency_valid=False, reason="clock lost")
    gate.observe_runtime(
        aggregator.snapshot(
            clock=lost,
            preflight_completed=True,
            preflight_passed=True,
            preflight_exit_code=0,
            preflight_run_id="run-1",
        )
    )
    assert gate.enabled and gate.state is MissionState.DEGRADED
    assert not gate.authorize(health, startup_dependencies_satisfied=True)
    assert gate.authorize(replace(health, preflight_run_id="run-2"), startup_dependencies_satisfied=True)


def test_json_write_is_atomic_and_operator_does_not_infer_mission_state(tmp_path: Path) -> None:
    config, snapshot = _evaluated()
    report = make_report(
        evaluate_checks(config, snapshot),
        now=datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc),
    )
    path = tmp_path / "preflight.json"
    path.write_text("prior", encoding="utf-8")
    report.write_json(path)
    assert path.read_text(encoding="utf-8") == report.to_json()
    observation = read_preflight_observation(path)
    assert observation.available and observation.mission_eligible
    forged = report.as_dict()
    forged["exit_code"] = 1
    path.write_text(json.dumps(forged), encoding="utf-8")
    assert not read_preflight_observation(path).available


def test_local_probe_without_runtime_provider_reports_unavailable_not_false_pass() -> None:
    class IsolatedLocalProbe(LocalSystemPreflightProbe):
        _broker_round_trip = staticmethod(lambda _config: False)
        _control_status = staticmethod(lambda _config: {})
        _surface_responds = staticmethod(lambda _address: True)

    report = run_preflight(CONFIG, IsolatedLocalProbe())
    checks = _by_id(report.checks)
    assert len(checks) == 20
    assert report.exit_code == PreflightExitCode.COULD_NOT_EXECUTE
    assert checks["PFL-010"].status is CheckStatus.UNAVAILABLE
    assert checks["PFL-020"].status is CheckStatus.UNAVAILABLE


def test_invalid_camera_mode_crosses_the_production_hardware_probe_boundary() -> None:
    config, snapshot = _evaluated()

    class RejectingHardwareProbe:
        def probe_camera(self, camera_id: str, camera: CameraConfig) -> CameraProbeResult:
            del camera_id, camera
            return CameraProbeResult(True, True, True, False, "injected unsupported capture tuple")

        def validate_runtime_and_artifact(self, config: AppConfig):  # type: ignore[no-untyped-def]
            del config
            return ()

        def validate_port_availability(self, config: AppConfig):  # type: ignore[no-untyped-def]
            del config
            return ()

    class IsolatedLocalProbe(LocalSystemPreflightProbe):
        _hashes = staticmethod(lambda _config: snapshot.model_hashes)
        _interface_up = staticmethod(lambda _name: True)
        _surface_responds = staticmethod(lambda _address: True)
        _broker_round_trip = staticmethod(lambda _config: True)
        _control_status = staticmethod(lambda _config: snapshot.control_status)

    def runtime_evidence(_config: AppConfig, _duration: float) -> dict[str, object]:
        return {
            "cameras": snapshot.cameras,
            "component_states": snapshot.component_states,
            "rtp_streams": snapshot.rtp_streams,
            "correlations": snapshot.correlations,
            "module_processed_frames": snapshot.module_processed_frames,
            "average_cpu_percent": snapshot.average_cpu_percent,
            "maximum_temperature_c": snapshot.maximum_temperature_c,
            "thermally_throttled": snapshot.thermally_throttled,
        }

    report = run_preflight(
        CONFIG,
        IsolatedLocalProbe(
            clock_probe=SimulatedClockProbe([snapshot.clock]),
            hardware_probe=RejectingHardwareProbe(),
            disk_guard=DiskSpaceGuard(lambda _path: 20 * GIB),
            runtime_evidence=runtime_evidence,
            monotonic=lambda: 100.0,
        ),
    )
    checks = _by_id(report.checks)
    assert report.exit_code == PreflightExitCode.REQUIRED_CHECK_FAILED
    assert checks["PFL-008"].status is CheckStatus.PASS
    assert checks["PFL-009"].status is CheckStatus.FAIL
    assert checks["PFL-009"].evidence is EvidenceKind.HARDWARE_VERIFIED


def test_runtime_video_evidence_uses_current_window_counter_deltas() -> None:
    first = diagnostics_pb2.DiagnosticStatus(source_id="video_receiver_0")
    first.video.rtp_packets_received = 100
    first.video.decoded_frames = 80
    first.video.frame_index_hits = 76
    unchanged = diagnostics_pb2.DiagnosticStatus.FromString(first.SerializeToString())
    assert BrokerRuntimeEvidenceProvider._video_counter_deltas(first, unchanged) == (0, 0, 0)

    latest = diagnostics_pb2.DiagnosticStatus.FromString(first.SerializeToString())
    latest.video.rtp_packets_received = 140
    latest.video.decoded_frames = 100
    latest.video.frame_index_hits = 95
    assert BrokerRuntimeEvidenceProvider._video_counter_deltas(first, latest) == (40, 20, 19)

    reset = diagnostics_pb2.DiagnosticStatus(source_id="video_receiver_0")
    reset.video.rtp_packets_received = 2
    reset.video.decoded_frames = 1
    reset.video.frame_index_hits = 1
    assert BrokerRuntimeEvidenceProvider._video_counter_deltas(first, reset) == (0, 0, 0)


@pytest.mark.parametrize(
    "arguments",
    [
        ["preflight", str(CONFIG), "--simulate", "--camera-duration", "9.999"],
        ["preflight", str(CONFIG), "--scenario", "invalid_model_hash"],
    ],
)
def test_preflight_cli_argument_errors_preserve_exit_64(arguments: list[str]) -> None:
    with pytest.raises(SystemExit) as caught:
        cli_main(arguments)
    assert caught.value.code == ExitCode.INVALID_ARGUMENTS
