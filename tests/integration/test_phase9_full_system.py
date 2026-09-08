"""Required §30.2 real-process simulated full-system integration."""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

import pytest

from purdue_rov_cv.preflight import (
    CheckStatus,
    ClockMonitor,
    ClockSample,
    ClockStatusService,
    ComponentHealth,
    EvidenceKind,
    MissionState,
    SimulatedClockProbe,
    SystemHealthAggregator,
)
from purdue_rov_cv.preflight.harness import Phase9ProcessHarness
from purdue_rov_cv.runtime.state import ComponentState


def test_phase9_real_process_full_system_and_five_second_shutdown(tmp_path: Path) -> None:
    harness = Phase9ProcessHarness(tmp_path)
    try:
        harness.start()
        assert harness.startup_order == [
            "network",
            "chronyd",
            "broker",
            "control_router",
            "cameras",
            "modules",
            "video_receivers",
            "recorder_operator",
            "preflight",
            "mission_enabled",
        ]
        assert {item.name for item in harness.processes} == {
            "broker",
            "control-router",
            "camera",
            "module",
            "video-receiver",
            "subscriber-primary",
            "subscriber-secondary",
            "recorder",
        }
        assert harness.preflight_report is not None
        assert len(harness.preflight_report.checks) == 20
        assert all(check.status is CheckStatus.PASS for check in harness.preflight_report.checks)
        checks = {check.check_id: check for check in harness.preflight_report.checks}
        assert checks["PFL-005"].evidence is EvidenceKind.OBSERVED
        assert checks["PFL-006"].evidence is EvidenceKind.OBSERVED
        assert checks["PFL-013"].measured["streams"]["front_camera"]
        assert checks["PFL-014"].measured["ratios"]["front_camera"] >= 0.95
        assert checks["PFL-010"].measured["duration_seconds"]["front_camera"] >= 10.0
        measured_fps_ratio = checks["PFL-011"].measured["ratios"]["front_camera"]
        assert 0.95 <= measured_fps_ratio <= 1.2
        measured_gap_ms = checks["PFL-012"].measured["maximum_gap_ms"]["front_camera"]
        assert 20.0 <= measured_gap_ms <= 500.0
        parsed = harness.marker_path.read_text(encoding="utf-8")
        second_parsed = harness.second_marker_path.read_text(encoding="utf-8")
        assert "payload_type=bounding_boxes_v1" in parsed
        assert "detections=1" in parsed
        assert "payload_type=bounding_boxes_v1" in second_parsed
        stopped, started = harness.exercise_control()
        assert stopped.succeeded and stopped.resulting_state == ComponentState.READY.value
        assert started.succeeded and started.resulting_state == ComponentState.RUNNING.value
        assert harness.gate.enabled
    except BaseException as error:
        pytest.fail(f"{error}\n{harness.log_text()}")
    finally:
        harness.shutdown()
    assert harness.shutdown_elapsed is not None and harness.shutdown_elapsed < 5.0
    assert harness.shutdown_durations and all(value < 5.0 for value in harness.shutdown_durations.values())
    assert all(item.process.poll() is not None for item in harness.processes)
    assert harness.recorded_result_count() >= 1
    assert all(harness.resource_cleanup().values())


def test_phase9_camera_disconnect_recovery_and_slow_subscriber_are_bounded(tmp_path: Path) -> None:
    harness = Phase9ProcessHarness(tmp_path)
    try:
        harness.start(disconnect_after_frames=8, slow_subscriber=True, complete_preflight=False)
        first = harness.marker_path.stat().st_mtime_ns
        deadline = time.monotonic() + 5.0
        while harness.marker_path.stat().st_mtime_ns == first and time.monotonic() < deadline:
            time.sleep(0.05)
        assert harness.marker_path.stat().st_mtime_ns > first
        assert "injected camera disconnect" in harness.log_text()
        video = harness.health_status(f"video_receiver_{harness.stream_index}")
        assert video is not None and video.video.rtp_packets_received > 0
        assert all(item.process.poll() is None for item in harness.processes)
    except BaseException as error:
        pytest.fail(f"{error}\n{harness.log_text()}")
    finally:
        harness.shutdown()


def test_phase9_clock_loss_and_module_crash_do_not_stop_unrelated_paths(tmp_path: Path) -> None:
    harness = Phase9ProcessHarness(tmp_path)
    try:
        harness.start()
        now = [100.0]
        monitor = ClockMonitor(monotonic=lambda: now[0])
        service = ClockStatusService(
            SimulatedClockProbe(
                [
                    ClockSample(True, True, 1.0, 100.0),
                    ClockSample(False, False, 25.0, 105.0),
                    ClockSample(False, False, 25.0, 110.0),
                    ClockSample(False, False, 25.0, 115.0),
                ]
            ),
            monitor,
            monotonic=lambda: now[0],
        )
        assert service.step().synchronized
        now[0] = 105.0
        assert service.step().cross_device_latency_valid
        now[0] = 110.0
        assert service.step().cross_device_latency_valid
        now[0] = 115.0
        lost = service.step()
        assert not lost.cross_device_latency_valid
        aggregator = SystemHealthAggregator(required_components={"camera"}, monotonic=lambda: now[0])
        aggregator.update(ComponentHealth("camera", "camera", ComponentState.RUNNING))
        health = aggregator.snapshot(
            clock=lost,
            preflight_completed=True,
            preflight_passed=True,
            preflight_exit_code=0,
            preflight_run_id=harness.preflight_report.run_id if harness.preflight_report else None,
        )
        harness.gate.observe_runtime(health)
        assert harness.gate.enabled and harness.gate.state is MissionState.DEGRADED
        assert not health.cross_device_latency_valid
        stopped, started = harness.exercise_control()
        assert stopped.succeeded and started.succeeded
        first = harness.marker_path.stat().st_mtime_ns
        Phase9ProcessHarness._wait_until(
            lambda: harness.marker_path.stat().st_mtime_ns > first,
            "CV processing after clock loss",
        )
        post_loss_result = dict(
            line.split("=", 1) for line in harness.marker_path.read_text(encoding="utf-8").splitlines()
        )
        assert int(post_loss_result["capture_time_unix_ns"]) > 0
        assert int(post_loss_result["publish_time_unix_ns"]) > 0
        assert int(post_loss_result["source_monotonic_ns"]) > 0
        before_video = harness.health_status(f"video_receiver_{harness.stream_index}")
        assert before_video is not None
        before_frames = before_video.video.decoded_frames
        Phase9ProcessHarness._wait_until(
            lambda: (
                (current := harness.health_status(f"video_receiver_{harness.stream_index}")) is not None
                and current.video.decoded_frames > before_frames
            ),
            "video after clock loss",
        )
        module = next(item for item in harness.processes if item.name == "module")
        module.process.kill()
        module.process.wait(timeout=5.0)
        Phase9ProcessHarness._wait_until(harness._camera_has_frame, "camera after module crash")
        assert all(item.process.poll() is None for item in harness.processes if item.name not in {"module"})
    except BaseException as error:
        pytest.fail(f"{error}\n{harness.log_text()}")
    finally:
        harness.shutdown()


def test_phase9_real_probe_detects_artifact_corruption_and_missing_module(tmp_path: Path) -> None:
    harness = Phase9ProcessHarness(tmp_path)
    try:
        harness.start()
        harness.artifact_path.write_bytes(b"corrupted model")
        corrupt = harness.run_actual_preflight()
        corrupt_checks = {check.check_id: check for check in corrupt.checks}
        assert corrupt.exit_code == 1
        assert corrupt_checks["PFL-002"].status is CheckStatus.FAIL

        harness.artifact_path.write_bytes(b"phase9 deterministic simulated model artifact\n")
        module = next(item for item in harness.processes if item.name == "module")
        module.process.kill()
        module.process.wait(timeout=5.0)
        missing = harness.run_actual_preflight()
        missing_checks = {check.check_id: check for check in missing.checks}
        assert missing.exit_code == 1
        assert missing_checks["PFL-006"].status is CheckStatus.FAIL
        assert missing_checks["PFL-019"].status is CheckStatus.FAIL
        assert missing_checks["PFL-020"].status is CheckStatus.FAIL
        assert "module:echo" in missing_checks["PFL-020"].measured["missing_required"]
        assert harness._process_alive("camera")
        assert harness._process_alive("video-receiver")
        assert harness._process_alive("broker")
        assert harness._process_alive("control-router")
    except BaseException as error:
        pytest.fail(f"{error}\n{harness.log_text()}")
    finally:
        harness.shutdown()


@pytest.mark.extended
def test_phase9_sixty_minute_memory_soak_entrypoint(tmp_path: Path) -> None:
    if os.environ.get("PURDUE_ROV_CV_RUN_60_MIN_SOAK") != "1":
        pytest.skip("set PURDUE_ROV_CV_RUN_60_MIN_SOAK=1 for the required 60-minute acceptance run")
    harness = Phase9ProcessHarness(tmp_path)
    samples: list[dict[str, object]] = []
    outcome = "FAIL"
    try:
        harness.start(disconnect_after_frames=100)
        baselines = {item.name: _rss(item.process.pid) for item in harness.processes}
        started = time.monotonic()
        deadline = started + 3_600.0
        while time.monotonic() < deadline:
            time.sleep(30.0)
            for item in harness.processes:
                assert item.process.poll() is None
            rss = {item.name: _rss(item.process.pid) for item in harness.processes}
            samples.append(
                {
                    "elapsed_seconds": time.monotonic() - started,
                    "rss_bytes": rss,
                    "all_processes_alive": True,
                }
            )
            for name, current in rss.items():
                assert current <= baselines[name] + 64 * 1024 * 1024
        outcome = "PASS"
    finally:
        harness.shutdown(raise_on_failure=False)
        (tmp_path / "phase9-60-minute-report.json").write_text(
            json.dumps(
                {
                    "duration_seconds": 3_600,
                    "maximum_growth_bytes": 64 * 1024 * 1024,
                    "outcome": outcome,
                    "samples": samples,
                    "shutdown_durations_seconds": harness.shutdown_durations,
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )


def _rss(pid: int) -> int:
    import psutil

    return int(psutil.Process(pid).memory_info().rss)
