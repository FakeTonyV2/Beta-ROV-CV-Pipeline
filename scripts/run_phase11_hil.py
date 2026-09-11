#!/usr/bin/env python3
"""Execute explicit Phase 11 HIL runs and persist honest JSON evidence."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psutil

from purdue_rov_cv.config.loader import load_config
from purdue_rov_cv.config.models import AppConfig
from purdue_rov_cv.deployment.evidence import (
    analyze_memory,
    artifact_envelope,
    configuration_hash,
    utc_now,
    write_artifact,
)
from purdue_rov_cv.preflight.probes import BrokerRuntimeEvidenceProvider
from purdue_rov_cv.runtime.state import ComponentState

ROOT = Path(__file__).parents[1]


def _runtime_component_results(config: AppConfig, runtime: dict[str, Any]) -> dict[str, bool]:
    states = runtime.get("component_states", {})
    state_mapping = states if isinstance(states, dict) else {}
    required = {
        *{f"camera:{camera_id}" for camera_id in config.cameras},
        *{f"module:{task_id}" for task_id, task in config.tasks.items() if task.enabled},
        *{f"video_receiver:{camera_id}" for camera_id, camera in config.cameras.items() if camera.stream_to_surface},
    }
    if config.recording.enabled:
        required.add("recorder")
    return {component_id: state_mapping.get(component_id) == ComponentState.RUNNING for component_id in required}


def _active(unit: str) -> bool:
    return _unit_status(unit)["active"]


def _unit_status(unit: str) -> dict[str, Any]:
    result = subprocess.run(
        [
            "systemctl",
            "show",
            unit,
            "--property=ActiveState,SubState,MainPID,NRestarts,ExecMainStatus",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    values = dict(line.split("=", 1) for line in result.stdout.splitlines() if "=" in line)
    pid = int(values.get("MainPID", "0")) if values.get("MainPID", "0").isdigit() else 0
    restarts = int(values.get("NRestarts", "0")) if values.get("NRestarts", "0").isdigit() else 0
    return {
        "active": result.returncode == 0 and values.get("ActiveState") == "active",
        "active_state": values.get("ActiveState", "unknown"),
        "sub_state": values.get("SubState", "unknown"),
        "main_pid": pid,
        "restart_count": restarts,
        "exec_main_status": values.get("ExecMainStatus", "unknown"),
        "query_returncode": result.returncode,
    }


def _service_inventory(config_path: Path, role: str) -> list[str]:
    config = load_config(config_path, environ={})
    if role == "pi":
        return [
            "purdue-cv-broker.service",
            "purdue-cv-control-router.service",
            *[f"purdue-cv-camera@{camera_id}.service" for camera_id in sorted(config.cameras)],
            *[f"purdue-cv-module@{task_id}.service" for task_id, task in sorted(config.tasks.items()) if task.enabled],
            "purdue-cv-system-health.service",
        ]
    units = [
        *[
            f"purdue-cv-video-receiver@{camera_id}.service"
            for camera_id, camera in sorted(config.cameras.items())
            if camera.stream_to_surface
        ],
        "purdue-cv-surface-health.service",
    ]
    if config.recording.enabled:
        units.insert(-1, "purdue-cv-recorder.service")
    return units


def _recording_snapshot(directory: Path | None) -> dict[str, int] | None:
    if directory is None:
        return None
    try:
        files = [path for path in directory.rglob("*") if path.is_file()]
        return {
            "file_count": len(files),
            "total_bytes": sum(path.stat().st_size for path in files),
        }
    except OSError:
        return None


def _health_snapshot(path: Path | None) -> dict[str, Any] | None:
    if path is None:
        return None
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        timestamp = datetime.fromisoformat(value["timestamp"].replace("Z", "+00:00"))
        age = (datetime.now(timezone.utc) - timestamp).total_seconds()
    except (OSError, json.JSONDecodeError, KeyError, TypeError, ValueError):
        return None
    if not isinstance(value, dict):
        return None
    value["fresh"] = -5.0 <= age <= 15.0
    return value


def _samples(
    duration: float,
    cadence: float,
    units: list[str],
    *,
    health_path: Path | None = None,
    recording_directory: Path | None = None,
) -> tuple[list[dict[str, Any]], dict[str, list[tuple[float, int]]]]:
    started = time.monotonic()
    samples: list[dict[str, Any]] = []
    rss: dict[str, list[tuple[float, int]]] = {unit: [] for unit in units}
    while True:
        now = time.monotonic()
        elapsed = now - started
        details = {unit: _unit_status(unit) for unit in units}
        states = {unit: bool(detail["active"]) for unit, detail in details.items()}
        for unit in units:
            pid = details[unit]["main_pid"]
            if pid:
                try:
                    rss[unit].append((elapsed, int(psutil.Process(pid).memory_info().rss)))
                except (psutil.Error, OSError):
                    pass
        samples.append(
            {
                "elapsed_seconds": elapsed,
                "services": states,
                "service_details": details,
                "available_memory_bytes": int(psutil.virtual_memory().available),
                "root_free_bytes": int(psutil.disk_usage("/").free),
                "cpu_percent": float(psutil.cpu_percent(interval=None)),
                "health": _health_snapshot(health_path),
                "recording": _recording_snapshot(recording_directory),
            }
        )
        if elapsed >= duration:
            return samples, rss
        time.sleep(min(cadence, duration - elapsed))


def _camera_hub(args: argparse.Namespace) -> int:
    if args.duration < 1800 and not args.smoke:
        raise SystemExit("camera-hub duration must be >=1800 seconds; use --smoke for a non-normative developer run")
    started = utc_now()
    config = load_config(args.config, environ={})
    units = [f"purdue-cv-camera@{camera_id}.service" for camera_id in sorted(config.cameras)]
    systemd_before = {unit: _unit_status(unit) for unit in units}
    evidence = BrokerRuntimeEvidenceProvider()(config, args.duration)
    systemd_after = {unit: _unit_status(unit) for unit in units}
    actual_duration = float(evidence.get("actual_duration_seconds", 0.0))
    normative = actual_duration >= 1800 and not args.smoke
    camera_metrics = evidence.get("camera_metrics", {})
    cameras: dict[str, Any] = {}
    passed = True
    for camera_id, camera in config.cameras.items():
        measurement = evidence["cameras"][camera_id]
        metrics = camera_metrics.get(camera_id, {}) if isinstance(camera_metrics, dict) else {}
        timeout_rate = (
            float(metrics.get("frame_timeouts", 0)) / (actual_duration / 600.0)
            if actual_duration > 0 and metrics
            else None
        )
        unit = f"purdue-cv-camera@{camera_id}.service"
        no_systemd_restart = (
            systemd_before[unit]["active"]
            and systemd_after[unit]["active"]
            and systemd_before[unit]["main_pid"] == systemd_after[unit]["main_pid"]
            and systemd_before[unit]["restart_count"] == systemd_after[unit]["restart_count"]
        )
        rtp_valid = not camera.stream_to_surface or evidence.get("rtp_streams", {}).get(camera_id) is True
        correlation = (
            evidence.get("correlations", {}).get(camera_id).ratio
            if camera_id in evidence.get("correlations", {})
            else None
        )
        correlation_valid = not camera.stream_to_surface or (correlation is not None and correlation >= 0.95)
        result = (
            measurement.opened
            and measurement.simultaneous_seconds >= 1800
            and measurement.achieved_fps >= 0.95 * camera.frame_rate
            and timeout_rate is not None
            and timeout_rate < 1.0
            and bool(metrics.get("usb_device_present", False))
            and not bool(metrics.get("usb_disconnect_observed", True))
            and int(metrics.get("pipeline_restarts", 1)) == 0
            and not bool(metrics.get("pipeline_restart_observed", True))
            and no_systemd_restart
            and rtp_valid
            and correlation_valid
        )
        passed = passed and result
        cameras[camera_id] = {
            "configured_fps": camera.frame_rate,
            "measured_fps": measurement.achieved_fps,
            "frames_received": metrics.get("frames_received"),
            "frame_timeouts": metrics.get("frame_timeouts"),
            "frame_timeouts_per_10_minutes": timeout_rate,
            "pipeline_restarts": metrics.get("pipeline_restarts"),
            "pipeline_restart_observed": metrics.get("pipeline_restart_observed"),
            "usb_present": metrics.get("usb_device_present"),
            "usb_disconnect_observed": metrics.get("usb_disconnect_observed"),
            "systemd_no_restart": no_systemd_restart,
            "shared_memory_writes": metrics.get("shared_memory_write_count"),
            "rtp_status": evidence.get("rtp_streams", {}).get(camera_id),
            "frame_index_status": correlation,
            "result": "PASS" if result else "FAIL",
        }
    result = "PASS" if normative and passed else "UNVERIFIED" if not normative else "FAIL"
    artifact = artifact_envelope(
        kind="camera-hub-hil",
        root=ROOT,
        config_path=args.config,
        started_at=started,
        ended_at=utc_now(),
        measurements={
            "actual_duration_seconds": actual_duration,
            "cameras": cameras,
            "systemd_before": systemd_before,
            "systemd_after": systemd_after,
            "maximum_temperature_c": evidence.get("maximum_temperature_c"),
            "thermally_throttled": evidence.get("thermally_throttled"),
        },
        events=list(evidence.get("events", [])),
        result=result,
        normative=normative,
    )
    write_artifact(args.output, artifact)
    print(args.output)
    return 0 if result == "PASS" else 1


def _stability(args: argparse.Namespace) -> int:
    if args.duration < 3600 and not args.smoke:
        raise SystemExit("stability duration must be >=3600 seconds; use --smoke for a non-normative developer run")
    started = utc_now()
    config = load_config(args.config, environ={})
    units = _service_inventory(args.config, args.role)
    health_path = Path(f"/run/purdue-rov-cv/{'system' if args.role == 'pi' else 'surface'}-health.json")
    recording_directory = config.recording.directory if args.role == "surface" and config.recording.enabled else None
    runtime: dict[str, Any] = {}
    if args.role == "pi":
        with ThreadPoolExecutor(max_workers=2) as executor:
            sample_future = executor.submit(
                _samples,
                args.duration,
                args.cadence,
                units,
                health_path=health_path,
                recording_directory=recording_directory,
            )
            runtime_future = executor.submit(BrokerRuntimeEvidenceProvider(), config, args.duration)
            samples, rss = sample_future.result()
            runtime = runtime_future.result()
    else:
        samples, rss = _samples(
            args.duration,
            args.cadence,
            units,
            health_path=health_path,
            recording_directory=recording_directory,
        )
    memory = {unit: asdict(analyze_memory(values)) for unit, values in rss.items() if len(values) >= 2}
    actual_duration = float(samples[-1]["elapsed_seconds"]) if samples else 0.0
    normative = actual_duration >= 3600 and not args.smoke
    survived = all(all(sample["services"].values()) for sample in samples)
    no_restarts = all(
        len({sample["service_details"][unit]["main_pid"] for sample in samples}) == 1
        and len({sample["service_details"][unit]["restart_count"] for sample in samples}) == 1
        for unit in units
    )
    plateau = len(memory) == len(units) and all(value["plateau"] for value in memory.values())
    health_valid = all(
        isinstance(sample.get("health"), dict)
        and sample["health"].get("fresh") is True
        and isinstance(sample["health"].get("clock"), dict)
        and sample["health"]["clock"].get("synchronized") is True
        for sample in samples
    )
    recording_valid = True
    if recording_directory is not None:
        recording_samples = [sample["recording"] for sample in samples if isinstance(sample.get("recording"), dict)]
        recording_valid = (
            len(recording_samples) == len(samples)
            and recording_samples[-1]["file_count"] > 0
            and recording_samples[-1]["total_bytes"] > recording_samples[0]["total_bytes"]
        )
    runtime_summary: dict[str, Any] = {}
    runtime_valid = True
    if args.role == "pi":
        runtime_duration = float(runtime.get("actual_duration_seconds", 0.0))
        camera_metrics = runtime.get("camera_metrics", {})
        camera_results: dict[str, bool] = {}
        for camera_id, camera in config.cameras.items():
            measurement = runtime.get("cameras", {}).get(camera_id)
            metrics = camera_metrics.get(camera_id, {}) if isinstance(camera_metrics, dict) else {}
            timeout_rate = (
                float(metrics.get("frame_timeouts", 0)) / (runtime_duration / 600.0)
                if runtime_duration > 0 and metrics
                else None
            )
            camera_results[camera_id] = bool(
                measurement is not None
                and measurement.simultaneous_seconds >= 3600
                and measurement.achieved_fps >= 0.95 * camera.frame_rate
                and timeout_rate is not None
                and timeout_rate < 1.0
                and metrics.get("usb_device_present") is True
                and metrics.get("usb_disconnect_observed") is False
                and metrics.get("pipeline_restarts") == 0
                and metrics.get("pipeline_restart_observed") is False
            )
        module_metrics = runtime.get("module_metrics", {})
        module_results = {
            task_id: (
                isinstance(module_metrics.get(task_id), dict)
                and module_metrics[task_id].get("frames_processed", 0) > 0
                and module_metrics[task_id].get("processing_exceptions") == 0
                and module_metrics[task_id].get("results_dropped_local_queue") == 0
                and module_metrics[task_id].get("zmq_send_dropped") == 0
            )
            for task_id, task in config.tasks.items()
            if task.enabled
        }
        video_metrics = runtime.get("video_metrics", {})
        video_results: dict[str, bool] = {}
        for camera_id, camera in config.cameras.items():
            if not camera.stream_to_surface:
                continue
            metrics = video_metrics.get(camera_id, {}) if isinstance(video_metrics, dict) else {}
            received = int(metrics.get("rtp_packets_received", 0))
            lost = int(metrics.get("rtp_packets_lost", 0))
            correlation = runtime.get("correlations", {}).get(camera_id)
            video_results[camera_id] = bool(
                runtime.get("rtp_streams", {}).get(camera_id) is True
                and received > 0
                and lost / max(1, received + lost) < 0.01
                and metrics.get("stream_restarts") == 0
                and correlation is not None
                and correlation.ratio >= 0.95
            )
        messaging_metrics = runtime.get("messaging_metrics", {})
        messaging_valid = bool(messaging_metrics) and all(
            metrics.get("invalid_messages") == 0
            and metrics.get("unknown_payload_types") == 0
            and metrics.get("observed_sequence_gaps") == 0
            for metrics in messaging_metrics.values()
            if isinstance(metrics, dict)
        )
        component_results = _runtime_component_results(config, runtime)
        runtime_valid = bool(
            runtime_duration >= 3600
            and camera_results
            and all(camera_results.values())
            and module_results
            and all(module_results.values())
            and all(video_results.values())
            and messaging_valid
            and component_results
            and all(component_results.values())
        )
        runtime_summary = {
            "actual_duration_seconds": runtime_duration,
            "camera_results": camera_results,
            "camera_metrics": camera_metrics,
            "module_results": module_results,
            "module_metrics": module_metrics,
            "video_results": video_results,
            "video_metrics": video_metrics,
            "messaging_metrics": messaging_metrics,
            "component_results": component_results,
            "component_states": runtime.get("component_states", {}),
            "events": runtime.get("events", []),
        }
    host_passed = (
        normative and survived and no_restarts and plateau and health_valid and recording_valid and runtime_valid
    )
    artifact = artifact_envelope(
        kind="host-stability",
        root=ROOT,
        config_path=args.config,
        started_at=started,
        ended_at=utc_now(),
        measurements={
            "role": args.role,
            "actual_duration_seconds": actual_duration,
            "samples": samples,
            "rss": memory,
            "no_service_restarts": no_restarts,
            "health_valid": health_valid,
            "recording_valid": recording_valid,
            "runtime_valid": runtime_valid,
            "runtime": runtime_summary,
            "host_result": "PASS" if host_passed else "FAIL",
        },
        events=[],
        result="UNVERIFIED",
        normative=normative,
    )
    write_artifact(args.output, artifact)
    print(args.output)
    return 0 if host_passed else 1


def _stability_merge(args: argparse.Namespace) -> int:
    started = utc_now()
    artifacts = [json.loads(path.read_text(encoding="utf-8")) for path in (args.pi_artifact, args.surface_artifact)]
    reasons: list[str] = []
    by_role = {
        artifact.get("measurements", {}).get("role"): artifact
        for artifact in artifacts
        if isinstance(artifact, dict) and isinstance(artifact.get("measurements"), dict)
    }
    if set(by_role) != {"pi", "surface"}:
        reasons.append("one Pi and one surface host artifact are required")
    for role, artifact in by_role.items():
        if artifact.get("kind") != "host-stability":
            reasons.append(f"{role} artifact kind is not host-stability")
        measurements = artifact.get("measurements", {})
        if measurements.get("host_result") != "PASS":
            reasons.append(f"{role} host result is not PASS")
        if float(measurements.get("actual_duration_seconds", 0.0)) < 3600:
            reasons.append(f"{role} duration is below 3600 seconds")
    if len(by_role) == 2:
        pi = by_role["pi"]
        surface = by_role["surface"]
        if pi.get("configuration_sha256") != surface.get("configuration_sha256"):
            reasons.append("host configuration hashes differ")
        if pi.get("configuration_sha256") != configuration_hash(args.config):
            reasons.append("host configuration hash differs from the merge configuration")
        if pi.get("repository", {}).get("revision") != surface.get("repository", {}).get("revision"):
            reasons.append("host repository revisions differ")
        try:
            overlap = (
                min(
                    datetime.fromisoformat(pi["end"].replace("Z", "+00:00")),
                    datetime.fromisoformat(surface["end"].replace("Z", "+00:00")),
                )
                - max(
                    datetime.fromisoformat(pi["start"].replace("Z", "+00:00")),
                    datetime.fromisoformat(surface["start"].replace("Z", "+00:00")),
                )
            ).total_seconds()
        except (KeyError, TypeError, ValueError):
            overlap = 0.0
        if overlap < 3600:
            reasons.append(f"concurrent overlap is {overlap:.3f} seconds, below 3600")
    else:
        overlap = 0.0
    passed = not reasons
    artifact = artifact_envelope(
        kind="full-system-stability",
        root=ROOT,
        config_path=args.config,
        started_at=started,
        ended_at=utc_now(),
        measurements={
            "concurrent_overlap_seconds": overlap,
            "host_artifacts": [str(args.pi_artifact), str(args.surface_artifact)],
            "reasons": reasons,
        },
        events=[],
        result="PASS" if passed else "FAIL",
        normative=True,
    )
    write_artifact(args.output, artifact)
    print(args.output)
    return 0 if passed else 1


def _clock_loss(args: argparse.Namespace) -> int:
    if not args.allow_disruptive_clock_test:
        raise SystemExit("clock-loss requires --allow-disruptive-clock-test")
    state_path = Path(f"/run/purdue-rov-cv/{'system' if args.role == 'pi' else 'surface'}-health.json")
    started = utc_now()
    events: list[dict[str, Any]] = []
    invalidated_after: float | None = None
    recovered_after: float | None = None
    start = time.monotonic()
    units = _service_inventory(args.config, args.role)
    service_before = {unit: _unit_status(unit) for unit in units}
    continuity_samples: list[dict[str, Any]] = []
    try:
        subprocess.run(["systemctl", "stop", "chrony.service"], check=True)
        events.append({"elapsed_seconds": time.monotonic() - start, "event": "chrony stopped"})
        deadline = start + 20.0
        while time.monotonic() < deadline:
            continuity_samples.append({unit: _unit_status(unit) for unit in units})
            if state_path.exists():
                state = json.loads(state_path.read_text(encoding="utf-8"))
                if not state["clock"]["cross_device_latency_valid"]:
                    invalidated_after = time.monotonic() - start
                    break
            time.sleep(1.0)
    finally:
        subprocess.run(["systemctl", "start", "chrony.service"], check=False)
        events.append({"elapsed_seconds": time.monotonic() - start, "event": "chrony restore requested"})
    deadline = time.monotonic() + 30.0
    while time.monotonic() < deadline:
        continuity_samples.append({unit: _unit_status(unit) for unit in units})
        if state_path.exists():
            state = json.loads(state_path.read_text(encoding="utf-8"))
            if state["clock"]["synchronized"] and state["clock"]["consecutive_failures"] == 0:
                recovered_after = time.monotonic() - start
                break
        time.sleep(1.0)
    service_after = {unit: _unit_status(unit) for unit in units}
    continuity = all(
        sample[unit]["active"]
        and sample[unit]["main_pid"] == service_before[unit]["main_pid"]
        and sample[unit]["restart_count"] == service_before[unit]["restart_count"]
        for sample in continuity_samples
        for unit in units
    )
    chrony_restored = _active("chrony.service")
    passed = (
        invalidated_after is not None
        and invalidated_after <= 15.0
        and recovered_after is not None
        and continuity
        and chrony_restored
    )
    artifact = artifact_envelope(
        kind="clock-loss-recovery-hil",
        root=ROOT,
        config_path=args.config,
        started_at=started,
        ended_at=utc_now(),
        measurements={
            "latency_invalidated_after_seconds": invalidated_after,
            "recovered_after_seconds": recovered_after,
            "cv_video_control_continuity": continuity,
            "chrony_restored": chrony_restored,
            "service_before": service_before,
            "service_after": service_after,
            "continuity_samples": continuity_samples,
        },
        events=events,
        result="PASS" if passed else "FAIL",
        normative=True,
    )
    write_artifact(args.output, artifact)
    print(args.output)
    return 0 if passed else 1


def main() -> int:
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    camera = commands.add_parser("camera-hub")
    camera.add_argument("--config", type=Path, default=Path("/etc/purdue-rov-cv/mission.yaml"))
    camera.add_argument("--duration", type=float, default=1800.0)
    camera.add_argument("--smoke", action="store_true")
    camera.add_argument("--output", type=Path, required=True)
    camera.set_defaults(handler=_camera_hub)
    stability = commands.add_parser("stability")
    stability.add_argument("--config", type=Path, default=Path("/etc/purdue-rov-cv/mission.yaml"))
    stability.add_argument("--role", choices=("pi", "surface"), required=True)
    stability.add_argument("--duration", type=float, default=3660.0)
    stability.add_argument("--cadence", type=float, default=30.0)
    stability.add_argument("--smoke", action="store_true")
    stability.add_argument("--output", type=Path, required=True)
    stability.set_defaults(handler=_stability)
    merge = commands.add_parser("stability-merge")
    merge.add_argument("--config", type=Path, default=Path("/etc/purdue-rov-cv/mission.yaml"))
    merge.add_argument("--pi-artifact", type=Path, required=True)
    merge.add_argument("--surface-artifact", type=Path, required=True)
    merge.add_argument("--output", type=Path, required=True)
    merge.set_defaults(handler=_stability_merge)
    clock = commands.add_parser("clock-loss")
    clock.add_argument("--config", type=Path, default=Path("/etc/purdue-rov-cv/mission.yaml"))
    clock.add_argument("--allow-disruptive-clock-test", action="store_true")
    clock.add_argument("--role", choices=("pi", "surface"), required=True)
    clock.add_argument("--output", type=Path, required=True)
    clock.set_defaults(handler=_clock_loss)
    args = parser.parse_args()
    if os.name != "posix":
        raise SystemExit("Phase 11 HIL commands require the deployed Linux hosts")
    return args.handler(args)


if __name__ == "__main__":
    raise SystemExit(main())
