"""The explicit §29.1 preflight check matrix and result model."""

from __future__ import annotations

import hashlib
import json
import math
import os
import tempfile
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timezone
from enum import IntEnum, StrEnum
from pathlib import Path
from typing import Any

from purdue_rov_cv.config.models import AppConfig
from purdue_rov_cv.recording.disk import GIB
from purdue_rov_cv.runtime.state import ComponentState

from .clock import ClockMonitor, ClockSample


class PreflightExitCode(IntEnum):
    PASS = 0
    REQUIRED_CHECK_FAILED = 1
    COULD_NOT_EXECUTE = 2


class CheckStatus(StrEnum):
    PASS = "PASS"
    WARNING = "WARNING"
    FAIL = "FAIL"
    UNAVAILABLE = "UNAVAILABLE"


class EvidenceKind(StrEnum):
    OBSERVED = "OBSERVED"
    SIMULATED = "SIMULATED"
    HARDWARE_VERIFIED = "HARDWARE_VERIFIED"


@dataclass(frozen=True, slots=True)
class CheckSpec:
    check_id: str
    name: str
    probe: str
    inputs: str
    calculation: str
    fatal: bool = True


CHECK_SPECS = (
    CheckSpec(
        "PFL-001",
        "Configuration is strict and valid",
        "configuration loader",
        "mission YAML",
        "strict parse succeeds; unknown fields are forbidden",
    ),
    CheckSpec(
        "PFL-002",
        "Model hashes match",
        "artifact SHA-256 probe",
        "enabled task artifacts",
        "every measured SHA-256 equals its configured lowercase digest",
    ),
    CheckSpec("PFL-003", "Tether interface is up", "network link probe", "network.tether_interface", "operstate is UP"),
    CheckSpec(
        "PFL-004",
        "Surface IP responds",
        "surface reachability probe",
        "network.surface_ip",
        "one bounded reachability request succeeds",
    ),
    CheckSpec(
        "PFL-005",
        "Broker round trip succeeds",
        "broker PUB/SUB probe",
        "configured broker endpoints",
        "a uniquely identified canonical envelope is received before deadline",
    ),
    CheckSpec(
        "PFL-006",
        "Control GET_STATUS succeeds",
        "surface ControlClient",
        "all enabled module IDs",
        "every enabled module returns COMMAND_STATUS_COMPLETED to GET_STATUS",
    ),
    CheckSpec(
        "PFL-007",
        "Clock synchronization is valid",
        "chrony clock probe",
        "reachable source, leap status, offset, freshness",
        "source reachable AND leap normal AND abs(offset) < 10 ms AND successful check age <= 15 s",
    ),
    CheckSpec(
        "PFL-008",
        "Required USB devices exist",
        "USB/device path probe",
        "all configured cameras",
        "every required stable device path exists and resolves to a video device",
    ),
    CheckSpec(
        "PFL-009",
        "Requested camera modes open",
        "camera mode probe",
        "format, width, height, FPS per camera",
        "every requested capture tuple opens successfully",
    ),
    CheckSpec(
        "PFL-010",
        "Required cameras run simultaneously",
        "camera soak probe",
        "all required cameras for 10 seconds",
        "every camera stays open concurrently for at least 10.0 monotonic seconds",
    ),
    CheckSpec(
        "PFL-011",
        "Camera FPS is at least 95%",
        "camera soak metrics",
        "configured and achieved FPS",
        "for each camera achieved_fps/configured_fps >= 0.95",
    ),
    CheckSpec(
        "PFL-012",
        "Camera frame gaps are bounded",
        "camera soak metrics",
        "maximum inter-frame gap",
        "every maximum frame gap <= 500 ms",
    ),
    CheckSpec(
        "PFL-013",
        "Required RTP streams reach surface",
        "surface video receiver probe",
        "stream_to_surface cameras",
        "at least one valid RTP frame is observed for every required stream",
    ),
    CheckSpec(
        "PFL-014",
        "Video/FrameIndex correlation is at least 95%",
        "FrameCorrelator metrics",
        "exact hits and received video frames",
        "exact FrameIndex hits/received frames >= 0.95 for every required stream",
    ),
    CheckSpec(
        "PFL-015",
        "Platform memory and CPU are within limits",
        "resource sampler",
        "memory and CPU samples during camera soak",
        "available memory >= 512 MiB AND arithmetic mean CPU percentage < 85",
    ),
    CheckSpec(
        "PFL-016",
        "Pi temperature remains below 80 C",
        "thermal probe",
        "maximum measured temperature",
        "report maximum CPU temperature and warn at the historical 80 C guideline",
        fatal=False,
    ),
    CheckSpec(
        "PFL-017",
        "No thermal throttling",
        "throttling probe",
        "platform throttle flags",
        "report throttle flags as diagnostic evidence",
        fatal=False,
    ),
    CheckSpec(
        "PFL-018",
        "Root and recording filesystems have required free space",
        "DiskSpaceGuard",
        "root and recording.directory filesystems",
        f"root free >= {2 * GIB} AND recording free >= {10 * GIB}",
    ),
    CheckSpec(
        "PFL-019",
        "Enabled modules process ten frames",
        "module health/metrics probe",
        "enabled module processed-frame counts",
        "every enabled module processed_frames >= 10",
    ),
    CheckSpec(
        "PFL-020",
        "No required component is in ERROR",
        "system-health aggregator",
        "required component inventory and canonical states",
        "every required component is present and its canonical ComponentState != ERROR",
    ),
)


@dataclass(frozen=True, slots=True)
class CameraMeasurement:
    configured_fps: float
    achieved_fps: float
    maximum_gap_ms: float
    opened: bool = True
    simultaneous_seconds: float = 10.0


@dataclass(frozen=True, slots=True)
class CorrelationMeasurement:
    received_frames: int
    exact_matches: int

    @property
    def ratio(self) -> float:
        return self.exact_matches / self.received_frames if self.received_frames else 0.0


@dataclass(frozen=True, slots=True)
class ProbeSnapshot:
    model_hashes: dict[str, tuple[str, str]]
    tether_interface_up: bool
    surface_responds: bool
    broker_round_trip: bool
    control_status: dict[str, bool]
    clock: ClockSample
    usb_devices: dict[str, bool]
    cameras: dict[str, CameraMeasurement]
    rtp_streams: dict[str, bool]
    correlations: dict[str, CorrelationMeasurement]
    average_cpu_percent: float
    maximum_temperature_c: float
    thermally_throttled: bool
    recording_free_bytes: int
    module_processed_frames: dict[str, int]
    component_states: dict[str, ComponentState]
    missing_required_components: tuple[str, ...] = ()
    observed_monotonic: float | None = None
    unavailable_checks: dict[str, str] = field(default_factory=dict)
    evidence_by_check: dict[str, EvidenceKind] = field(default_factory=dict)
    available_memory_bytes: int | None = None
    root_free_bytes: int | None = None


@dataclass(frozen=True, slots=True)
class CheckResult:
    check_id: str
    name: str
    probe: str
    inputs: str
    calculation: str
    status: CheckStatus
    fatal: bool
    failure_reason: str
    measured: dict[str, Any]
    thresholds: dict[str, Any]
    evidence: EvidenceKind = EvidenceKind.OBSERVED

    @property
    def passed(self) -> bool:
        return self.status is CheckStatus.PASS

    def as_dict(self) -> dict[str, Any]:
        value = _json_safe(asdict(self))
        assert isinstance(value, dict)
        value["passed"] = self.passed
        return value


@dataclass(frozen=True, slots=True)
class PreflightReport:
    schema_version: int
    run_id: str
    timestamp: str
    overall_result: str
    exit_code: int
    checks: tuple[CheckResult, ...]
    mission_enable_decision: bool
    execution_error: str = ""
    configuration_sha256: str | None = None

    @property
    def mission_enable(self) -> bool:
        """Compatibility alias for the report's eligibility decision.

        Actual mission state is owned exclusively by ``MissionEnableGate``.
        """

        return self.mission_enable_decision

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "timestamp": self.timestamp,
            "overall_result": self.overall_result,
            "exit_code": self.exit_code,
            "checks": [check.as_dict() for check in self.checks],
            "mission_enable_decision": self.mission_enable_decision,
            "execution_error": self.execution_error,
            "configuration_sha256": self.configuration_sha256,
        }

    def to_json(self, *, indent: int | None = 2) -> str:
        return json.dumps(self.as_dict(), sort_keys=True, indent=indent, allow_nan=False) + (
            "\n" if indent is not None else ""
        )

    def write_json(self, path: Path) -> None:
        # Replace only after the complete report has been flushed in the target
        # directory.  A failed write therefore cannot truncate a prior report.
        temporary: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=path.parent,
                prefix=f".{path.name}.",
                suffix=".tmp",
                delete=False,
            ) as output:
                temporary = Path(output.name)
                output.write(self.to_json())
                output.flush()
                os.fsync(output.fileno())
            temporary.replace(path)
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)

    def human_summary(self) -> str:
        lines = [
            f"Phase 9 preflight: {self.overall_result}",
            f"Timestamp: {self.timestamp}",
        ]
        if self.configuration_sha256 is not None:
            lines.append(f"Configuration SHA-256: {self.configuration_sha256}")
        for check in self.checks:
            line = f"[{check.status.value}] {check.check_id} {check.name} ({check.evidence.value})"
            if check.failure_reason:
                line += f": {check.failure_reason}"
            lines.append(line)
            lines.append(f"  measured: {json.dumps(_json_safe(check.measured), sort_keys=True, allow_nan=False)}")
            if check.thresholds:
                lines.append(
                    f"  threshold: {json.dumps(_json_safe(check.thresholds), sort_keys=True, allow_nan=False)}"
                )
        if self.execution_error:
            lines.append(f"Execution error: {self.execution_error}")
        lines.extend(
            [
                f"Mission enable decision: {'ELIGIBLE' if self.mission_enable_decision else 'INELIGIBLE'}",
                f"Exit code: {self.exit_code}",
            ]
        )
        return "\n".join(lines)


def _json_safe(value: Any) -> Any:
    if isinstance(value, float) and not math.isfinite(value):
        if math.isnan(value):
            return "NaN"
        return "Infinity" if value > 0 else "-Infinity"
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return value


def _result(
    spec: CheckSpec,
    passed: bool,
    reason: str,
    measured: dict[str, Any],
    thresholds: dict[str, Any],
    *,
    evidence: EvidenceKind = EvidenceKind.OBSERVED,
    warning: bool = False,
) -> CheckResult:
    return CheckResult(
        spec.check_id,
        spec.name,
        spec.probe,
        spec.inputs,
        spec.calculation,
        CheckStatus.PASS if passed else CheckStatus.WARNING if warning else CheckStatus.FAIL,
        spec.fatal,
        "" if passed else reason,
        measured,
        thresholds,
        evidence,
    )


def evaluate_checks(config: AppConfig, snapshot: ProbeSnapshot) -> tuple[CheckResult, ...]:
    """Evaluate all checks using the same calculations for real and simulated probes."""

    specs = {item.check_id: item for item in CHECK_SPECS}
    results: list[CheckResult] = []
    results.append(_result(specs["PFL-001"], True, "", {"schema_version": config.schema_version}, {}))

    enabled_tasks = {task_id: task for task_id, task in config.tasks.items() if task.enabled}
    hash_failures = {
        task_id: {"expected": expected, "actual": actual}
        for task_id, (expected, actual) in snapshot.model_hashes.items()
        if expected != actual
    }
    missing_hashes = sorted(set(enabled_tasks) - set(snapshot.model_hashes))
    hashes_pass = not hash_failures and not missing_hashes
    results.append(
        _result(
            specs["PFL-002"],
            hashes_pass,
            f"hash mismatches={hash_failures}; missing={missing_hashes}",
            {"hashes": snapshot.model_hashes},
            {"algorithm": "sha256", "match": True},
        )
    )
    results.append(
        _result(
            specs["PFL-003"],
            snapshot.tether_interface_up,
            "tether interface is not UP",
            {"up": snapshot.tether_interface_up, "interface": config.network.tether_interface},
            {"up": True},
        )
    )
    results.append(
        _result(
            specs["PFL-004"],
            snapshot.surface_responds,
            f"surface {config.network.surface_ip} did not respond",
            {"responded": snapshot.surface_responds, "surface_ip": str(config.network.surface_ip)},
            {"responded": True},
        )
    )
    results.append(
        _result(
            specs["PFL-005"],
            snapshot.broker_round_trip,
            "canonical broker round trip timed out",
            {"round_trip": snapshot.broker_round_trip},
            {"round_trip": True},
        )
    )
    missing_control = sorted(set(enabled_tasks) - {key for key, value in snapshot.control_status.items() if value})
    results.append(
        _result(
            specs["PFL-006"],
            not missing_control,
            f"GET_STATUS did not complete for {missing_control}",
            {"status": snapshot.control_status},
            {"completed_for_all_enabled_modules": True},
        )
    )
    observed = snapshot.clock.checked_monotonic if snapshot.observed_monotonic is None else snapshot.observed_monotonic
    clock_age = observed - snapshot.clock.checked_monotonic
    clock_status = ClockMonitor(
        maximum_offset_ms=10.0,
        freshness_seconds=15.0,
        monotonic=lambda: observed,
    ).observe(snapshot.clock)
    clock_pass = clock_status.synchronized
    clock_reason = clock_status.reason
    results.append(
        _result(
            specs["PFL-007"],
            clock_pass,
            clock_reason,
            {
                "source_reachable": snapshot.clock.source_reachable,
                "leap_normal": snapshot.clock.leap_normal,
                "offset_ms": snapshot.clock.estimated_offset_ms,
                "successful_check_age_seconds": clock_age,
            },
            {"absolute_offset_ms_exclusive_maximum": 10.0, "freshness_seconds_maximum_inclusive": 15.0},
        )
    )

    required_cameras = set(config.cameras)
    missing_usb = sorted(camera_id for camera_id in required_cameras if not snapshot.usb_devices.get(camera_id, False))
    results.append(
        _result(
            specs["PFL-008"],
            not missing_usb,
            f"required USB devices missing or invalid: {missing_usb}",
            {"devices": snapshot.usb_devices},
            {"all_required": True},
        )
    )
    invalid_modes = sorted(
        camera_id
        for camera_id in required_cameras
        if camera_id not in snapshot.cameras or not snapshot.cameras[camera_id].opened
    )
    results.append(
        _result(
            specs["PFL-009"],
            not invalid_modes,
            f"camera modes failed to open: {invalid_modes}",
            {"opened": {key: value.opened for key, value in snapshot.cameras.items()}},
            {"all_requested_modes_open": True},
        )
    )
    short_runs = {
        key: value.simultaneous_seconds
        for key, value in snapshot.cameras.items()
        if key in required_cameras and value.simultaneous_seconds < 10.0
    }
    simultaneous_pass = not invalid_modes and not short_runs and required_cameras <= snapshot.cameras.keys()
    results.append(
        _result(
            specs["PFL-010"],
            simultaneous_pass,
            f"cameras did not run simultaneously for ten seconds: {short_runs or invalid_modes}",
            {"duration_seconds": {key: value.simultaneous_seconds for key, value in snapshot.cameras.items()}},
            {"minimum_seconds": 10.0},
        )
    )
    fps_ratios = {
        key: value.achieved_fps / value.configured_fps if value.configured_fps > 0 else 0.0
        for key, value in snapshot.cameras.items()
        if key in required_cameras
    }
    low_fps = {key: ratio for key, ratio in fps_ratios.items() if ratio < 0.95}
    results.append(
        _result(
            specs["PFL-011"],
            not low_fps and required_cameras <= fps_ratios.keys(),
            f"camera FPS below 95%: {low_fps}",
            {"ratios": fps_ratios},
            {"minimum_ratio": 0.95},
        )
    )
    gaps = {key: value.maximum_gap_ms for key, value in snapshot.cameras.items() if key in required_cameras}
    excessive_gaps = {key: gap for key, gap in gaps.items() if gap > 500.0}
    results.append(
        _result(
            specs["PFL-012"],
            not excessive_gaps and required_cameras <= gaps.keys(),
            f"camera frame gaps exceed 500 ms: {excessive_gaps}",
            {"maximum_gap_ms": gaps},
            {"maximum_gap_ms_inclusive": 500.0},
        )
    )

    required_streams = {key for key, camera in config.cameras.items() if camera.stream_to_surface}
    absent_streams = sorted(key for key in required_streams if not snapshot.rtp_streams.get(key, False))
    results.append(
        _result(
            specs["PFL-013"],
            not absent_streams,
            f"required RTP streams absent: {absent_streams}",
            {"streams": snapshot.rtp_streams},
            {"received_for_all_required": True},
        )
    )
    ratios = {key: value.ratio for key, value in snapshot.correlations.items() if key in required_streams}
    bad_ratios = {key: ratio for key, ratio in ratios.items() if ratio < 0.95}
    results.append(
        _result(
            specs["PFL-014"],
            not bad_ratios and required_streams <= ratios.keys(),
            f"FrameIndex correlation below 95%: {bad_ratios}",
            {"ratios": ratios},
            {"minimum_ratio": 0.95},
        )
    )
    results.append(
        _result(
            specs["PFL-015"],
            snapshot.average_cpu_percent < 85.0
            and snapshot.available_memory_bytes is not None
            and snapshot.available_memory_bytes >= 512 * 1024**2,
            (
                f"average CPU={snapshot.average_cpu_percent:.3f}% or available memory="
                f"{snapshot.available_memory_bytes!r} bytes violates the platform resource gate"
            ),
            {
                "average_cpu_percent": snapshot.average_cpu_percent,
                "available_memory_bytes": snapshot.available_memory_bytes,
            },
            {"exclusive_maximum_cpu_percent": 85.0, "minimum_available_memory_bytes_inclusive": 512 * 1024**2},
        )
    )
    results.append(
        _result(
            specs["PFL-016"],
            snapshot.maximum_temperature_c < 80.0,
            f"temperature {snapshot.maximum_temperature_c:.3f} C is not below 80 C",
            {"maximum_temperature_c": snapshot.maximum_temperature_c},
            {"historical_guideline_c": 80.0, "mission_gate": False},
            warning=True,
        )
    )
    results.append(
        _result(
            specs["PFL-017"],
            not snapshot.thermally_throttled,
            "thermal throttling flag is set",
            {"thermally_throttled": snapshot.thermally_throttled},
            {"historical_preference": False, "mission_gate": False},
            warning=True,
        )
    )
    results.append(
        _result(
            specs["PFL-018"],
            snapshot.root_free_bytes is not None
            and snapshot.root_free_bytes >= 2 * GIB
            and snapshot.recording_free_bytes >= 10 * GIB,
            (
                f"root has {snapshot.root_free_bytes!r} bytes free (requires {2 * GIB}); "
                f"recording filesystem has {snapshot.recording_free_bytes} bytes free (requires {10 * GIB})"
            ),
            {
                "root_free_bytes": snapshot.root_free_bytes,
                "recording_free_bytes": snapshot.recording_free_bytes,
            },
            {
                "minimum_root_free_bytes_inclusive": 2 * GIB,
                "minimum_recording_free_bytes_inclusive": 10 * GIB,
            },
        )
    )
    insufficient_frames = {
        key: snapshot.module_processed_frames.get(key, 0)
        for key in enabled_tasks
        if snapshot.module_processed_frames.get(key, 0) < 10
    }
    results.append(
        _result(
            specs["PFL-019"],
            not insufficient_frames,
            f"enabled modules processed fewer than ten frames: {insufficient_frames}",
            {"processed_frames": snapshot.module_processed_frames},
            {"minimum_frames_inclusive": 10},
        )
    )
    errors = sorted(key for key, state in snapshot.component_states.items() if state is ComponentState.ERROR)
    component_pass = not errors and not snapshot.missing_required_components
    results.append(
        _result(
            specs["PFL-020"],
            component_pass,
            f"ERROR components={errors}; missing required={list(snapshot.missing_required_components)}",
            {
                "states": {key: value.value for key, value in snapshot.component_states.items()},
                "missing_required": list(snapshot.missing_required_components),
            },
            {"required_state_not": "ERROR", "all_required_present": True},
        )
    )
    finalized: list[CheckResult] = []
    for result in results:
        evidence = snapshot.evidence_by_check.get(result.check_id, EvidenceKind.OBSERVED)
        unavailable = snapshot.unavailable_checks.get(result.check_id)
        if unavailable is not None:
            finalized.append(
                replace(
                    result,
                    status=CheckStatus.UNAVAILABLE,
                    failure_reason=unavailable,
                    evidence=evidence,
                )
            )
        else:
            finalized.append(replace(result, evidence=evidence))
    return tuple(finalized)


def make_report(
    checks: tuple[CheckResult, ...],
    *,
    now: datetime | None = None,
    execution_error: str = "",
    configuration_sha256: str | None = None,
) -> PreflightReport:
    timestamp = (now or datetime.now(timezone.utc)).astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    if execution_error or any(check.status is CheckStatus.UNAVAILABLE and check.fatal for check in checks):
        code = PreflightExitCode.COULD_NOT_EXECUTE
        overall = "ERROR"
    elif any(not check.passed and check.fatal for check in checks):
        code = PreflightExitCode.REQUIRED_CHECK_FAILED
        overall = "FAIL"
    else:
        code = PreflightExitCode.PASS
        overall = "PASS"
    identity_source = json.dumps(
        {
            "timestamp": timestamp,
            "overall_result": overall,
            "exit_code": int(code),
            "checks": [check.as_dict() for check in checks],
            "execution_error": execution_error,
            "configuration_sha256": configuration_sha256,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    run_id = hashlib.sha256(identity_source.encode("utf-8")).hexdigest()[:32]
    return PreflightReport(
        1,
        run_id,
        timestamp,
        overall,
        int(code),
        checks,
        code is PreflightExitCode.PASS,
        execution_error,
        configuration_sha256,
    )


__all__ = [
    "CHECK_SPECS",
    "CameraMeasurement",
    "CheckStatus",
    "CheckResult",
    "CheckSpec",
    "CorrelationMeasurement",
    "EvidenceKind",
    "PreflightExitCode",
    "PreflightReport",
    "ProbeSnapshot",
    "evaluate_checks",
    "make_report",
]
