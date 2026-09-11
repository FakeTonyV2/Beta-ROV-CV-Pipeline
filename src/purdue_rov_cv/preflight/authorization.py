"""Fail-closed production authorization for mission module START commands."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import time
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import cast

from purdue_rov_cv.config.loader import config_hash
from purdue_rov_cv.config.models import AppConfig
from purdue_rov_cv.frame_buffer.buffer import ReadStatus, SharedMemoryFrameReader
from purdue_rov_cv.runtime.state import ComponentState

from .clock import ClockStatus
from .health import ComponentHealth, MissionEnableGate, MissionState, SystemHealth

MIB = 1024**2
GIB = 1024**3


LiveHealthProbe = Callable[[], tuple[tuple[ComponentHealth, ...], ClockStatus]]


class PreflightStartAuthorizer:
    """Authorize START from a fresh canonical report plus current hard gates.

    The control router owns one instance, so ``MissionEnableGate`` remains the
    single mission latch for every client capable of issuing START.
    """

    def __init__(
        self,
        config: AppConfig,
        *,
        report_path: Path = Path("/var/lib/purdue-rov-cv/preflight.json"),
        health_path: Path = Path("/run/purdue-rov-cv/system-health.json"),
        state_path: Path = Path("/run/purdue-rov-cv/mission-state.json"),
        report_max_age_seconds: float = 30.0,
        health_max_age_seconds: float = 15.0,
        frame_max_age_seconds: float = 2.0,
        live_health_probe: LiveHealthProbe | None = None,
        now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.config = config
        self.report_path = report_path
        self.health_path = health_path
        self.state_path = state_path
        self.report_max_age_seconds = report_max_age_seconds
        self.health_max_age_seconds = health_max_age_seconds
        self.frame_max_age_seconds = frame_max_age_seconds
        self._live_health_probe = live_health_probe or self._probe_live_health
        self._now = now
        self._monotonic = monotonic
        self._gate = MissionEnableGate()

    @property
    def state(self) -> MissionState:
        return self._gate.state

    @property
    def reason(self) -> str:
        return self._gate.reason

    @staticmethod
    def _read_json(path: Path) -> dict[str, object]:
        value = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(value, dict):
            raise ValueError(f"{path} is not a JSON object")
        return value

    def _fresh_timestamp(self, value: object, maximum_age: float) -> bool:
        if not isinstance(value, str):
            return False
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return False
        if parsed.tzinfo is None:
            return False
        age = (self._now() - parsed.astimezone(timezone.utc)).total_seconds()
        return -5.0 <= age <= maximum_age

    def _valid_report(self, report: dict[str, object]) -> tuple[bool, str, str | None]:
        checks = report.get("checks")
        run_id = report.get("run_id")
        expected_check_ids = {f"PFL-{index:03d}" for index in range(1, 21)}
        check_items = [item for item in checks if isinstance(item, dict)] if isinstance(checks, list) else []
        check_ids = [item.get("check_id") for item in check_items]
        canonical_check_set = (
            len(check_items) == 20 and len(set(check_ids)) == 20 and set(check_ids) == expected_check_ids
        )
        acceptable_checks = canonical_check_set and all(
            (
                item.get("status") == "PASS"
                if item.get("check_id") not in {"PFL-016", "PFL-017"}
                else item.get("status") in {"PASS", "WARNING", "UNAVAILABLE"}
            )
            and (
                item.get("fatal") is True
                if item.get("check_id") not in {"PFL-016", "PFL-017"}
                else item.get("fatal") is False
            )
            for item in check_items
        )
        expected_hash = config_hash(self.config)
        conditions = {
            "schema version is 1": report.get("schema_version") == 1,
            "run identifier is present": isinstance(run_id, str) and bool(run_id),
            "result is PASS": report.get("overall_result") == "PASS",
            "exit code is zero": report.get("exit_code") == 0,
            "mission decision is eligible": report.get("mission_enable_decision") is True,
            "canonical PFL-001 through PFL-020 checks are acceptable": acceptable_checks,
            "configuration hash matches": report.get("configuration_sha256") == expected_hash,
            "report is fresh": self._fresh_timestamp(report.get("timestamp"), self.report_max_age_seconds),
        }
        failures = [name for name, passed in conditions.items() if not passed]
        return not failures, ", ".join(failures), run_id if isinstance(run_id, str) else None

    @staticmethod
    def _sha256(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def _component(self, component_id: str, passed: bool, detail: str) -> ComponentHealth:
        return ComponentHealth(
            component_id,
            "mission-gate",
            ComponentState.RUNNING if passed else ComponentState.ERROR,
            detail=detail,
            observed_monotonic=self._monotonic(),
        )

    def _probe_live_health(self) -> tuple[tuple[ComponentHealth, ...], ClockStatus]:
        components: list[ComponentHealth] = []
        try:
            state = self._read_json(self.health_path)
        except (OSError, json.JSONDecodeError, ValueError) as error:
            state = {}
            state_error = str(error)
        else:
            state_error = ""
        resources_value = state.get("resources")
        clock_value_raw = state.get("clock")
        resources = cast(dict[str, object], resources_value) if isinstance(resources_value, dict) else {}
        clock_value = cast(dict[str, object], clock_value_raw) if isinstance(clock_value_raw, dict) else {}
        health_fresh = self._fresh_timestamp(state.get("timestamp"), self.health_max_age_seconds)
        memory = resources.get("available_memory_bytes")
        root_free = resources.get("root_free_bytes")
        components.extend(
            (
                self._component(
                    "system-health",
                    health_fresh,
                    state_error or ("fresh" if health_fresh else "state is stale"),
                ),
                self._component(
                    "memory",
                    health_fresh and isinstance(memory, int) and memory >= 512 * MIB,
                    f"available={memory!r}; required>={512 * MIB}",
                ),
                self._component(
                    "root-disk",
                    health_fresh and isinstance(root_free, int) and root_free >= 2 * GIB,
                    f"free={root_free!r}; required>={2 * GIB}",
                ),
            )
        )
        synchronized = health_fresh and clock_value.get("synchronized") is True
        consecutive_failures = clock_value.get("consecutive_failures", 0)
        clock = ClockStatus(
            synchronized,
            synchronized and clock_value.get("cross_device_latency_valid") is True,
            consecutive_failures if isinstance(consecutive_failures, int) else 0,
            None,
            None,
            str(clock_value.get("reason", "")) or ("" if synchronized else "clock state is not synchronized and fresh"),
        )
        interface = self.config.network.tether_interface
        try:
            operstate = Path(f"/sys/class/net/{interface}/operstate").read_text(encoding="ascii").strip().lower()
        except OSError:
            operstate = "unavailable"
        try:
            address = subprocess.run(
                ["ip", "-json", "address", "show", "dev", interface],
                capture_output=True,
                text=True,
                timeout=2.0,
                check=False,
            )
            address_data = json.loads(address.stdout) if address.returncode == 0 else []
            local_address_present = any(
                item.get("family") == "inet" and item.get("local") == str(self.config.network.rov_ip)
                for link in address_data
                if isinstance(link, dict)
                for item in link.get("addr_info", [])
                if isinstance(item, dict)
            )
        except (OSError, subprocess.SubprocessError, json.JSONDecodeError, TypeError):
            local_address_present = False
        try:
            ping = subprocess.run(
                ["ping", "-n", "-c", "1", "-W", "1", str(self.config.network.surface_ip)],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=2.0,
                check=False,
            )
            peer_reachable = ping.returncode == 0
        except (OSError, subprocess.SubprocessError):
            peer_reachable = False
        components.append(
            self._component(
                "tether",
                operstate == "up" and local_address_present and peer_reachable,
                f"interface={interface} operstate={operstate} local_address_present={local_address_present} peer_reachable={peer_reachable}",
            )
        )
        for camera_id, camera in self.config.cameras.items():
            device_present = bool(camera.device_path and camera.device_path.exists())
            frame_fresh = False
            reader = SharedMemoryFrameReader(
                camera_id,
                expected_slot_capacity_bytes=camera.slot_capacity_bytes,
            )
            try:
                if reader.attach():
                    result = reader.read()
                    if result.status is ReadStatus.FRAME and result.header is not None:
                        age = self._monotonic() - result.header.capture_monotonic_ns / 1_000_000_000
                        frame_fresh = 0.0 <= age <= self.frame_max_age_seconds
            except Exception:
                frame_fresh = False
            finally:
                reader.close()
            components.append(
                self._component(
                    f"camera:{camera_id}",
                    device_present and frame_fresh,
                    f"device_present={device_present} frame_fresh={frame_fresh}",
                )
            )
        for task_id, task in self.config.tasks.items():
            if not task.enabled:
                continue
            try:
                artifact_valid = (
                    task.artifact.path.is_file() and self._sha256(task.artifact.path) == task.artifact.sha256
                )
            except OSError:
                artifact_valid = False
            components.append(
                self._component(
                    f"artifact:{task_id}",
                    artifact_valid,
                    f"path={task.artifact.path} sha256_match={artifact_valid}",
                )
            )
        return tuple(components), clock

    def authorize(self, *, startup_dependencies_satisfied: bool) -> tuple[bool, str]:
        try:
            report = self._read_json(self.report_path)
            report_valid, report_reason, run_id = self._valid_report(report)
            components, clock = self._live_health_probe()
        except (OSError, json.JSONDecodeError, ValueError) as error:
            return False, f"mission authorization evidence unavailable: {error}"
        health = SystemHealth(
            components=components,
            clock=clock,
            preflight_completed=True,
            preflight_passed=report_valid,
            preflight_exit_code=0 if report_valid else 1,
            mission_state=self._gate.state,
            mission_enabled=self._gate.enabled,
            cross_device_latency_valid=clock.cross_device_latency_valid,
            failure_reasons=(report_reason,) if report_reason else (),
            observed_monotonic=self._monotonic(),
            preflight_run_id=run_id,
        )
        authorized = self._gate.authorize(
            health,
            startup_dependencies_satisfied=startup_dependencies_satisfied,
        )
        detail = self._gate.reason or "mission START authorized"
        if report_reason and report_reason not in detail:
            detail = f"{detail}; {report_reason}"
        failed_components = [
            f"{component.component_id}: {component.detail}"
            for component in components
            if component.state is ComponentState.ERROR
        ]
        if failed_components:
            detail = f"{detail}; " + "; ".join(failed_components)
        state = {
            "schema_version": 1,
            "timestamp": self._now().isoformat().replace("+00:00", "Z"),
            "configuration_sha256": config_hash(self.config),
            "preflight_run_id": run_id,
            "mission_enabled": authorized,
            "mission_state": self._gate.state.value,
            "reason": detail,
            "startup_dependencies_satisfied": startup_dependencies_satisfied,
            "required_components": {component.component_id: component.state.value for component in components},
            "clock_synchronized": clock.synchronized,
        }
        temporary = self.state_path.with_name(f".{self.state_path.name}.{os.getpid()}.tmp")
        try:
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
            temporary.write_text(json.dumps(state, sort_keys=True) + "\n", encoding="utf-8")
            temporary.replace(self.state_path)
        except OSError as error:
            if authorized:
                detail = f"mission state evidence could not be persisted: {error}"
                self._gate.disable(detail)
                return False, detail
        finally:
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass
        return authorized, detail


__all__ = ["LiveHealthProbe", "PreflightStartAuthorizer"]
