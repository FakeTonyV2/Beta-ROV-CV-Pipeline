"""Reference-platform validator with explicit, machine-readable outcomes."""

from __future__ import annotations

import json
import os
import platform
import shutil
import subprocess
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from enum import StrEnum
from pathlib import Path
from typing import Callable

import psutil

from purdue_rov_cv.config.issues import ConfigurationError
from purdue_rov_cv.config.loader import config_hash as canonical_config_hash
from purdue_rov_cv.config.loader import load_config
from purdue_rov_cv.module_runner.entrypoints import load_module

MIB = 1024**2
GIB = 1024**3


class EnvironmentStatus(StrEnum):
    SUPPORTED = "SUPPORTED"
    UNSUPPORTED = "UNSUPPORTED"
    WARNING = "WARNING"
    UNVERIFIED = "UNVERIFIED"


@dataclass(frozen=True, slots=True)
class EnvironmentCheck:
    check_id: str
    name: str
    status: EnvironmentStatus
    detected: str
    required: str
    detail: str = ""
    hard_requirement: bool = True


@dataclass(frozen=True, slots=True)
class EnvironmentReport:
    schema_version: int
    role: str
    generated_at: str
    overall_status: EnvironmentStatus
    supported: bool
    checks: tuple[EnvironmentCheck, ...]
    configuration_sha256: str | None

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "role": self.role,
            "generated_at": self.generated_at,
            "overall_status": self.overall_status.value,
            "supported": self.supported,
            "checks": [asdict(check) for check in self.checks],
            "configuration_sha256": self.configuration_sha256,
        }

    def to_json(self) -> str:
        return json.dumps(self.as_dict(), indent=2, sort_keys=True) + "\n"

    def human_summary(self) -> str:
        lines = [f"Deployment environment ({self.role}): {self.overall_status.value}"]
        for check in self.checks:
            line = f"[{check.status.value}] {check.check_id} {check.name}: detected={check.detected}; required={check.required}"
            if check.detail:
                line += f"; {check.detail}"
            lines.append(line)
        return "\n".join(lines)


class EnvironmentValidator:
    """Validate a Pi or surface host without modifying it."""

    def __init__(
        self,
        *,
        machine: Callable[[], str] = platform.machine,
        python_version: tuple[int, int] | None = None,
        which: Callable[[str], str | None] = shutil.which,
        run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
        root: Path = Path("/"),
    ) -> None:
        self._machine = machine
        self._python_version = python_version or sys.version_info[:2]
        self._which = which
        self._run = run
        self._root = root

    @staticmethod
    def _check(
        check_id: str,
        name: str,
        passed: bool,
        detected: str,
        required: str,
        *,
        detail: str = "",
        hard: bool = True,
        unverifiable: bool = False,
    ) -> EnvironmentCheck:
        status = (
            EnvironmentStatus.UNVERIFIED
            if unverifiable
            else EnvironmentStatus.SUPPORTED
            if passed
            else EnvironmentStatus.UNSUPPORTED
            if hard
            else EnvironmentStatus.WARNING
        )
        return EnvironmentCheck(check_id, name, status, detected, required, detail, hard)

    def _os_release(self) -> dict[str, str]:
        path = self._root / "etc" / "os-release"
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except OSError:
            return {}
        return {key: value.strip().strip('"') for line in lines if "=" in line for key, value in [line.split("=", 1)]}

    def _gst_version(self) -> tuple[str, bool]:
        executable = self._which("gst-launch-1.0")
        if executable is None:
            return "not found", False
        try:
            result = self._run([executable, "--version"], capture_output=True, text=True, timeout=3.0, check=False)
        except (OSError, subprocess.SubprocessError) as error:
            return f"query failed: {error}", False
        output = result.stdout.splitlines()[0] if result.stdout else "unknown"
        numbers = [part for part in output.replace("-", " ").split() if part[:1].isdigit()]
        if not numbers:
            return output, False
        try:
            major, minor, *_ = (int(part) for part in numbers[-1].split("."))
        except ValueError:
            return output, False
        return output, result.returncode == 0 and (major, minor) >= (1, 22)

    def _pi_model(self) -> str:
        for relative in (
            "sys/firmware/devicetree/base/model",
            "proc/device-tree/model",
        ):
            try:
                return (self._root / relative).read_text(encoding="ascii").rstrip("\x00\n")
            except OSError:
                continue
        return "unavailable"

    def _tether_status(self, interface: str, local_ip: str, peer_ip: str) -> tuple[bool, str]:
        try:
            address = self._run(
                ["ip", "-json", "address", "show", "dev", interface],
                capture_output=True,
                text=True,
                timeout=3.0,
                check=False,
            )
            decoded = json.loads(address.stdout) if address.returncode == 0 else []
            addresses = {
                item.get("local")
                for link in decoded
                if isinstance(link, dict)
                for item in link.get("addr_info", [])
                if isinstance(item, dict) and item.get("family") == "inet"
            }
        except (OSError, subprocess.SubprocessError, json.JSONDecodeError, TypeError):
            addresses = set()
        try:
            ping = self._run(
                ["ping", "-n", "-c", "1", "-W", "1", peer_ip],
                capture_output=True,
                text=True,
                timeout=3.0,
                check=False,
            )
            reachable = ping.returncode == 0
        except (OSError, subprocess.SubprocessError):
            reachable = False
        passed = local_ip in addresses and reachable
        return (
            passed,
            f"local={local_ip in addresses}; peer_reachable={reachable}; addresses={sorted(address for address in addresses if isinstance(address, str))}",
        )

    def validate(self, *, role: str, config_path: Path) -> EnvironmentReport:
        if role not in {"pi", "surface"}:
            raise ValueError("role must be 'pi' or 'surface'")
        checks: list[EnvironmentCheck] = []
        machine = self._machine().lower()
        expected_arch = "aarch64" if role == "pi" else "x86_64 or aarch64"
        arch_ok = machine in ({"aarch64", "arm64"} if role == "pi" else {"x86_64", "amd64", "aarch64", "arm64"})
        checks.append(self._check("ENV-001", "Architecture", arch_ok, machine, expected_arch))
        if role == "pi":
            model = self._pi_model()
            checks.append(
                self._check(
                    "ENV-001A",
                    "Raspberry Pi model",
                    "raspberry pi 5" in model.lower(),
                    model,
                    "Raspberry Pi 5",
                )
            )
        release = self._os_release()
        detected_os = f"{release.get('ID', 'unknown')} {release.get('VERSION_ID', 'unknown')}"
        checks.append(
            self._check(
                "ENV-002",
                "Operating system",
                release.get("ID") == "ubuntu" and release.get("VERSION_ID") == "24.04",
                detected_os,
                "Ubuntu 24.04 LTS",
            )
        )
        systemd = self._root / "run" / "systemd" / "system"
        checks.append(self._check("ENV-003", "systemd supervisor", systemd.exists(), str(systemd), "active systemd"))
        python = ".".join(str(value) for value in self._python_version)
        checks.append(self._check("ENV-004", "Python runtime", self._python_version == (3, 12), python, "3.12.x"))
        in_venv = sys.prefix != sys.base_prefix
        checks.append(
            self._check(
                "ENV-004A",
                "Isolated Python environment",
                in_venv,
                sys.prefix,
                "/opt/purdue-rov-cv/.venv production environment",
            )
        )
        gst, gst_ok = self._gst_version()
        checks.append(self._check("ENV-005", "GStreamer runtime", gst_ok, gst, ">= 1.22"))
        binaries = ["chronyc", "ip", "systemctl"] + (["v4l2-ctl", "udevadm"] if role == "pi" else [])
        missing = [binary for binary in binaries if self._which(binary) is None]
        checks.append(
            self._check(
                "ENV-006",
                "Required system binaries",
                not missing,
                ", ".join(missing) or "all found",
                ", ".join(binaries),
            )
        )
        try:
            dependency_result = self._run(
                [sys.executable, "-m", "pip", "check"],
                capture_output=True,
                text=True,
                timeout=15.0,
                check=False,
            )
            dependencies_ok = dependency_result.returncode == 0
            dependency_detail = (dependency_result.stdout or dependency_result.stderr).strip()
        except (OSError, subprocess.SubprocessError) as error:
            dependencies_ok = False
            dependency_detail = f"dependency check could not execute: {error}"
        checks.append(
            self._check(
                "ENV-006A",
                "Python dependency health",
                dependencies_ok,
                dependency_detail or ("healthy" if dependencies_ok else "invalid"),
                "locked environment with pip check passing",
            )
        )
        try:
            config = load_config(config_path, environ={})
            config_hash = canonical_config_hash(config)
            checks.append(
                self._check("ENV-007", "Production configuration", True, str(config_path), "strict valid mission YAML")
            )
        except (ConfigurationError, OSError) as error:
            config = None
            config_hash = None
            checks.append(
                self._check(
                    "ENV-007",
                    "Production configuration",
                    False,
                    str(config_path),
                    "strict valid mission YAML",
                    detail=str(error),
                )
            )
        runtime_paths = [self._root / "run/purdue-rov-cv", self._root / "var/lib/purdue-rov-cv"]
        absent_runtime = [str(path) for path in runtime_paths if not path.exists()]
        checks.append(
            self._check(
                "ENV-008",
                "Runtime directories",
                not absent_runtime,
                ", ".join(absent_runtime) or "present",
                ", ".join(str(path) for path in runtime_paths),
            )
        )
        available = int(psutil.virtual_memory().available)
        checks.append(
            self._check("ENV-009", "Available memory", available >= 512 * MIB, str(available), f">= {512 * MIB} bytes")
        )
        try:
            root_free = int(shutil.disk_usage(self._root).free)
            disk_unverified = False
        except OSError:
            root_free = 0
            disk_unverified = True
        checks.append(
            self._check(
                "ENV-010",
                "Root filesystem free space",
                root_free >= 2 * GIB,
                str(root_free),
                f">= {2 * GIB} bytes",
                unverifiable=disk_unverified,
            )
        )
        if config is None:
            checks.append(
                self._check(
                    "ENV-011",
                    "Tether interface",
                    False,
                    "configuration unavailable",
                    "configured interface operational",
                    unverifiable=True,
                )
            )
            checks.append(
                self._check(
                    "ENV-012",
                    "Camera provisioning",
                    False,
                    "configuration unavailable",
                    "all configured Pi cameras available",
                    unverifiable=True,
                )
            )
            checks.append(
                self._check(
                    "ENV-013",
                    "Model artifacts",
                    False,
                    "configuration unavailable",
                    "all enabled artifacts readable",
                    unverifiable=True,
                )
            )
            checks.append(
                self._check(
                    "ENV-013A",
                    "Module entry points",
                    False,
                    "configuration unavailable",
                    "every enabled module_class is an installed CVModule",
                    unverifiable=True,
                )
            )
        else:
            interface = self._root / "sys/class/net" / config.network.tether_interface
            try:
                operstate = (interface / "operstate").read_text(encoding="ascii").strip().lower()
            except OSError:
                operstate = "unavailable"
            local_ip = str(config.network.rov_ip if role == "pi" else config.network.surface_ip)
            peer_ip = str(config.network.surface_ip if role == "pi" else config.network.rov_ip)
            tether_configured, tether_detail = self._tether_status(config.network.tether_interface, local_ip, peer_ip)
            checks.append(
                self._check(
                    "ENV-011",
                    "Tether interface",
                    operstate == "up" and tether_configured,
                    f"operstate={operstate}; {tether_detail}",
                    f"{config.network.tether_interface} up with local {local_ip} and reachable peer {peer_ip}",
                )
            )
            invalid_classes: list[str] = []
            for task_id, task in config.tasks.items():
                if not task.enabled:
                    continue
                try:
                    load_module(task.module_class)
                except (ImportError, AttributeError, TypeError, ValueError):
                    invalid_classes.append(task_id)
            checks.append(
                self._check(
                    "ENV-013A",
                    "Module entry points",
                    not invalid_classes,
                    ", ".join(invalid_classes) or "all importable",
                    "every enabled module_class is an installed CVModule",
                )
            )
            missing_cameras = [
                camera_id
                for camera_id, camera in config.cameras.items()
                if role == "pi"
                and (camera.device_path is None or not (self._root / str(camera.device_path).lstrip("/")).exists())
            ]
            checks.append(
                self._check(
                    "ENV-012",
                    "Camera provisioning",
                    not missing_cameras,
                    ", ".join(missing_cameras) or "all present",
                    "all configured Pi cameras available",
                )
            )
            missing_artifacts = [
                task_id
                for task_id, task in config.tasks.items()
                if role == "pi" and task.enabled and not (self._root / str(task.artifact.path).lstrip("/")).is_file()
            ]
            checks.append(
                self._check(
                    "ENV-013",
                    "Model artifacts",
                    not missing_artifacts,
                    ", ".join(missing_artifacts) or "all readable",
                    "all enabled artifacts readable",
                )
            )
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
        detected_temperature = f"{max(temperatures):.1f} C" if temperatures else "sensor unavailable"
        checks.append(
            self._check(
                "ENV-014",
                "Thermal observation",
                bool(temperatures) and max(temperatures) < 80.0,
                detected_temperature,
                "diagnostic only; historical guideline < 80 C",
                hard=False,
                unverifiable=not temperatures,
                detail="never an independent deployment or mission gate",
            )
        )
        hard_failure = any(check.hard_requirement and check.status is EnvironmentStatus.UNSUPPORTED for check in checks)
        hard_unverified = any(
            check.hard_requirement and check.status is EnvironmentStatus.UNVERIFIED for check in checks
        )
        warning = any(check.status is EnvironmentStatus.WARNING for check in checks)
        unverified = any(check.status is EnvironmentStatus.UNVERIFIED for check in checks)
        overall = (
            EnvironmentStatus.UNSUPPORTED
            if hard_failure
            else EnvironmentStatus.UNVERIFIED
            if hard_unverified
            else EnvironmentStatus.UNVERIFIED
            if unverified
            else EnvironmentStatus.WARNING
            if warning
            else EnvironmentStatus.SUPPORTED
        )
        return EnvironmentReport(
            1,
            role,
            datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            overall,
            not hard_failure and not hard_unverified,
            tuple(checks),
            config_hash,
        )


def write_report(report: EnvironmentReport, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(report.to_json(), encoding="utf-8")
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


__all__ = ["EnvironmentCheck", "EnvironmentReport", "EnvironmentStatus", "EnvironmentValidator", "write_report"]
