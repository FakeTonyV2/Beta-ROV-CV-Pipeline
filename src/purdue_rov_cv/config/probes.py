"""Opt-in, Linux hardware checks for an already statically valid configuration."""

from __future__ import annotations

import hashlib
import importlib.util
import math
import os
import re
import shutil
import socket
import stat
import subprocess
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol
from urllib.parse import urlparse

from .issues import ConfigIssue
from .models import AppConfig, CameraConfig, CameraFormat, Runtime
from .ports import derive_stream_allocation

_V4L2_FOURCC: dict[CameraFormat, str] = {
    CameraFormat.H264: "H264",
    CameraFormat.MJPEG: "MJPG",
    CameraFormat.YUYV: "YUYV",
    CameraFormat.NV12: "NV12",
}
_RUNTIME_MODULE: dict[Runtime, str] = {
    Runtime.ONNXRUNTIME: "onnxruntime",
    Runtime.TENSORRT: "tensorrt",
}
_FORMAT_LINE = re.compile(r"^\s*\[\d+]:\s+'(?P<fourcc>[^']+)'")
_DISCRETE_SIZE_LINE = re.compile(r"^\s*Size:\s+Discrete\s+(?P<width>\d+)x(?P<height>\d+)")
_STEPPED_SIZE_LINE = re.compile(
    r"^\s*Size:\s+(?:Stepwise|Continuous)\s+"
    r"(?P<minimum_width>\d+)x(?P<minimum_height>\d+)\s+-\s+"
    r"(?P<maximum_width>\d+)x(?P<maximum_height>\d+)"
    r"(?:\s+with\s+step\s+(?P<step_width>\d+)/(?P<step_height>\d+))?"
)
_DISCRETE_INTERVAL_LINE = re.compile(r"^\s*Interval:\s+Discrete\s+.*\((?P<fps>[0-9.]+)\s+fps\)")
_STEPPED_INTERVAL_LINE = re.compile(
    r"^\s*Interval:\s+(?:Stepwise|Continuous)\s+"
    r"(?P<minimum_seconds>[0-9.]+)s\s+-\s+(?P<maximum_seconds>[0-9.]+)s"
    r"(?:\s+with\s+step\s+(?P<step_seconds>[0-9.]+)s)?"
)


@dataclass(frozen=True)
class CameraProbeResult:
    path_exists: bool
    resolves_to_video_device: bool
    path_kind_matches: bool
    capture_tuple_supported: bool
    detail: str = ""


class HardwareProbe(Protocol):
    """Contract used only after static validation has succeeded."""

    def probe_camera(self, camera_id: str, camera: CameraConfig) -> CameraProbeResult: ...

    def validate_runtime_and_artifact(self, config: AppConfig) -> tuple[ConfigIssue, ...]: ...

    def validate_port_availability(self, config: AppConfig) -> tuple[ConfigIssue, ...]: ...


class HardwareProbeUnavailable(RuntimeError):
    """Raised when this host cannot perform the requested hardware preflight."""


def _run_command(command: Sequence[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, check=False, capture_output=True, text=True, timeout=5)


def _runtime_available(module_name: str) -> bool:
    return importlib.util.find_spec(module_name) is not None


def _is_video_device(path: Path) -> bool:
    try:
        return stat.S_ISCHR(path.stat().st_mode) and re.fullmatch(r"video\d+", path.name) is not None
    except OSError:
        return False


def _check_port_availability(host: str, port: int, protocol: str) -> str | None:
    socket_type = socket.SOCK_STREAM if protocol == "tcp" else socket.SOCK_DGRAM
    try:
        with socket.socket(socket.AF_INET, socket_type) as candidate:
            candidate.bind((host, port))
    except OSError as error:
        detail = error.strerror or str(error)
        return f"cannot bind {protocol} {host}:{port}: {detail}"
    return None


def _matches_stepped_value(value: int, minimum: int, maximum: int, step: int | None) -> bool:
    if not minimum <= value <= maximum:
        return False
    if step is None or step == 0:
        return True
    return (value - minimum) % step == 0


def _matches_frame_interval(
    frame_rate: int,
    minimum_seconds: float,
    maximum_seconds: float,
    step_seconds: float | None,
) -> bool:
    requested_seconds = 1 / frame_rate
    tolerance = 1e-6
    if not minimum_seconds - tolerance <= requested_seconds <= maximum_seconds + tolerance:
        return False
    if step_seconds is None or step_seconds == 0.0:
        return True
    quotient = (requested_seconds - minimum_seconds) / step_seconds
    return math.isclose(quotient, round(quotient), abs_tol=tolerance)


def _capture_tuple_supported(listing: str, camera: CameraConfig) -> bool:
    """Parse ``v4l2-ctl --list-formats-ext`` without changing a device setting."""
    expected_fourcc = _V4L2_FOURCC[camera.format]
    current_fourcc: str | None = None
    size_matches = False

    for line in listing.splitlines():
        format_match = _FORMAT_LINE.match(line)
        if format_match:
            current_fourcc = format_match["fourcc"]
            size_matches = False
            continue
        if current_fourcc != expected_fourcc:
            continue

        size_match = _DISCRETE_SIZE_LINE.match(line)
        if size_match:
            size_matches = (int(size_match["width"]), int(size_match["height"])) == (camera.width, camera.height)
            continue

        stepped_size_match = _STEPPED_SIZE_LINE.match(line)
        if stepped_size_match:
            size_matches = _matches_stepped_value(
                camera.width,
                int(stepped_size_match["minimum_width"]),
                int(stepped_size_match["maximum_width"]),
                int(stepped_size_match["step_width"]) if stepped_size_match["step_width"] else None,
            ) and _matches_stepped_value(
                camera.height,
                int(stepped_size_match["minimum_height"]),
                int(stepped_size_match["maximum_height"]),
                int(stepped_size_match["step_height"]) if stepped_size_match["step_height"] else None,
            )
            continue

        if not size_matches:
            continue
        interval_match = _DISCRETE_INTERVAL_LINE.match(line)
        if interval_match and math.isclose(float(interval_match["fps"]), camera.frame_rate, abs_tol=1e-3):
            return True
        stepped_interval_match = _STEPPED_INTERVAL_LINE.match(line)
        if stepped_interval_match and _matches_frame_interval(
            camera.frame_rate,
            float(stepped_interval_match["minimum_seconds"]),
            float(stepped_interval_match["maximum_seconds"]),
            float(stepped_interval_match["step_seconds"]) if stepped_interval_match["step_seconds"] else None,
        ):
            return True
    return False


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as artifact:
        for chunk in iter(lambda: artifact.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _tcp_endpoint(endpoint: str) -> tuple[str, int]:
    parsed = urlparse(endpoint)
    assert parsed.hostname is not None and parsed.port is not None  # guaranteed by static validation
    return parsed.hostname, parsed.port


@dataclass(frozen=True)
class LinuxHardwareProbe:
    """Read-only hardware preflight for deployed Linux configurations.

    The temporary TCP/UDP binds are closed immediately: they detect a current
    conflict but do not reserve a port against a later process startup.
    """

    command_runner: Callable[[Sequence[str]], subprocess.CompletedProcess[str]] = _run_command
    runtime_available: Callable[[str], bool] = _runtime_available
    video_device_check: Callable[[Path], bool] = _is_video_device
    symlink_check: Callable[[Path], bool] = lambda path: path.is_symlink()
    port_checker: Callable[[str, int, str], str | None] = _check_port_availability
    v4l2_ctl: str = "v4l2-ctl"

    def probe_camera(self, camera_id: str, camera: CameraConfig) -> CameraProbeResult:
        path = camera.device_path
        if not path.exists():
            return CameraProbeResult(False, False, False, False, f"{path} does not exist")
        try:
            resolved_path = path.resolve(strict=True)
        except (OSError, RuntimeError) as error:
            return CameraProbeResult(True, False, False, False, f"cannot resolve {path}: {error}")

        is_video_device = self.video_device_check(resolved_path)
        path_kind_matches = self.symlink_check(path)
        if not is_video_device:
            return CameraProbeResult(
                True,
                False,
                path_kind_matches,
                False,
                f"{path} resolves to {resolved_path}, not a V4L2 /dev/videoN character device",
            )

        try:
            result = self.command_runner((self.v4l2_ctl, "--device", str(resolved_path), "--list-formats-ext"))
        except FileNotFoundError:
            return CameraProbeResult(True, True, path_kind_matches, False, f"{self.v4l2_ctl} is not installed")
        except (OSError, subprocess.TimeoutExpired) as error:
            return CameraProbeResult(True, True, path_kind_matches, False, f"{self.v4l2_ctl} failed: {error}")
        if result.returncode != 0:
            detail = result.stderr.strip() or result.stdout.strip() or f"exit status {result.returncode}"
            return CameraProbeResult(True, True, path_kind_matches, False, f"{self.v4l2_ctl} failed: {detail}")
        supported = _capture_tuple_supported(result.stdout, camera)
        detail = "" if supported else "configured format, resolution, or frame rate is not listed by v4l2-ctl"
        return CameraProbeResult(True, True, path_kind_matches, supported, detail)

    def validate_runtime_and_artifact(self, config: AppConfig) -> tuple[ConfigIssue, ...]:
        issues: list[ConfigIssue] = []
        for task_id, task in sorted(config.tasks.items()):
            if not task.enabled:
                continue
            path = task.artifact.path
            artifact_path = f"tasks.{task_id}.artifact.path"
            if not path.is_file():
                issues.append(ConfigIssue("MODEL_NOT_FOUND", artifact_path, f"artifact does not exist: {path}"))
            else:
                try:
                    actual_hash = _sha256(path)
                except OSError as error:
                    issues.append(ConfigIssue("MODEL_NOT_FOUND", artifact_path, f"cannot read artifact: {error}"))
                else:
                    if actual_hash != task.artifact.sha256:
                        issues.append(
                            ConfigIssue(
                                "MODEL_HASH_MISMATCH",
                                f"tasks.{task_id}.artifact.sha256",
                                "artifact SHA-256 does not match the configured digest",
                                {"actual": actual_hash},
                            )
                        )
            module_name = _RUNTIME_MODULE[task.artifact.runtime]
            if not self.runtime_available(module_name):
                issues.append(
                    ConfigIssue(
                        "RUNTIME_UNAVAILABLE",
                        f"tasks.{task_id}.artifact.runtime",
                        f"required runtime module is not importable: {module_name}",
                    )
                )
        return tuple(issues)

    def validate_port_availability(self, config: AppConfig) -> tuple[ConfigIssue, ...]:
        candidates: list[tuple[str, str, int, str]] = []
        for camera_id, camera in sorted(config.cameras.items()):
            allocation = derive_stream_allocation(camera_id, camera.stream_index)
            candidates.extend(
                (
                    (f"cameras.{camera_id}.stream_index", str(config.network.rov_ip), allocation.rtp_port, "udp"),
                    (f"cameras.{camera_id}.stream_index", str(config.network.rov_ip), allocation.rtcp_port, "udp"),
                )
            )
        for path, endpoint in (
            ("messaging.broker.publisher_endpoint", config.messaging.broker.publisher_endpoint),
            ("messaging.broker.subscriber_endpoint", config.messaging.broker.subscriber_endpoint),
            ("messaging.control.client_endpoint", config.messaging.control.client_endpoint),
        ):
            host, port = _tcp_endpoint(endpoint)
            candidates.append((path, host, port, "tcp"))

        issues = [
            ConfigIssue("PORT_UNAVAILABLE", path, detail)
            for path, host, port, protocol in candidates
            if (detail := self.port_checker(host, port, protocol)) is not None
        ]

        ipc_path = Path(config.messaging.control.module_endpoint.removeprefix("ipc://"))
        if not ipc_path.parent.is_dir():
            issues.append(
                ConfigIssue(
                    "IPC_DIRECTORY_UNAVAILABLE",
                    "messaging.control.module_endpoint",
                    f"IPC directory does not exist: {ipc_path.parent}",
                )
            )
        elif not os.access(ipc_path.parent, os.W_OK | os.X_OK):
            issues.append(
                ConfigIssue(
                    "IPC_DIRECTORY_UNAVAILABLE",
                    "messaging.control.module_endpoint",
                    f"IPC directory is not writable: {ipc_path.parent}",
                )
            )
        elif ipc_path.exists():
            issues.append(
                ConfigIssue(
                    "IPC_ENDPOINT_OCCUPIED",
                    "messaging.control.module_endpoint",
                    f"IPC endpoint path already exists: {ipc_path}",
                )
            )
        return tuple(issues)


def create_default_hardware_probe() -> LinuxHardwareProbe:
    """Create the host probe or clearly report why live checks cannot run."""
    if sys.platform != "linux":
        raise HardwareProbeUnavailable("hardware probing requires Linux with V4L2 support")
    if shutil.which("v4l2-ctl") is None:
        raise HardwareProbeUnavailable("v4l2-ctl is not installed; install the v4l-utils system package")
    return LinuxHardwareProbe()


def validate_hardware_config(config: AppConfig, probe: HardwareProbe) -> tuple[ConfigIssue, ...]:
    """Use an injected probe only after static validation has already succeeded."""
    issues: list[ConfigIssue] = []
    for camera_id, camera in sorted(config.cameras.items()):
        result = probe.probe_camera(camera_id, camera)
        path = f"cameras.{camera_id}.device_path"
        if not result.path_exists:
            issues.append(
                ConfigIssue("CAMERA_NOT_FOUND", path, result.detail or "configured camera path does not exist")
            )
            continue
        if not result.resolves_to_video_device:
            issues.append(
                ConfigIssue("CAMERA_DEVICE_INVALID", path, result.detail or "path does not resolve to a video device")
            )
            continue
        if not result.path_kind_matches:
            issues.append(
                ConfigIssue(
                    "CAMERA_PATH_KIND_HARDWARE_MISMATCH",
                    path,
                    result.detail or "configured path is not a stable device symlink",
                )
            )
        if not result.capture_tuple_supported:
            issues.append(
                ConfigIssue(
                    "CAMERA_MODE_UNSUPPORTED", f"cameras.{camera_id}", result.detail or "capture tuple is unsupported"
                )
            )
    issues.extend(probe.validate_runtime_and_artifact(config))
    issues.extend(probe.validate_port_availability(config))
    return tuple(sorted(issues, key=lambda issue: (issue.path, issue.code, issue.message)))
