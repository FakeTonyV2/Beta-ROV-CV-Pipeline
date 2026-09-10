"""Opt-in, Linux hardware checks for an already statically valid configuration."""

from __future__ import annotations

import hashlib
import importlib.util
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

from purdue_rov_cv.camera.v4l2 import (
    V4L2ConfigurationError,
    V4L2DeviceInvalid,
    V4L2DeviceProbe,
    V4L2Disconnected,
    V4L2IdentityMismatch,
    V4L2ModeUnsupported,
    V4L2ProbeError,
)

from .issues import ConfigIssue
from .models import AppConfig, CameraAdapter, CameraConfig, Runtime
from .ports import derive_stream_allocation

_RUNTIME_MODULE: dict[Runtime, str] = {
    Runtime.ONNXRUNTIME: "onnxruntime",
    Runtime.TENSORRT: "tensorrt",
}


@dataclass(frozen=True)
class CameraProbeResult:
    path_exists: bool
    resolves_to_video_device: bool
    path_kind_matches: bool
    capture_tuple_supported: bool
    detail: str = ""
    mode_opened: bool = True
    resolved_path: str = ""
    resolution_tier: str = ""
    physical_identity: str = ""
    backend_supported: bool = True
    probe_executed: bool = True


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


def sha256_file(path: Path) -> str:
    """Return the canonical artifact digest used by validation and preflight."""

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
    gst_launch: str = "gst-launch-1.0"

    def probe_camera(self, camera_id: str, camera: CameraConfig) -> CameraProbeResult:
        if camera.adapter is not CameraAdapter.V4L2:
            return CameraProbeResult(
                False,
                False,
                True,
                False,
                f"hardware probe for backend {camera.adapter.value} is outside Phase 10",
                False,
                backend_supported=False,
                probe_executed=False,
            )
        assert camera.device_path is not None
        probe = V4L2DeviceProbe(
            command_runner=self.command_runner,
            video_device_check=self.video_device_check,
            symlink_check=self.symlink_check,
            v4l2_ctl=self.v4l2_ctl,
            gst_launch=self.gst_launch,
        )
        try:
            report = probe.validate(camera_id, camera, open_mode=True)
        except V4L2Disconnected as error:
            return CameraProbeResult(False, False, True, False, str(error), False)
        except V4L2DeviceInvalid as error:
            path_exists = camera.device_path.exists()
            path_kind_matches = self.symlink_check(camera.device_path) if path_exists else False
            resolves_to_video = False
            if path_exists:
                try:
                    resolves_to_video = self.video_device_check(camera.device_path.resolve(strict=True))
                except (OSError, RuntimeError):
                    pass
            return CameraProbeResult(
                path_exists,
                resolves_to_video,
                path_kind_matches,
                False,
                str(error),
                False,
            )
        except V4L2IdentityMismatch as error:
            return CameraProbeResult(True, True, False, False, str(error), False)
        except V4L2ModeUnsupported as error:
            return CameraProbeResult(True, True, True, False, str(error), False)
        except V4L2ConfigurationError as error:
            return CameraProbeResult(True, True, True, False, str(error), False, probe_executed=False)
        except V4L2ProbeError as error:
            return CameraProbeResult(True, True, True, False, str(error), False)
        return CameraProbeResult(
            True,
            True,
            True,
            True,
            report.open_detail,
            report.mode_opened,
            str(report.device.resolved_path),
            report.device.resolution_tier.value,
            report.device.stable_identity,
        )

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
                    actual_hash = sha256_file(path)
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
    if shutil.which("gst-launch-1.0") is None:
        raise HardwareProbeUnavailable("gst-launch-1.0 is not installed; install the GStreamer tools package")
    return LinuxHardwareProbe()


def validate_hardware_config(config: AppConfig, probe: HardwareProbe) -> tuple[ConfigIssue, ...]:
    """Use an injected probe only after static validation has already succeeded."""
    issues: list[ConfigIssue] = []
    for camera_id, camera in sorted(config.cameras.items()):
        result = probe.probe_camera(camera_id, camera)
        path = f"cameras.{camera_id}.device_path"
        if not result.backend_supported or not result.probe_executed:
            issues.append(
                ConfigIssue(
                    "CAMERA_BACKEND_UNAVAILABLE",
                    f"cameras.{camera_id}.adapter",
                    result.detail or "physical backend probe is unavailable",
                )
            )
            continue
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
        elif not result.mode_opened:
            issues.append(
                ConfigIssue(
                    "CAMERA_MODE_OPEN_FAILED",
                    f"cameras.{camera_id}",
                    result.detail or "advertised capture mode did not open",
                )
            )
    issues.extend(probe.validate_runtime_and_artifact(config))
    issues.extend(probe.validate_port_availability(config))
    return tuple(sorted(issues, key=lambda issue: (issue.path, issue.code, issue.message)))
