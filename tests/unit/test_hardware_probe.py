"""Deterministic coverage for the opt-in Linux configuration preflight."""

from __future__ import annotations

import hashlib
import subprocess
from pathlib import Path

from purdue_rov_cv.config import load_config
from purdue_rov_cv.config.probes import LinuxHardwareProbe

MISSION_PATH = Path(__file__).parents[2] / "config" / "mission.yaml"

V4L2_LISTING = """\
ioctl: VIDIOC_ENUM_FMT
\tType: Video Capture
\t[0]: 'H264' (H.264, compressed)
\t\tSize: Discrete 1920x1080
\t\t\tInterval: Discrete 0.033s (30.000 fps)
"""


def _config():
    return load_config(MISSION_PATH, environ={})


def _probe(**overrides):
    defaults = {
        "command_runner": lambda command: subprocess.CompletedProcess(command, 0, V4L2_LISTING, ""),
        "runtime_available": lambda module_name: True,
        "video_device_check": lambda path: True,
        "symlink_check": lambda path: True,
        "port_checker": lambda host, port, protocol: None,
    }
    defaults.update(overrides)
    return LinuxHardwareProbe(**defaults)


def test_linux_probe_checks_exact_v4l2_capture_tuple_without_changing_device(tmp_path):
    path = tmp_path / "video0"
    path.write_bytes(b"placeholder")
    camera = _config().cameras["front_camera"].model_copy(update={"device_path": path})

    result = _probe().probe_camera("front_camera", camera)

    assert result == result.__class__(True, True, True, True)


def test_linux_probe_rejects_unlisted_capture_tuple(tmp_path):
    path = tmp_path / "video0"
    path.write_bytes(b"placeholder")
    camera = _config().cameras["front_camera"].model_copy(update={"device_path": path, "frame_rate": 60})

    result = _probe().probe_camera("front_camera", camera)

    assert result.capture_tuple_supported is False
    assert "not listed" in result.detail


def test_hardware_probe_hashes_enabled_artifacts_and_skips_disabled_tasks(tmp_path):
    artifact = tmp_path / "gate_detector.onnx"
    artifact.write_bytes(b"approved model bytes")
    digest = hashlib.sha256(artifact.read_bytes()).hexdigest()
    config = _config()
    task = config.tasks["gate_detection"]
    checked_task = task.model_copy(
        update={"artifact": task.artifact.model_copy(update={"path": artifact, "sha256": digest})}
    )
    checked_config = config.model_copy(update={"tasks": {"gate_detection": checked_task}})

    assert _probe().validate_runtime_and_artifact(checked_config) == ()

    mismatched_task = checked_task.model_copy(
        update={"artifact": checked_task.artifact.model_copy(update={"sha256": "0" * 64})}
    )
    mismatched_config = config.model_copy(update={"tasks": {"gate_detection": mismatched_task}})
    assert [issue.code for issue in _probe().validate_runtime_and_artifact(mismatched_config)] == [
        "MODEL_HASH_MISMATCH"
    ]

    disabled_task = checked_task.model_copy(update={"enabled": False})
    disabled_config = config.model_copy(update={"tasks": {"gate_detection": disabled_task}})
    assert _probe(runtime_available=lambda module_name: False).validate_runtime_and_artifact(disabled_config) == ()


def test_hardware_probe_reports_port_conflicts():
    config = _config()
    conflicting = _probe(
        port_checker=lambda host, port, protocol: "already bound" if (protocol, port) == ("udp", 5000) else None
    )

    issues = conflicting.validate_port_availability(config)

    assert [(issue.code, issue.path) for issue in issues if issue.code == "PORT_UNAVAILABLE"] == [
        ("PORT_UNAVAILABLE", "cameras.front_camera.stream_index")
    ]
