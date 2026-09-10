"""Deterministic coverage for the opt-in Linux configuration preflight."""

from __future__ import annotations

import hashlib
import subprocess
from pathlib import Path

from purdue_rov_cv.config import load_config
from purdue_rov_cv.config.probes import LinuxHardwareProbe, validate_hardware_config

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
    def command_runner(command):
        if command[0] == "udevadm":
            path = command[-1]
            return subprocess.CompletedProcess(
                command,
                0,
                f"DEVLINKS={path}\nID_SERIAL=test-camera\nID_V4L_CAPABILITIES=:capture:\n",
                "",
            )
        return subprocess.CompletedProcess(command, 0, V4L2_LISTING, "")

    defaults = {
        "command_runner": command_runner,
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

    assert result.path_exists and result.resolves_to_video_device
    assert result.path_kind_matches and result.capture_tuple_supported and result.mode_opened
    assert result.resolved_path == str(path)
    assert result.resolution_tier == "by_id"


def test_linux_probe_rejects_unlisted_capture_tuple(tmp_path):
    path = tmp_path / "video0"
    path.write_bytes(b"placeholder")
    camera = _config().cameras["front_camera"].model_copy(update={"device_path": path, "frame_rate": 60})

    result = _probe().probe_camera("front_camera", camera)

    assert result.capture_tuple_supported is False
    assert "CAMERA_MODE_UNSUPPORTED" in result.detail


def test_linux_probe_does_not_report_an_invalid_target_as_a_video_device(tmp_path):
    path = tmp_path / "video0"
    path.write_bytes(b"placeholder")
    camera = _config().cameras["front_camera"].model_copy(update={"device_path": path})
    result = _probe(video_device_check=lambda _path: False).probe_camera("front_camera", camera)
    assert result.path_exists
    assert not result.resolves_to_video_device
    assert not result.capture_tuple_supported


def test_linux_probe_marks_missing_tooling_as_not_executed(tmp_path):
    path = tmp_path / "video0"
    path.write_bytes(b"placeholder")
    camera = _config().cameras["front_camera"].model_copy(update={"device_path": path})

    def missing(_command):
        raise FileNotFoundError("missing")

    result = _probe(command_runner=missing).probe_camera("front_camera", camera)
    assert not result.probe_executed
    assert "not installed" in result.detail

    config = _config().model_copy(update={"cameras": {"front_camera": camera}})
    issues = validate_hardware_config(config, _probe(command_runner=missing))
    camera_codes = {issue.code for issue in issues if issue.path.startswith("cameras.")}
    assert camera_codes == {"CAMERA_BACKEND_UNAVAILABLE"}


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
