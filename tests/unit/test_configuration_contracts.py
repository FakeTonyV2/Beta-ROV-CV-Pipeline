"""Public-behavior coverage for Phase 2 configuration contracts."""

from __future__ import annotations

import copy
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

import purdue_rov_cv.cli as cli
from purdue_rov_cv.config import (
    diff_configs,
    load_config,
    parse_config_data,
    plan_dynamic_update,
    resolve_config_path,
)
from purdue_rov_cv.config.issues import (
    ConfigFileError,
    ConfigIssue,
    ConfigSchemaError,
    ConfigStaticValidationError,
    ConfigYamlError,
)
from purdue_rov_cv.config.policy import ChangeClass, classify_field_path
from purdue_rov_cv.config.ports import derive_stream_allocation
from purdue_rov_cv.config.probes import CameraProbeResult, validate_hardware_config
from purdue_rov_cv.config.transactions import TransactionPlanState
from purdue_rov_cv.runtime.exit_codes import ExitCode
from purdue_rov_cv.wire.errors import ErrorCode

MISSION_PATH = Path(__file__).parents[2] / "config" / "mission.yaml"
DEVELOPMENT_PATH = Path(__file__).parents[2] / "config" / "development.yaml"
FIXTURE_ROOT = Path(__file__).parents[1] / "fixtures" / "config"


def _data() -> dict:
    return yaml.safe_load(MISSION_PATH.read_text(encoding="utf-8"))


def _valid_config():
    return parse_config_data(_data())


def test_checked_in_valid_and_invalid_fixtures():
    assert load_config(DEVELOPMENT_PATH, environ={}).device.execution_target == "surface_laptop"
    assert load_config(FIXTURE_ROOT / "valid" / "single_camera.yaml", environ={}).tasks["gate_detection"].enabled
    assert (
        load_config(FIXTURE_ROOT / "valid" / "fallback_camera.yaml", environ={})
        .cameras["front_camera"]
        .device_path_kind.value
        == "fallback"
    )
    for name in ("duplicate.yaml", "malformed.yaml", "non_mapping.yaml"):
        with pytest.raises(ConfigYamlError):
            load_config(FIXTURE_ROOT / "invalid" / name, environ={})


def _write_yaml(tmp_path: Path, name: str, data: object) -> Path:
    path = tmp_path / name
    path.write_text(yaml.safe_dump(data), encoding="utf-8")
    return path


def test_loader_resolution_and_permitted_environment_overrides(tmp_path):
    path = _write_yaml(tmp_path, "mission.yaml", _data())
    assert resolve_config_path(environ={}) == Path("/etc/purdue-rov-cv/mission.yaml")
    assert resolve_config_path(environ={"PURDUE_ROV_CV_CONFIG": str(path)}) == path
    config = load_config(environ={"PURDUE_ROV_CV_CONFIG": str(path), "PURDUE_ROV_CV_LOG_LEVEL": "DEBUG"})
    assert config.diagnostics.log_level.value == "DEBUG"
    with pytest.raises(ConfigSchemaError, match="CONFIG_ENV_INVALID"):
        load_config(path, environ={"PURDUE_ROV_CV_CAMERA": "unsafe"})


@pytest.mark.parametrize("content", ["", "- item\n", "schema_version: ["])
def test_loader_rejects_empty_non_mapping_and_malformed_yaml(tmp_path, content):
    path = tmp_path / "invalid.yaml"
    path.write_text(content, encoding="utf-8")
    with pytest.raises(ConfigYamlError):
        load_config(path, environ={})


def test_loader_rejects_duplicate_yaml_keys_with_canonical_config_error(tmp_path):
    path = tmp_path / "duplicate.yaml"
    path.write_text("schema_version: 1\nschema_version: 1\n", encoding="utf-8")

    with pytest.raises(ConfigYamlError) as raised:
        load_config(path, environ={})

    assert raised.value.error_code is ErrorCode.CONFIG_INVALID
    assert raised.value.issues[0].code == "CONFIG_YAML_INVALID"
    assert "duplicate key" in raised.value.issues[0].message


@pytest.mark.parametrize(
    "mutate",
    [
        lambda data: data.update({"unknown": True}),
        lambda data: data["clock"].update({"maximum_offset_ms": "10"}),
        lambda data: data["debug_snapshots"].update({"enabled": "true"}),
        lambda data: data["cameras"]["front_camera"].update({"adapter": "unknown"}),
        lambda data: data["tasks"]["gate_detection"].update({"module_class": "   "}),
    ],
)
def test_pydantic_strictness_and_enums(mutate):
    data = _data()
    mutate(data)
    with pytest.raises(ConfigSchemaError):
        parse_config_data(data)


@pytest.mark.parametrize(
    "mutate, code",
    [
        (lambda data: data.update({"schema_version": 2}), "UNSUPPORTED_SCHEMA_VERSION"),
        (lambda data: data["cameras"].__setitem__("Bad-ID", data["cameras"]["front_camera"]), "IDENTIFIER_INVALID"),
        (lambda data: data["tasks"]["gate_detection"].update({"input_camera": "missing"}), "TASK_CAMERA_MISSING"),
        (
            lambda data: data["tasks"]["gate_detection"].update({"publish_topic": "cv.result.wrong.front_camera"}),
            "TASK_TOPIC_MISMATCH",
        ),
        (
            lambda data: data["cameras"]["front_camera"].update({"device_path": "/dev/video0"}),
            "CAMERA_PATH_ENUMERATION_DEPENDENT",
        ),
        (
            lambda data: data["cameras"]["front_camera"].update({"device_path": "relative-camera"}),
            "CONFIG_SCHEMA_INVALID",
        ),
        (
            lambda data: data["cameras"]["front_camera"].update(
                {"device_path_kind": "fallback", "device_path": "/dev/v4l/by-id/camera"}
            ),
            "CAMERA_PATH_KIND_MISMATCH",
        ),
        (lambda data: data["tasks"]["gate_detection"]["artifact"].update({"sha256": "bad"}), "CONFIG_SCHEMA_INVALID"),
        (
            lambda data: data["tasks"]["gate_detection"]["artifact"].update(
                {"format": "tensorrt", "runtime": "onnxruntime"}
            ),
            "ARTIFACT_RUNTIME_INCOMPATIBLE",
        ),
        (
            lambda data: data["messaging"]["broker"].update({"publisher_endpoint": "tcp://rov.local:5555"}),
            "ENDPOINT_HOST_INVALID",
        ),
    ],
)
def test_static_validation_reports_stable_issue_codes(mutate, code):
    data = _data()
    mutate(data)
    with pytest.raises((ConfigStaticValidationError, ConfigSchemaError)) as raised:
        parse_config_data(data)
    assert code in str(raised.value)


def test_valid_fallback_path_does_not_need_hardware():
    data = _data()
    data["cameras"]["front_camera"].update(
        {"device_path_kind": "fallback", "device_path": "/dev/purdue-rov-cv/front_camera"}
    )
    assert parse_config_data(data).cameras["front_camera"].device_path.as_posix() == "/dev/purdue-rov-cv/front_camera"


def test_documented_oakd_variation_is_valid_static_configuration():
    data = _data()
    data["cameras"]["front_camera"].update(
        {
            "adapter": "oakd",
            "device_path": "/dev/v4l/by-id/usb-luxonis-oakd",
        }
    )

    assert parse_config_data(data).cameras["front_camera"].adapter.value == "oakd"


@pytest.mark.parametrize(
    "path",
    [
        "/dev/v4l/by-id/../video0",
        "/dev/v4l/by-id/camera/",
        "/dev/v4l//by-id/camera",
    ],
)
def test_camera_paths_reject_traversal_trailing_and_empty_components(path):
    data = _data()
    data["cameras"]["front_camera"]["device_path"] = path

    with pytest.raises(ConfigSchemaError, match="device_path"):
        parse_config_data(data)


def test_port_derivation_collision_and_more_than_eight_cameras():
    allocation = derive_stream_allocation("front_camera", 3)
    assert (allocation.rtp_port, allocation.rtcp_port, allocation.rtp_payload_type) == (5006, 5007, 99)
    data = _data()
    data["camera_limits"] = {"maximum_configured": 16, "maximum_active": 16}
    original = data["cameras"]["front_camera"]
    for index in range(1, 10):
        camera_id = f"camera_{index}"
        camera = copy.deepcopy(original)
        camera.update(
            {"stream_index": index, "device_path_kind": "fallback", "device_path": f"/dev/purdue-rov-cv/{camera_id}"}
        )
        data["cameras"][camera_id] = camera
    assert len(parse_config_data(data).cameras) == 10
    data["cameras"]["camera_1"]["stream_index"] = 0
    with pytest.raises(ConfigStaticValidationError, match="STREAM_INDEX_DUPLICATE"):
        parse_config_data(data)


def test_service_port_collision_is_reported_without_binding_a_socket():
    data = _data()
    data["messaging"]["broker"]["publisher_endpoint"] = "tcp://192.168.50.2:5556"
    with pytest.raises(ConfigStaticValidationError, match="PORT_COLLISION"):
        parse_config_data(data)


def test_wildcard_service_endpoint_collides_with_specific_address_without_binding():
    data = _data()
    data["messaging"]["broker"]["publisher_endpoint"] = "tcp://0.0.0.0:5560"

    with pytest.raises(ConfigStaticValidationError, match="PORT_COLLISION"):
        parse_config_data(data)


@pytest.mark.parametrize("stream_index, code", [(32, "RTP_PAYLOAD_TYPE_OUT_OF_RANGE"), (30_500, "PORT_OUT_OF_RANGE")])
def test_stream_boundaries(stream_index, code):
    data = _data()
    data["cameras"]["front_camera"]["stream_index"] = stream_index
    with pytest.raises(ConfigStaticValidationError, match=code):
        parse_config_data(data)


def test_camera_operational_limits_are_configured_not_hard_coded():
    data = _data()
    data["camera_limits"] = {"maximum_configured": 1, "maximum_active": 2}
    with pytest.raises(ConfigStaticValidationError, match="CAMERA_LIMIT_INVALID"):
        parse_config_data(data)


def test_recursive_diff_and_dynamic_transaction_plan():
    active = _valid_config()
    dynamic_data = _data()
    dynamic_data["tasks"]["gate_detection"]["dynamic"]["confidence_threshold"] = 0.7
    dynamic_data["debug_snapshots"]["jpeg_quality"] = 65
    proposed = parse_config_data(dynamic_data)
    diff = diff_configs(active, proposed)
    assert [change.path for change in diff.changes] == [
        "debug_snapshots.jpeg_quality",
        "tasks.gate_detection.dynamic.confidence_threshold",
    ]
    assert all(change.classification is ChangeClass.DYNAMIC for change in diff.changes)
    plan = plan_dynamic_update(active, proposed)
    assert plan.state is TransactionPlanState.PLANNED
    assert plan.affected_modules == ("gate_detection",)
    assert plan.dynamic_updates[0].values["dynamic.confidence_threshold"] == 0.7
    assert plan.dynamic_updates[0].previous_values["dynamic.confidence_threshold"] == 0.6
    assert plan.rollback_order == ("gate_detection",)
    assert plan.event and plan.event["event_type"] == "configuration_changed"


def test_dynamic_change_for_disabled_task_does_not_target_an_absent_module():
    active_data = _data()
    active_data["tasks"]["gate_detection"]["enabled"] = False
    active = parse_config_data(active_data)
    proposed_data = copy.deepcopy(active_data)
    proposed_data["tasks"]["gate_detection"]["dynamic"]["confidence_threshold"] = 0.7

    plan = plan_dynamic_update(active, parse_config_data(proposed_data))

    assert plan.state is TransactionPlanState.PLANNED
    assert plan.affected_modules == ()
    assert plan.dynamic_updates == ()
    assert plan.event and plan.event["configuration_hash"] == plan.proposed_config_hash


def test_unlisted_dynamic_field_path_is_static_by_safe_default():
    assert classify_field_path("tasks.gate_detection.dynamic.future_threshold") is ChangeClass.STATIC


def test_static_and_unsupported_changes_reject_an_atomic_plan():
    active = _valid_config()
    static_data = _data()
    static_data["cameras"]["front_camera"]["width"] = 1280
    static_plan = plan_dynamic_update(active, parse_config_data(static_data))
    assert static_plan.state is TransactionPlanState.REJECTED_STATIC
    assert static_plan.failure_code is ErrorCode.RESTART_REQUIRED
    unsupported_data = _data()
    unsupported_data["recording"]["enabled"] = False
    unsupported_plan = plan_dynamic_update(active, parse_config_data(unsupported_data))
    assert unsupported_plan.state is TransactionPlanState.REJECTED_UNSUPPORTED
    assert unsupported_plan.failure_code is ErrorCode.INVALID_COMMAND


def test_cli_exit_codes_and_hardware_probe_boundary(capsys, monkeypatch):
    class PassingProbe:
        def probe_camera(self, camera_id, camera):
            return CameraProbeResult(True, True, True, True)

        def validate_runtime_and_artifact(self, config):
            return ()

        def validate_port_availability(self, config):
            return ()

    class FailingProbe:
        def probe_camera(self, camera_id, camera):
            return CameraProbeResult(False, False, False, False, "test-only hardware failure")

        def validate_runtime_and_artifact(self, config):
            return ()

        def validate_port_availability(self, config):
            return ()

    assert cli.main(["config", "validate", str(MISSION_PATH)]) is ExitCode.CLEAN_SHUTDOWN
    assert "valid:" in capsys.readouterr().out
    monkeypatch.setattr(cli, "create_default_hardware_probe", PassingProbe)
    assert cli.main(["config", "validate", str(MISSION_PATH), "--probe-hardware"]) is ExitCode.CLEAN_SHUTDOWN
    assert "hardware=ok" in capsys.readouterr().out
    monkeypatch.setattr(cli, "create_default_hardware_probe", FailingProbe)
    assert cli.main(["config", "validate", str(MISSION_PATH), "--probe-hardware"]) is ExitCode.INVALID_CONFIGURATION
    assert "CAMERA_NOT_FOUND" in capsys.readouterr().err
    monkeypatch.setattr(
        cli,
        "create_default_hardware_probe",
        lambda: (_ for _ in ()).throw(cli.HardwareProbeUnavailable("test-only unavailable")),
    )
    assert cli.main(["config", "validate", str(MISSION_PATH), "--probe-hardware"]) is ExitCode.INVALID_CONFIGURATION
    assert "HARDWARE_PROBE_UNAVAILABLE" in capsys.readouterr().err
    assert cli.main(["config", "validate", "/missing/mission.yaml"]) is ExitCode.INVALID_CONFIGURATION
    assert "CONFIG_INVALID CONFIG_FILE_NOT_FOUND" in capsys.readouterr().err


def test_cli_argument_internal_and_io_exit_contracts(capsys, monkeypatch):
    with pytest.raises(SystemExit) as invalid_arguments:
        cli.main(["not-a-command"])
    assert invalid_arguments.value.code == ExitCode.INVALID_ARGUMENTS

    def unreadable_config(path):
        del path
        raise ConfigFileError([ConfigIssue("CONFIG_FILE_READ_ERROR", "<file>", "unreadable")])

    monkeypatch.setattr(cli, "load_config", unreadable_config)
    assert cli.main(["config", "validate", "mission.yaml"]) is ExitCode.IO_FAILURE

    monkeypatch.setattr(cli, "main", lambda argv=None: (_ for _ in ()).throw(RuntimeError("unexpected")))
    assert cli.entrypoint([]) == ExitCode.INTERNAL_SOFTWARE_FAILURE
    assert "INTERNAL_ERROR <cli>: RuntimeError: unexpected" in capsys.readouterr().err


@pytest.mark.parametrize(("interval", "valid"), [(499, False), (500, True), (5_000, True), (5_001, False)])
def test_diagnostics_publish_interval_uses_runtime_contract_boundaries(interval, valid):
    data = _data()
    data["diagnostics"]["publish_interval_ms"] = interval

    if valid:
        assert parse_config_data(data).diagnostics.publish_interval_ms == interval
    else:
        with pytest.raises(ConfigSchemaError, match="publish_interval_ms"):
            parse_config_data(data)


def test_installed_cli_entry_point_validates_configuration():
    command = Path(sys.executable).with_name("rov-cv")
    result = subprocess.run(
        [command, "config", "validate", str(MISSION_PATH)],
        capture_output=True,
        check=False,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert "valid:" in result.stdout


def test_injected_hardware_probe_is_separate_from_static_validation():
    class MissingCameraProbe:
        def probe_camera(self, camera_id, camera):
            return CameraProbeResult(False, False, False, False, "test probe")

        def validate_runtime_and_artifact(self, config):
            return ()

        def validate_port_availability(self, config):
            return ()

    issues = validate_hardware_config(_valid_config(), MissingCameraProbe())
    assert {issue.code for issue in issues} == {"CAMERA_NOT_FOUND"}
