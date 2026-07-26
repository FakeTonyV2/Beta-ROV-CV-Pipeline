from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from purdue_rov_cv.config import AppConfig, load_config

CONFIG_PATH = Path(__file__).parents[2] / "config" / "mission.yaml"


def test_valid_config_loads():
    config = load_config(CONFIG_PATH)

    assert isinstance(config, AppConfig)
    assert config.schema_version == 1
    assert config.cameras.front_camera.width == 1920


def _config_data() -> dict:
    with CONFIG_PATH.open(encoding="utf-8") as stream:
        return yaml.safe_load(stream)


def test_invalid_config_rejects_unknown_key():
    data = _config_data()
    data["unexpected"] = True

    with pytest.raises(ValidationError, match="unexpected"):
        AppConfig.model_validate(data)


def test_invalid_config_rejects_missing_required_key():
    data = _config_data()
    del data["device"]["device_id"]

    with pytest.raises(ValidationError, match="device_id"):
        AppConfig.model_validate(data)


def test_invalid_config_rejects_string_for_int():
    data = _config_data()
    data["clock"]["maximum_offset_ms"] = "10"

    with pytest.raises(ValidationError, match="maximum_offset_ms"):
        AppConfig.model_validate(data)
