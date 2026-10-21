from __future__ import annotations

import hashlib
from pathlib import Path

import pytest
import yaml

from scripts.install_model_artifacts import install_artifacts


def _config(tmp_path: Path, digest: str) -> Path:
    source = Path(__file__).parents[2] / "config" / "mission.yaml"
    data = yaml.safe_load(source.read_text(encoding="utf-8"))
    artifact_path = tmp_path / "opt/purdue-rov-cv/models/crab_dection.onnx"
    data["tasks"]["crab_detection"]["artifact"]["path"] = artifact_path.as_posix()
    data["tasks"]["crab_detection"]["artifact"]["sha256"] = digest
    config = tmp_path / "mission.yaml"
    config.write_text(yaml.safe_dump(data), encoding="utf-8")
    return config


def test_installer_copies_only_hash_pinned_model_and_is_idempotent(tmp_path: Path) -> None:
    model_dir = tmp_path / "models"
    model_dir.mkdir()
    source = model_dir / "crab_dection.onnx"
    source.write_bytes(b"checked model artifact")
    config = _config(tmp_path, hashlib.sha256(source.read_bytes()).hexdigest())

    destination = tmp_path / "opt/purdue-rov-cv/models/crab_dection.onnx"
    assert install_artifacts(config, model_dir) == (destination,)
    assert destination.read_bytes() == source.read_bytes()
    assert install_artifacts(config, model_dir) == ()


def test_installer_rejects_artifact_hash_mismatch(tmp_path: Path) -> None:
    model_dir = tmp_path / "models"
    model_dir.mkdir()
    (model_dir / "crab_dection.onnx").write_bytes(b"wrong artifact")
    config = _config(tmp_path, "0" * 64)

    with pytest.raises(ValueError, match="SHA-256 mismatch"):
        install_artifacts(config, model_dir)
