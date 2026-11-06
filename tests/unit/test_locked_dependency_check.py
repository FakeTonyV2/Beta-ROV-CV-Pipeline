from __future__ import annotations

from importlib.metadata import PackageNotFoundError
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import check_locked_dependencies as checker


def write_lock(tmp_path: Path, content: str) -> Path:
    path = tmp_path / "requirements.lock"
    path.write_text(content, encoding="utf-8")
    return path


def test_locked_check_ignores_conflicting_unlocked_system_distributions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = write_lock(tmp_path, "numpy==2.5.1\nonnxruntime==1.30.0\n")
    packages = {
        "numpy": SimpleNamespace(version="2.5.1", requires=[]),
        "onnxruntime": SimpleNamespace(version="1.30.0", requires=[]),
    }

    def get_distribution(name: str) -> SimpleNamespace:
        if name not in packages:
            raise PackageNotFoundError(name)
        return packages[name]

    monkeypatch.setattr(checker, "distribution", get_distribution)
    assert checker.check_locked_dependencies(path) == []


def test_locked_check_rejects_missing_or_wrongly_pinned_package(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = write_lock(tmp_path, "numpy==2.5.1\nonnxruntime==1.30.0\n")
    packages = {"numpy": SimpleNamespace(version="2.4.0", requires=[])}

    def get_distribution(name: str) -> SimpleNamespace:
        if name not in packages:
            raise PackageNotFoundError(name)
        return packages[name]

    monkeypatch.setattr(checker, "distribution", get_distribution)
    errors = checker.check_locked_dependencies(path)
    assert any("numpy==2.4.0" in error for error in errors)
    assert any("onnxruntime is missing" in error for error in errors)


def test_locked_check_rejects_unsatisfied_dependency_of_locked_package(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = write_lock(tmp_path, "onnxruntime==1.30.0\n")
    packages = {
        "onnxruntime": SimpleNamespace(version="1.30.0", requires=["numpy<1.28"]),
        "numpy": SimpleNamespace(version="2.5.1", requires=[]),
    }

    monkeypatch.setattr(checker, "distribution", lambda name: packages[name])
    errors = checker.check_locked_dependencies(path)
    assert errors == ["onnxruntime requires numpy<1.28, but numpy==2.5.1 is installed"]


def test_lock_parser_requires_exact_pins(tmp_path: Path) -> None:
    path = write_lock(tmp_path, "numpy>=2\n")
    with pytest.raises(ValueError, match="exact == pins"):
        checker.parse_lock(path)
