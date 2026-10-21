#!/usr/bin/env python3
"""Snapshot and restore project-owned deployment files for installer rollback."""

from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path


def _copy(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if source.is_symlink():
        destination.symlink_to(source.readlink())
    elif source.is_dir():
        shutil.copytree(source, destination, symlinks=True)
    else:
        shutil.copy2(source, destination)


def _remove(path: Path) -> None:
    if path.is_symlink() or path.is_file():
        path.unlink(missing_ok=True)
    elif path.is_dir():
        shutil.rmtree(path)


def _owned_systemd_paths(root: Path) -> list[Path]:
    candidates = [path for path in root.rglob("purdue-cv-*")]
    owned: list[Path] = []
    for path in sorted(candidates, key=lambda item: (len(item.parts), str(item))):
        if not any(parent in owned for parent in path.parents):
            owned.append(path)
    return owned


def capture(snapshot: Path, config: Path, chrony: Path, systemd: Path, models: Path) -> None:
    snapshot.mkdir(parents=True, exist_ok=False)
    file_records: dict[str, dict[str, object]] = {}
    systemd_records: list[str] = []
    records: dict[str, object] = {"files": file_records, "systemd": systemd_records}
    for key, source in (("config", config), ("chrony", chrony), ("models", models)):
        exists = source.exists() or source.is_symlink()
        file_records[key] = {"path": str(source), "exists": exists}
        if exists:
            _copy(source, snapshot / key)
    systemd_paths = _owned_systemd_paths(systemd) if systemd.exists() else []
    for source in systemd_paths:
        relative = source.relative_to(systemd)
        _copy(source, snapshot / "systemd" / relative)
        systemd_records.append(str(relative))
    (snapshot / "manifest.json").write_text(json.dumps(records, indent=2) + "\n", encoding="utf-8")


def restore(snapshot: Path, systemd: Path) -> None:
    records = json.loads((snapshot / "manifest.json").read_text(encoding="utf-8"))
    files = records["files"]
    for key in ("config", "chrony", "models"):
        record = files[key]
        destination = Path(record["path"])
        _remove(destination)
        backup = snapshot / key
        if record["exists"]:
            _copy(backup, destination)
    if systemd.exists():
        for path in sorted(_owned_systemd_paths(systemd), key=lambda item: len(item.parts), reverse=True):
            _remove(path)
    for relative in records["systemd"]:
        _copy(snapshot / "systemd" / relative, systemd / relative)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="action", required=True)
    for action in ("capture", "restore"):
        command = subparsers.add_parser(action)
        command.add_argument("--snapshot", type=Path, required=True)
        command.add_argument("--config", type=Path, required=True)
        command.add_argument("--chrony", type=Path, required=True)
        command.add_argument("--systemd", type=Path, required=True)
        command.add_argument("--models", type=Path, required=True)
    args = parser.parse_args()
    if args.action == "capture":
        capture(args.snapshot, args.config, args.chrony, args.systemd, args.models)
    else:
        restore(args.snapshot, args.systemd)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
