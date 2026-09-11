"""Common Phase 11 evidence metadata and boundedness decisions."""

from __future__ import annotations

import hashlib
import json
import platform
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import fmean
from typing import Any


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def repository_state(root: Path) -> dict[str, object]:
    def git(*args: str) -> str:
        result = subprocess.run(["git", *args], cwd=root, capture_output=True, text=True, check=False)
        return result.stdout.strip() if result.returncode == 0 else f"UNVERIFIED: {result.stderr.strip()}"

    revision = git("rev-parse", "HEAD")
    worktree = git("status", "--porcelain")
    revision_file = root / "REVISION"
    status_file = root / "INSTALL_WORKTREE_STATUS"
    if revision.startswith("UNVERIFIED") and revision_file.is_file():
        revision = revision_file.read_text(encoding="ascii").strip()
    if worktree.startswith("UNVERIFIED") and status_file.is_file():
        worktree = status_file.read_text(encoding="utf-8").strip()
    return {"revision": revision, "worktree_status": worktree}


def configuration_hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def software_inventory() -> dict[str, str]:
    inventory = {"python": platform.python_version(), "platform": platform.platform()}
    for package in ("pyzmq", "protobuf", "mcap"):
        try:
            from importlib.metadata import version

            inventory[package] = version(package)
        except Exception:
            inventory[package] = "UNVERIFIED"
    return inventory


@dataclass(frozen=True, slots=True)
class MemoryDecision:
    baseline_bytes: int
    peak_bytes: int
    final_bytes: int
    slope_bytes_per_second: float
    plateau: bool


def analyze_memory(samples: list[tuple[float, int]], *, growth_tolerance_bytes: int = 64 * 1024**2) -> MemoryDecision:
    if len(samples) < 2:
        raise ValueError("at least two RSS samples are required")
    baseline = samples[0][1]
    final = samples[-1][1]
    elapsed = samples[-1][0] - samples[0][0]
    if elapsed <= 0:
        raise ValueError("RSS sample times must increase")
    slope = (final - baseline) / elapsed
    tail = [value for _, value in samples[len(samples) // 2 :]]
    tail_mean = fmean(tail)
    plateau = final - baseline <= growth_tolerance_bytes and abs(final - tail_mean) <= growth_tolerance_bytes / 2
    return MemoryDecision(baseline, max(value for _, value in samples), final, slope, plateau)


def artifact_envelope(
    *,
    kind: str,
    root: Path,
    config_path: Path,
    started_at: str,
    ended_at: str,
    measurements: dict[str, Any],
    events: list[dict[str, Any]],
    result: str,
    normative: bool,
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "kind": kind,
        "start": started_at,
        "end": ended_at,
        "repository": repository_state(root),
        "configuration_sha256": configuration_hash(config_path),
        "software": software_inventory(),
        "hardware": {"machine": platform.machine(), "inventory": "captured by environment validation"},
        "measurements": measurements,
        "events": events,
        "normative_duration_completed": normative,
        "result": result,
    }


def write_artifact(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    try:
        temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


__all__ = [
    "MemoryDecision",
    "analyze_memory",
    "artifact_envelope",
    "configuration_hash",
    "repository_state",
    "software_inventory",
    "utc_now",
    "write_artifact",
]
