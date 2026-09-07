"""Byte-accurate, injectable recording disk-space protection."""

from __future__ import annotations

import shutil
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

GIB = 1024**3
START_MINIMUM_BYTES = 10 * GIB
RUNTIME_MINIMUM_BYTES = 2 * GIB


@dataclass(frozen=True, slots=True)
class DiskSpaceSnapshot:
    path: Path
    free_bytes: int


DiskUsageProvider = Callable[[Path], int]


def _free_bytes(path: Path) -> int:
    return int(shutil.disk_usage(path).free)


class DiskSpaceGuard:
    """Measure the filesystem containing the target, including before it exists."""

    def __init__(self, provider: DiskUsageProvider = _free_bytes) -> None:
        self._provider = provider

    @staticmethod
    def existing_ancestor(target: Path) -> Path:
        candidate = target.absolute()
        while not candidate.exists():
            parent = candidate.parent
            if parent == candidate:
                raise FileNotFoundError(f"no existing ancestor for recording target {target}")
            candidate = parent
        return candidate

    def inspect(self, target: Path) -> DiskSpaceSnapshot:
        measured = self.existing_ancestor(target)
        free = self._provider(measured)
        if isinstance(free, bool) or free < 0:
            raise ValueError("disk usage provider returned invalid free-byte count")
        return DiskSpaceSnapshot(measured, free)

    def allows_start(self, target: Path) -> DiskSpaceSnapshot:
        snapshot = self.inspect(target)
        if snapshot.free_bytes < START_MINIMUM_BYTES:
            raise OSError(
                f"recording requires at least {START_MINIMUM_BYTES} free bytes; filesystem has {snapshot.free_bytes}"
            )
        return snapshot

    def runtime_is_low(self, target: Path) -> DiskSpaceSnapshot:
        return self.inspect(target)


__all__ = [
    "DiskSpaceGuard",
    "DiskSpaceSnapshot",
    "DiskUsageProvider",
    "GIB",
    "RUNTIME_MINIMUM_BYTES",
    "START_MINIMUM_BYTES",
]
