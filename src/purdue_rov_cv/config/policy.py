"""Central field policy and recursive, deterministic configuration differ."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from .models import AppConfig


class ChangeClass(StrEnum):
    STATIC = "static"
    DYNAMIC = "dynamic"
    UNSUPPORTED = "unsupported"


@dataclass(frozen=True)
class ConfigChange:
    path: str
    before: Any
    after: Any
    classification: ChangeClass


@dataclass(frozen=True)
class ConfigDiff:
    changes: tuple[ConfigChange, ...]

    @property
    def static_changes(self) -> tuple[ConfigChange, ...]:
        return tuple(change for change in self.changes if change.classification is ChangeClass.STATIC)

    @property
    def dynamic_changes(self) -> tuple[ConfigChange, ...]:
        return tuple(change for change in self.changes if change.classification is ChangeClass.DYNAMIC)

    @property
    def unsupported_changes(self) -> tuple[ConfigChange, ...]:
        return tuple(change for change in self.changes if change.classification is ChangeClass.UNSUPPORTED)


_DYNAMIC_PATTERNS = (
    "tasks.*.max_input_fps",
    "tasks.*.dynamic.confidence_threshold",
    "diagnostics.publish_interval_ms",
    "debug_snapshots.enabled",
    "debug_snapshots.maximum_rate_hz",
    "debug_snapshots.jpeg_quality",
)
_STATIC_PATTERNS = (
    "schema_version",
    "device.*",
    "network.*",
    "clock.*",
    "messaging.*",
    "camera_limits.*",
    "cameras.*",
    "tasks.*.module_class",
    "tasks.*.enabled",
    "tasks.*.input_camera",
    "tasks.*.execution_target",
    "tasks.*.processing_deadline_ms",
    "tasks.*.publish_topic",
    "tasks.*.payload_type",
    "tasks.*.artifact.*",
)


def _matches(path: str, pattern: str) -> bool:
    path_parts = path.split(".")
    pattern_parts = pattern.split(".")
    return len(path_parts) == len(pattern_parts) and all(
        expected == "*" or actual == expected for actual, expected in zip(path_parts, pattern_parts, strict=True)
    )


def classify_field_path(path: str) -> ChangeClass:
    """Classify a leaf or mapping-addition path using the one central policy."""
    if any(_matches(path, pattern) for pattern in _DYNAMIC_PATTERNS):
        return ChangeClass.DYNAMIC
    if any(_matches(path, pattern) for pattern in _STATIC_PATTERNS):
        return ChangeClass.STATIC
    # Adding/removing a mapping item is structural even if the item has future dynamic leaves.
    if path.startswith("cameras.") or path.startswith("tasks."):
        return ChangeClass.STATIC
    return ChangeClass.UNSUPPORTED


_MISSING = object()


def _diff_values(before: Any, after: Any, path: str, output: list[ConfigChange]) -> None:
    if isinstance(before, dict) and isinstance(after, dict):
        for key in sorted(set(before) | set(after)):
            next_path = f"{path}.{key}" if path else key
            _diff_values(before.get(key, _MISSING), after.get(key, _MISSING), next_path, output)
        return
    if before is _MISSING or after is _MISSING or before != after:
        output.append(
            ConfigChange(
                path=path,
                before=None if before is _MISSING else before,
                after=None if after is _MISSING else after,
                classification=classify_field_path(path),
            )
        )


def diff_configs(active: AppConfig, proposed: AppConfig) -> ConfigDiff:
    """Compare validated models recursively, including dynamic camera/task mappings."""
    changes: list[ConfigChange] = []
    _diff_values(active.model_dump(mode="json"), proposed.model_dump(mode="json"), "", changes)
    return ConfigDiff(tuple(sorted(changes, key=lambda change: change.path)))
