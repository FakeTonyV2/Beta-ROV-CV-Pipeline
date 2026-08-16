"""Atomic dynamic-configuration planning; execution belongs to control-plane work."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

from purdue_rov_cv.wire.errors import ErrorCode

from .loader import config_hash
from .models import AppConfig
from .policy import ConfigChange, ConfigDiff, diff_configs


class TransactionPlanState(StrEnum):
    PLANNED = "planned"
    NO_CHANGES = "no_changes"
    REJECTED_STATIC = "rejected_static"
    REJECTED_UNSUPPORTED = "rejected_unsupported"


@dataclass(frozen=True)
class ModuleDynamicUpdate:
    module_id: str
    values: dict[str, object]
    previous_values: dict[str, object]


@dataclass(frozen=True)
class DynamicTransactionPlan:
    current_config_hash: str
    proposed_config_hash: str
    diff: ConfigDiff
    affected_modules: tuple[str, ...]
    dynamic_updates: tuple[ModuleDynamicUpdate, ...]
    static_violations: tuple[ConfigChange, ...]
    unsupported_changes: tuple[ConfigChange, ...]
    apply_order: tuple[str, ...]
    rollback_order: tuple[str, ...]
    per_module_timeout_seconds: float
    state: TransactionPlanState
    event: dict[str, str] | None
    failure_code: ErrorCode | None
    failure_reason: str | None


class DynamicUpdateExecutor(Protocol):
    """Later control-plane implementation must apply and roll back this plan atomically."""

    def apply(self, module_id: str, values: dict[str, object], timeout_seconds: float) -> bool: ...

    def rollback(self, module_id: str, previous_values: dict[str, object], timeout_seconds: float) -> bool: ...


def _affected_modules(active: AppConfig, diff: ConfigDiff) -> tuple[ModuleDynamicUpdate, ...]:
    updates: dict[str, dict[str, object]] = {}
    previous_updates: dict[str, dict[str, object]] = {}
    all_enabled = tuple(sorted(task_id for task_id, task in active.tasks.items() if task.enabled))
    for change in diff.dynamic_changes:
        parts = change.path.split(".")
        if parts[0] == "tasks" and len(parts) >= 3:
            module_id = parts[1]
            if not active.tasks[module_id].enabled:
                continue
            field_path = ".".join(parts[2:])
            updates.setdefault(module_id, {})[field_path] = change.after
            previous_updates.setdefault(module_id, {})[field_path] = change.before
        else:
            for module_id in all_enabled:
                updates.setdefault(module_id, {})[change.path] = change.after
                previous_updates.setdefault(module_id, {})[change.path] = change.before
    return tuple(
        ModuleDynamicUpdate(module_id, updates[module_id], previous_updates[module_id]) for module_id in sorted(updates)
    )


def plan_dynamic_update(active: AppConfig, proposed: AppConfig) -> DynamicTransactionPlan:
    """Create an all-or-nothing plan without sending control messages or persisting config."""
    diff = diff_configs(active, proposed)
    current_hash = config_hash(active)
    proposed_hash = config_hash(proposed)
    static = diff.static_changes
    unsupported = diff.unsupported_changes
    if static:
        return DynamicTransactionPlan(
            current_hash,
            proposed_hash,
            diff,
            (),
            (),
            static,
            unsupported,
            (),
            (),
            1.0,
            TransactionPlanState.REJECTED_STATIC,
            None,
            ErrorCode.RESTART_REQUIRED,
            "static configuration fields changed",
        )
    if unsupported:
        return DynamicTransactionPlan(
            current_hash,
            proposed_hash,
            diff,
            (),
            (),
            (),
            unsupported,
            (),
            (),
            1.0,
            TransactionPlanState.REJECTED_UNSUPPORTED,
            None,
            ErrorCode.INVALID_COMMAND,
            "unsupported runtime configuration fields changed",
        )
    updates = _affected_modules(active, diff)
    modules = tuple(update.module_id for update in updates)
    state = TransactionPlanState.NO_CHANGES if not diff.changes else TransactionPlanState.PLANNED
    event = (
        None
        if not diff.changes
        else {
            "event_type": "configuration_changed",
            "previous_hash": current_hash,
            "configuration_hash": proposed_hash,
        }
    )
    return DynamicTransactionPlan(
        current_hash,
        proposed_hash,
        diff,
        modules,
        updates,
        (),
        (),
        modules,
        tuple(reversed(modules)),
        1.0,
        state,
        event,
        None,
        None,
    )
