"""System health aggregation and the authoritative mission-enable gate."""

from __future__ import annotations

import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass, replace
from enum import StrEnum
from threading import RLock

from purdue_rov_cv.runtime.state import ComponentState

from .clock import ClockStatus


class MissionState(StrEnum):
    DISABLED = "DISABLED"
    ENABLED = "ENABLED"
    DEGRADED = "DEGRADED"


@dataclass(frozen=True, slots=True)
class ComponentHealth:
    component_id: str
    category: str
    state: ComponentState
    required: bool = True
    detail: str = ""
    observed_monotonic: float | None = None


@dataclass(frozen=True, slots=True)
class SystemHealth:
    components: tuple[ComponentHealth, ...]
    clock: ClockStatus
    preflight_completed: bool
    preflight_passed: bool
    preflight_exit_code: int | None
    mission_state: MissionState
    mission_enabled: bool
    cross_device_latency_valid: bool
    failure_reasons: tuple[str, ...]
    observed_monotonic: float
    missing_required_components: tuple[str, ...] = ()
    stale_required_components: tuple[str, ...] = ()
    preflight_run_id: str | None = None

    @property
    def required_errors(self) -> tuple[ComponentHealth, ...]:
        return tuple(item for item in self.components if item.required and item.state is ComponentState.ERROR)

    @property
    def required_components_available(self) -> bool:
        return not self.missing_required_components and not self.stale_required_components


class SystemHealthAggregator:
    """Combine canonical component state without creating a competing lifecycle."""

    def __init__(
        self,
        *,
        required_components: Iterable[str] = (),
        freshness_seconds: float = 3.0,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if freshness_seconds <= 0:
            raise ValueError("component-health freshness must be positive")
        self._monotonic = monotonic
        self._required_components = frozenset(required_components)
        self._freshness_seconds = freshness_seconds
        self._components: dict[str, ComponentHealth] = {}
        self._lock = RLock()

    def update(self, health: ComponentHealth) -> None:
        observed = self._monotonic() if health.observed_monotonic is None else health.observed_monotonic
        with self._lock:
            self._components[health.component_id] = replace(health, observed_monotonic=observed)

    def remove(self, component_id: str) -> None:
        with self._lock:
            self._components.pop(component_id, None)

    def snapshot(
        self,
        *,
        clock: ClockStatus,
        preflight_completed: bool,
        preflight_passed: bool,
        preflight_exit_code: int | None,
        mission_state: MissionState = MissionState.DISABLED,
        mission_enabled: bool = False,
        additional_reasons: tuple[str, ...] = (),
        preflight_run_id: str | None = None,
    ) -> SystemHealth:
        now = self._monotonic()
        with self._lock:
            components = tuple(sorted(self._components.values(), key=lambda item: item.component_id))
        by_id = {item.component_id: item for item in components}
        missing = tuple(sorted(self._required_components - by_id.keys()))
        stale_ids: list[str] = []
        for component_id in self._required_components & by_id.keys():
            observed = by_id[component_id].observed_monotonic
            if observed is None or not 0.0 <= now - observed <= self._freshness_seconds:
                stale_ids.append(component_id)
        stale = tuple(sorted(stale_ids))
        reasons = list(additional_reasons)
        if not clock.synchronized:
            reasons.append(clock.reason or "clock synchronization invalid")
        errors = [item for item in components if item.required and item.state is ComponentState.ERROR]
        reasons.extend(f"required component {item.component_id} is ERROR: {item.detail}" for item in errors)
        reasons.extend(f"required component {item} is missing" for item in missing)
        reasons.extend(f"required component {item} health is stale" for item in stale)
        if preflight_completed and not preflight_passed:
            reasons.append("preflight did not pass")
        return SystemHealth(
            components=components,
            clock=clock,
            preflight_completed=preflight_completed,
            preflight_passed=preflight_passed,
            preflight_exit_code=preflight_exit_code,
            mission_state=mission_state,
            mission_enabled=mission_enabled,
            cross_device_latency_valid=clock.cross_device_latency_valid,
            failure_reasons=tuple(dict.fromkeys(reason for reason in reasons if reason)),
            observed_monotonic=now,
            missing_required_components=missing,
            stale_required_components=stale,
            preflight_run_id=preflight_run_id,
        )


class MissionEnableGate:
    """Single latch controlling mission enable and post-enable degradation."""

    def __init__(self) -> None:
        self._state = MissionState.DISABLED
        self._enabled = False
        self._reason = "preflight has not completed"
        self._accepted_preflight_run_id: str | None = None
        self._invalidated_preflight_run_ids: set[str] = set()
        self._lock = RLock()

    @property
    def state(self) -> MissionState:
        return self._state

    @property
    def enabled(self) -> bool:
        return self._enabled

    @property
    def reason(self) -> str:
        return self._reason

    def authorize(self, health: SystemHealth, *, startup_dependencies_satisfied: bool) -> bool:
        with self._lock:
            reasons: list[str] = []
            if not startup_dependencies_satisfied:
                reasons.append("required startup dependencies are not satisfied")
            if not health.preflight_completed:
                reasons.append("preflight has not completed")
            elif not health.preflight_passed or health.preflight_exit_code != 0:
                reasons.append(f"preflight exit status is {health.preflight_exit_code}")
            if not health.preflight_run_id:
                reasons.append("preflight run identifier is missing")
            if health.preflight_run_id in self._invalidated_preflight_run_ids:
                reasons.append("preflight success was invalidated by a later required-condition failure")
            if not health.clock.synchronized:
                reasons.append(health.clock.reason or "clock synchronization invalid")
            if health.required_errors:
                reasons.append("one or more required components are in ERROR")
            if not health.required_components_available:
                reasons.append("one or more required components are missing or stale")
            if reasons:
                failed_rerun = health.preflight_completed and (
                    not health.preflight_passed or health.preflight_exit_code != 0
                )
                if failed_rerun:
                    if self._accepted_preflight_run_id is not None:
                        self._invalidated_preflight_run_ids.add(self._accepted_preflight_run_id)
                    self._accepted_preflight_run_id = None
                    self._enabled = False
                    self._state = MissionState.DISABLED
                elif self._enabled:
                    self._state = MissionState.DEGRADED
                    if self._accepted_preflight_run_id is not None:
                        self._invalidated_preflight_run_ids.add(self._accepted_preflight_run_id)
                else:
                    self._state = MissionState.DISABLED
                self._reason = "; ".join(reasons)
                return False
            self._enabled = True
            self._state = MissionState.ENABLED
            self._reason = ""
            self._accepted_preflight_run_id = health.preflight_run_id
            return True

    def observe_runtime(self, health: SystemHealth) -> None:
        with self._lock:
            if not self._enabled:
                return
            if not health.clock.synchronized or health.required_errors or not health.required_components_available:
                self._state = MissionState.DEGRADED
                if self._accepted_preflight_run_id is not None:
                    self._invalidated_preflight_run_ids.add(self._accepted_preflight_run_id)
                if not health.clock.synchronized:
                    self._reason = health.clock.reason or "clock synchronization invalid"
                elif health.required_errors:
                    self._reason = "one or more required components entered ERROR"
                else:
                    self._reason = "one or more required components became missing or stale"

    def disable(self, reason: str, *, invalidate_accepted_run: bool = True) -> None:
        """Fail closed and optionally retire the preflight run that enabled the gate."""

        with self._lock:
            if invalidate_accepted_run and self._accepted_preflight_run_id is not None:
                self._invalidated_preflight_run_ids.add(self._accepted_preflight_run_id)
            self._accepted_preflight_run_id = None
            self._enabled = False
            self._state = MissionState.DISABLED
            self._reason = reason


__all__ = [
    "ComponentHealth",
    "MissionEnableGate",
    "MissionState",
    "SystemHealth",
    "SystemHealthAggregator",
]
