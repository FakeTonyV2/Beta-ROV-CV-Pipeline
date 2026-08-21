"""Thread-safe canonical component lifecycle state machine."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from threading import RLock

from purdue_rov.cv.v1 import module_state_pb2

from purdue_rov_cv.wire.errors import ErrorCode


class ComponentState(StrEnum):
    STARTING = "STARTING"
    READY = "READY"
    RUNNING = "RUNNING"
    DEGRADED = "DEGRADED"
    ERROR = "ERROR"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"


_NORMAL_TRANSITIONS: dict[ComponentState, frozenset[ComponentState]] = {
    ComponentState.STARTING: frozenset(
        {ComponentState.READY, ComponentState.DEGRADED, ComponentState.ERROR, ComponentState.STOPPING}
    ),
    ComponentState.READY: frozenset({ComponentState.RUNNING, ComponentState.STOPPING, ComponentState.ERROR}),
    ComponentState.RUNNING: frozenset(
        {ComponentState.READY, ComponentState.DEGRADED, ComponentState.ERROR, ComponentState.STOPPING}
    ),
    ComponentState.DEGRADED: frozenset(
        {ComponentState.RUNNING, ComponentState.READY, ComponentState.ERROR, ComponentState.STOPPING}
    ),
    ComponentState.ERROR: frozenset({ComponentState.STOPPING}),
    ComponentState.STOPPING: frozenset({ComponentState.STOPPED}),
    ComponentState.STOPPED: frozenset(),
}


@dataclass(frozen=True)
class StateTransitionResult:
    accepted: bool
    previous: ComponentState
    current: ComponentState
    requested: ComponentState
    error_code: ErrorCode | None = None
    detail: str = ""


TransitionObserver = Callable[[StateTransitionResult], None]


class ComponentStateMachine:
    """Enforce lifecycle transitions; ERROR recovery requires explicit reset."""

    def __init__(
        self,
        initial: ComponentState | str = ComponentState.STARTING,
        *,
        observer: TransitionObserver | None = None,
    ) -> None:
        self._state = ComponentState(initial)
        self._observer = observer
        self._lock = RLock()

    @property
    def state(self) -> ComponentState:
        with self._lock:
            return self._state

    def transition_to(self, requested: ComponentState) -> StateTransitionResult:
        requested = ComponentState(requested)
        with self._lock:
            previous = self._state
            if requested not in _NORMAL_TRANSITIONS[previous]:
                result = StateTransitionResult(
                    False,
                    previous,
                    previous,
                    requested,
                    ErrorCode.INVALID_STATE_TRANSITION,
                    f"{previous} cannot transition normally to {requested}",
                )
            else:
                self._state = requested
                result = StateTransitionResult(True, previous, requested, requested)
        if self._observer is not None:
            self._observer(result)
        return result

    def reset_from_error(self) -> StateTransitionResult:
        """Apply explicit RESET semantics; ordinary transitions cannot perform this move."""
        with self._lock:
            previous = self._state
            if previous is not ComponentState.ERROR:
                result = StateTransitionResult(
                    False,
                    previous,
                    previous,
                    ComponentState.STARTING,
                    ErrorCode.INVALID_STATE_TRANSITION,
                    "reset is valid only from ERROR",
                )
            else:
                self._state = ComponentState.STARTING
                result = StateTransitionResult(
                    True,
                    previous,
                    ComponentState.STARTING,
                    ComponentState.STARTING,
                )
        if self._observer is not None:
            self._observer(result)
        return result


def to_wire_component_state(state: ComponentState) -> module_state_pb2.ComponentState:
    """Map runtime state to the existing protobuf enum without a competing wire model."""
    return {
        ComponentState.STARTING: module_state_pb2.STARTING,
        ComponentState.READY: module_state_pb2.READY,
        ComponentState.RUNNING: module_state_pb2.RUNNING,
        ComponentState.DEGRADED: module_state_pb2.DEGRADED,
        ComponentState.ERROR: module_state_pb2.ERROR,
        ComponentState.STOPPING: module_state_pb2.STOPPING,
        ComponentState.STOPPED: module_state_pb2.STOPPED,
    }[ComponentState(state)]


def from_wire_component_state(value: int) -> ComponentState:
    """Map a known nonzero protobuf state to canonical runtime state."""
    name = module_state_pb2.ComponentState.Name(value)
    if name == "COMPONENT_STATE_UNSPECIFIED":
        raise ValueError("unspecified wire component state has no runtime state")
    return ComponentState(name)


__all__ = [
    "ComponentState",
    "ComponentStateMachine",
    "StateTransitionResult",
    "from_wire_component_state",
    "to_wire_component_state",
]
