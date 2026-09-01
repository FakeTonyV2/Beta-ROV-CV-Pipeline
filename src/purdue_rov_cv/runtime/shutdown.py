"""Idempotent shutdown token and bounded ordered cleanup coordination."""

from __future__ import annotations

import signal
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from threading import Event, Lock, Thread
from types import FrameType
from typing import Any, TypeAlias

from .exit_codes import ExitCode
from .state import ComponentState, ComponentStateMachine, StateTransitionResult

SignalHandler: TypeAlias = int | Callable[[int, FrameType | None], Any] | None


class ShutdownToken:
    """Thread-safe, idempotent shutdown request observable by worker loops."""

    def __init__(self) -> None:
        self._event = Event()
        self._lock = Lock()
        self._reason: str | None = None

    def request(self, reason: str) -> bool:
        with self._lock:
            first = not self._event.is_set()
            if first:
                self._reason = reason
                self._event.set()
            return first

    @property
    def is_requested(self) -> bool:
        return self._event.is_set()

    @property
    def reason(self) -> str | None:
        with self._lock:
            return self._reason

    def wait(self, timeout_seconds: float | None = None) -> bool:
        return self._event.wait(timeout_seconds)


@dataclass(frozen=True)
class ShutdownRequestResult:
    first_request: bool
    state_transition: StateTransitionResult | None


@dataclass(frozen=True)
class CleanupFailure:
    hook_name: str
    exception_type: str
    message: str


@dataclass(frozen=True)
class ShutdownResult:
    completed: bool
    timed_out: bool
    completed_hooks: tuple[str, ...]
    failures: tuple[CleanupFailure, ...]
    exit_code: ExitCode


@dataclass(frozen=True)
class _Hook:
    order: int
    name: str
    callback: Callable[[], None]


def _invoke_hook(callback: Callable[[], None], captured: list[Exception]) -> None:
    try:
        callback()
    except Exception as error:  # captured as a structured cleanup failure
        captured.append(error)


class ShutdownCoordinator:
    """Run cleanup hooks in order while bounding the service-facing wait."""

    def __init__(
        self,
        *,
        token: ShutdownToken | None = None,
        state_machine: ComponentStateMachine | None = None,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.token = token or ShutdownToken()
        self._state_machine = state_machine
        self._monotonic = monotonic
        self._hooks: list[_Hook] = []
        self._lock = Lock()
        self._run_lock = Lock()
        self._complete = Event()
        self._result: ShutdownResult | None = None
        self._running = False

    @property
    def accepting_work(self) -> bool:
        return not self.token.is_requested

    def register(self, name: str, callback: Callable[[], None], *, order: int = 0) -> None:
        with self._lock:
            if self.token.is_requested:
                raise RuntimeError("cannot register cleanup after shutdown begins")
            if any(hook.name == name for hook in self._hooks):
                raise ValueError(f"duplicate shutdown hook: {name}")
            self._hooks.append(_Hook(order, name, callback))

    def request(self, reason: str) -> ShutdownRequestResult:
        with self._lock:
            first = self.token.request(reason)
        transition = None
        if first and self._state_machine is not None:
            transition = self._state_machine.transition_to(ComponentState.STOPPING)
        return ShutdownRequestResult(first, transition)

    def run(self, *, timeout_seconds: float = 5.0) -> ShutdownResult:
        if timeout_seconds < 0:
            raise ValueError("timeout_seconds must be non-negative")
        with self._run_lock:
            if self._result is not None:
                return self._result
            if self._running:
                owns_run = False
            else:
                self._running = True
                owns_run = True
        if not owns_run:
            if self._complete.wait(timeout_seconds):
                with self._run_lock:
                    assert self._result is not None
                    return self._result
            return ShutdownResult(False, True, (), (), ExitCode.TEMPORARY_FAILURE)
        if not self.token.is_requested:
            self.request("shutdown coordinator run")
        deadline = self._monotonic() + timeout_seconds
        with self._lock:
            hooks = tuple(sorted(self._hooks, key=lambda hook: (hook.order, hook.name)))
        completed: list[str] = []
        failures: list[CleanupFailure] = []
        timed_out = False
        for hook in hooks:
            remaining = deadline - self._monotonic()
            if remaining <= 0:
                timed_out = True
                break
            captured: list[Exception] = []

            thread = Thread(
                target=_invoke_hook,
                args=(hook.callback, captured),
                name=f"shutdown:{hook.name}",
                daemon=True,
            )
            thread.start()
            thread.join(remaining)
            if thread.is_alive():
                timed_out = True
                break
            completed.append(hook.name)
            if captured:
                error = captured[0]
                failures.append(CleanupFailure(hook.name, type(error).__name__, str(error)))
        if self._state_machine is not None and not timed_out:
            if self._state_machine.state is ComponentState.STOPPING:
                self._state_machine.transition_to(ComponentState.STOPPED)
        exit_code = (
            ExitCode.TEMPORARY_FAILURE
            if timed_out
            else ExitCode.INTERNAL_SOFTWARE_FAILURE
            if failures
            else ExitCode.CLEAN_SHUTDOWN
        )
        result = ShutdownResult(not timed_out, timed_out, tuple(completed), tuple(failures), exit_code)
        with self._run_lock:
            self._result = result
            self._running = False
            self._complete.set()
        return result

    def wait_complete(self, timeout_seconds: float | None = None) -> bool:
        return self._complete.wait(timeout_seconds)


def install_signal_handlers(
    coordinator: ShutdownCoordinator,
    *,
    signals: Iterable[signal.Signals] = (signal.SIGTERM, signal.SIGINT),
) -> dict[signal.Signals, SignalHandler]:
    """Explicitly connect OS signals; importing the runtime package changes nothing globally."""
    previous: dict[signal.Signals, SignalHandler] = {}

    def handle(signum: int, frame: FrameType | None) -> None:
        del frame
        coordinator.request(f"signal:{signal.Signals(signum).name}")

    for signum in signals:
        previous[signum] = signal.getsignal(signum)
        signal.signal(signum, handle)
    return previous


__all__ = [
    "CleanupFailure",
    "ShutdownCoordinator",
    "ShutdownRequestResult",
    "ShutdownResult",
    "ShutdownToken",
    "install_signal_handlers",
]
