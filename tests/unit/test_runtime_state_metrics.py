"""State, metrics, rate-limit, and publisher identity contracts."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from uuid import UUID

import pytest

from purdue_rov_cv.config.models import (
    DIAGNOSTICS_PUBLISH_INTERVAL_MAX_MS,
    DIAGNOSTICS_PUBLISH_INTERVAL_MIN_MS,
)
from purdue_rov_cv.runtime.exit_codes import ExitCode
from purdue_rov_cv.runtime.metrics import (
    HEALTH_INTERVAL_MAX_MS,
    HEALTH_INTERVAL_MIN_MS,
    MetricKind,
    RuntimeMetrics,
)
from purdue_rov_cv.runtime.publisher import PublisherSequence
from purdue_rov_cv.runtime.rate_limit import WarningRateLimiter
from purdue_rov_cv.runtime.state import (
    ComponentState,
    ComponentStateMachine,
    from_wire_component_state,
    to_wire_component_state,
)
from purdue_rov_cv.wire.errors import ErrorCode

NORMAL_TRANSITIONS = (
    (ComponentState.STARTING, ComponentState.READY),
    (ComponentState.STARTING, ComponentState.DEGRADED),
    (ComponentState.STARTING, ComponentState.ERROR),
    (ComponentState.STARTING, ComponentState.STOPPING),
    (ComponentState.READY, ComponentState.RUNNING),
    (ComponentState.READY, ComponentState.STOPPING),
    (ComponentState.READY, ComponentState.ERROR),
    (ComponentState.RUNNING, ComponentState.READY),
    (ComponentState.RUNNING, ComponentState.DEGRADED),
    (ComponentState.RUNNING, ComponentState.ERROR),
    (ComponentState.RUNNING, ComponentState.STOPPING),
    (ComponentState.DEGRADED, ComponentState.RUNNING),
    (ComponentState.DEGRADED, ComponentState.READY),
    (ComponentState.DEGRADED, ComponentState.ERROR),
    (ComponentState.DEGRADED, ComponentState.STOPPING),
    (ComponentState.ERROR, ComponentState.STOPPING),
    (ComponentState.STOPPING, ComponentState.STOPPED),
)
INVALID_TRANSITIONS = tuple(
    (source, target)
    for source in ComponentState
    for target in ComponentState
    if (source, target) not in NORMAL_TRANSITIONS
)


@pytest.mark.parametrize(
    ("source", "target"),
    NORMAL_TRANSITIONS,
)
def test_every_normal_component_state_transition(source, target):
    machine = ComponentStateMachine(source)
    result = machine.transition_to(target)
    assert result.accepted
    assert machine.state is target


def test_invalid_transition_preserves_state_and_explicit_reset_is_required():
    machine = ComponentStateMachine(ComponentState.ERROR)
    rejected = machine.transition_to(ComponentState.STARTING)
    assert not rejected.accepted
    assert rejected.error_code is ErrorCode.INVALID_STATE_TRANSITION
    assert machine.state is ComponentState.ERROR
    assert machine.reset_from_error().accepted
    assert machine.state is ComponentState.STARTING


@pytest.mark.parametrize(("source", "target"), INVALID_TRANSITIONS)
def test_every_invalid_normal_transition_is_rejected_without_changing_state(source, target):
    machine = ComponentStateMachine(source)

    result = machine.transition_to(target)

    assert not result.accepted
    assert result.error_code is ErrorCode.INVALID_STATE_TRANSITION
    assert result.previous is source
    assert result.current is source
    assert machine.state is source


def test_state_machine_canonicalizes_string_initial_state_for_reset_semantics():
    machine = ComponentStateMachine("ERROR")

    assert machine.state is ComponentState.ERROR
    assert machine.reset_from_error().accepted
    assert machine.state is ComponentState.STARTING
    repeated = machine.transition_to(ComponentState.STARTING)
    assert not repeated.accepted
    assert machine.state is ComponentState.STARTING


def test_state_observation_is_safe_during_concurrent_reads():
    machine = ComponentStateMachine(ComponentState.READY)
    with ThreadPoolExecutor(max_workers=8) as executor:
        states = list(executor.map(lambda _: machine.state, range(100)))
    assert states == [ComponentState.READY] * 100


@pytest.mark.parametrize("state", list(ComponentState))
def test_runtime_state_maps_to_existing_wire_enum(state):
    assert from_wire_component_state(to_wire_component_state(state)) is state


def test_exit_codes_are_exactly_the_supervisor_contract():
    assert {member.name: member.value for member in ExitCode} == {
        "CLEAN_SHUTDOWN": 0,
        "INVALID_ARGUMENTS": 64,
        "INTERNAL_SOFTWARE_FAILURE": 70,
        "IO_FAILURE": 74,
        "TEMPORARY_FAILURE": 75,
        "INVALID_CONFIGURATION": 78,
    }


class FakeClock:
    def __init__(self, value: float = 0.0):
        self.value = value

    def __call__(self) -> float:
        return self.value


def test_runtime_metrics_counter_gauge_observation_and_snapshot():
    clock = FakeClock(10.0)
    metrics = RuntimeMetrics(monotonic=clock)
    assert metrics.kind("frames_received") is MetricKind.COUNTER
    assert metrics.increment("frames_received") == 1
    assert metrics.increment("frames_received", 2) == 3
    with pytest.raises(ValueError):
        metrics.increment("frames_received", -1)
    with pytest.raises(KeyError):
        metrics.increment("cpu_temperature_c")
    metrics.set_gauge("cpu_temperature_c", 42.5)
    metrics.set_metadata("state", "RUNNING")
    metrics.observe_processing_ms(10.0)
    metrics.observe_processing_ms(20.0)
    clock.value = 12.5
    snapshot = metrics.snapshot().values
    assert snapshot["frames_received"] == 3
    assert snapshot["cpu_temperature_c"] == 42.5
    assert snapshot["state"] == "RUNNING"
    assert snapshot["average_processing_ms"] == 15.0
    assert snapshot["p95_processing_ms"] == 20.0
    assert snapshot["uptime_seconds"] == 2.5
    assert list(snapshot) == sorted(snapshot)


def test_processing_metrics_use_lifetime_average_and_bounded_percentile_samples():
    metrics = RuntimeMetrics(processing_sample_capacity=3)
    for duration in (1.0, 2.0, 100.0, 5.0):
        metrics.observe_processing_ms(duration)

    snapshot = metrics.snapshot().values
    assert metrics.processing_sample_count == 3
    assert snapshot["average_processing_ms"] == 27.0
    assert snapshot["p95_processing_ms"] == 100.0


def test_metrics_reject_noncanonical_state_and_error_metadata():
    metrics = RuntimeMetrics()
    metrics.set_metadata("state", ComponentState.RUNNING)
    metrics.set_metadata("last_error_code", ErrorCode.MODULE_BUSY)
    with pytest.raises(ValueError, match="canonical component state"):
        metrics.set_metadata("state", "BROKEN")
    with pytest.raises(ValueError, match="canonical error code"):
        metrics.set_metadata("last_error_code", "TYPO")


def test_health_interval_uses_phase2_configuration_bounds():
    assert HEALTH_INTERVAL_MIN_MS == DIAGNOSTICS_PUBLISH_INTERVAL_MIN_MS == 500
    assert HEALTH_INTERVAL_MAX_MS == DIAGNOSTICS_PUBLISH_INTERVAL_MAX_MS == 5_000


def test_metrics_concurrent_counter_updates_are_not_lost():
    metrics = RuntimeMetrics()
    with ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(lambda _: metrics.increment("messages_sent"), range(1000)))
    assert metrics.snapshot().values["messages_sent"] == 1000


def test_warning_rate_limiter_boundaries_suppression_and_independent_keys():
    clock = FakeClock()
    limiter = WarningRateLimiter(monotonic=clock, max_keys=2)
    assert limiter.check(("A", "source")).emit
    suppressed = limiter.check(("A", "source"))
    assert not suppressed.emit and suppressed.suppressed_count == 1
    assert limiter.check(("B", "source")).emit
    clock.value = 1.0
    emitted = limiter.check(("A", "source"))
    assert emitted.emit and emitted.suppressed_count == 1
    assert limiter.check(("C", "source")).emit
    assert limiter.tracked_keys == 2


def test_warning_rate_limiter_is_thread_safe():
    clock = FakeClock()
    limiter = WarningRateLimiter(monotonic=clock)
    with ThreadPoolExecutor(max_workers=8) as executor:
        decisions = list(executor.map(lambda _: limiter.check("same"), range(100)))
    assert sum(decision.emit for decision in decisions) == 1
    assert max(decision.suppressed_count for decision in decisions) == 99


def test_publisher_session_and_sequence_are_thread_safe_and_instance_scoped():
    first_uuid = UUID("123e4567-e89b-12d3-a456-426614174000")
    publisher = PublisherSequence(uuid_factory=lambda: first_uuid)
    assert publisher.session_id == first_uuid.bytes
    assert len(publisher.session_id) == 16
    assert publisher.next_attempt().sequence_number == 0
    assert publisher.next_attempt().sequence_number == 1
    with ThreadPoolExecutor(max_workers=8) as executor:
        numbers = list(executor.map(lambda _: publisher.next_attempt().sequence_number, range(100)))
    assert sorted(numbers) == list(range(2, 102))
    second = PublisherSequence()
    assert second.next_attempt().sequence_number == 0
    assert second.session_id != publisher.session_id
