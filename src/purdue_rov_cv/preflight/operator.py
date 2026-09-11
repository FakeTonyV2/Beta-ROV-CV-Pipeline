"""Minimum surface operator built on the canonical control protocol."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

from purdue_rov.cv.v1 import control_pb2

from purdue_rov_cv.messaging.client import ControlClient


@dataclass(frozen=True, slots=True)
class OperatorResult:
    target_id: str
    command: str
    status: str
    resulting_state: str
    error_code: str
    message: str

    @property
    def succeeded(self) -> bool:
        return self.status == "COMMAND_STATUS_COMPLETED"

    def to_json(self) -> str:
        return json.dumps(
            {
                "command": self.command,
                "error_code": self.error_code,
                "message": self.message,
                "resulting_state": self.resulting_state,
                "status": self.status,
                "target_id": self.target_id,
            },
            sort_keys=True,
        )


class SurfaceOperator:
    """Query and control an enabled module without an alternate test protocol."""

    def __init__(self, client: ControlClient) -> None:
        self.client = client

    @staticmethod
    def _request(target_id: str, command: str) -> control_pb2.CommandRequest:
        request = control_pb2.CommandRequest(
            command_id=uuid4().bytes,
            target_id=target_id,
            issued_time_unix_ns=time.time_ns(),
        )
        getattr(request, command).SetInParent()
        return request

    @staticmethod
    def _result(command: str, response: control_pb2.CommandResponse) -> OperatorResult:
        state = response.resulting_state or "UNSPECIFIED"
        return OperatorResult(
            response.target_id,
            command.upper(),
            control_pb2.CommandStatus.Name(response.status),
            state,
            response.error_code,
            response.message,
        )

    def get_status(self, target_id: str) -> OperatorResult:
        return self._result("GET_STATUS", self.client.send_command(self._request(target_id, "get_status")))

    def start(self, target_id: str) -> OperatorResult:
        return self._result("START", self.client.execute_command(self._request(target_id, "start")))

    def stop(self, target_id: str) -> OperatorResult:
        return self._result("STOP", self.client.execute_command(self._request(target_id, "stop")))


@dataclass(frozen=True, slots=True)
class PreflightObservation:
    available: bool
    run_id: str
    result: str
    exit_code: int | None
    mission_eligible: bool
    detail: str


def read_preflight_observation(path: Path | None) -> PreflightObservation:
    """Read a report as operator evidence, never as authoritative mission state."""

    if path is None:
        return PreflightObservation(False, "", "UNAVAILABLE", None, False, "no preflight report supplied")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        return PreflightObservation(False, "", "UNAVAILABLE", None, False, f"preflight report unavailable: {error}")
    if not isinstance(value, dict):
        return PreflightObservation(False, "", "INVALID", None, False, "preflight report is not an object")
    run_id = value.get("run_id")
    result = value.get("overall_result")
    exit_code = value.get("exit_code")
    checks = value.get("checks")
    decision = value.get("mission_enable_decision")
    valid = (
        value.get("schema_version") == 1
        and isinstance(run_id, str)
        and bool(run_id)
        and result in {"PASS", "FAIL", "ERROR"}
        and isinstance(exit_code, int)
        and isinstance(checks, list)
        and isinstance(decision, bool)
    )
    if not valid:
        return PreflightObservation(False, "", "INVALID", None, False, "preflight report schema is invalid")
    assert isinstance(run_id, str)
    assert isinstance(result, str)
    assert isinstance(exit_code, int)
    assert isinstance(checks, list)
    assert isinstance(decision, bool)
    valid_checks = [
        item
        for item in checks
        if isinstance(item, dict)
        and (
            item.get("status") == "PASS"
            or (item.get("status") in {"WARNING", "UNAVAILABLE"} and item.get("fatal") is False)
        )
    ]
    consistent_pass = result == "PASS" and exit_code == 0 and decision and len(checks) == 20 and len(valid_checks) == 20
    consistent_failure = result != "PASS" and exit_code in {1, 2} and not decision
    if not (consistent_pass or consistent_failure):
        return PreflightObservation(False, run_id, "INVALID", exit_code, False, "preflight report fields disagree")
    return PreflightObservation(
        True,
        run_id,
        result,
        exit_code,
        consistent_pass,
        "preflight passed" if consistent_pass else f"preflight result is {result}",
    )


def read_preflight_decision(path: Path | None) -> tuple[bool, str]:
    """Backward-compatible eligibility helper; this does not report mission state."""

    observation = read_preflight_observation(path)
    return observation.mission_eligible, observation.detail


__all__ = [
    "OperatorResult",
    "PreflightObservation",
    "SurfaceOperator",
    "read_preflight_decision",
    "read_preflight_observation",
]
