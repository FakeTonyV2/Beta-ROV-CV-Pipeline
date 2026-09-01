"""Task-scoped adapter around Phase 2 artifact/runtime validation."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from purdue_rov_cv.config.issues import ConfigIssue
from purdue_rov_cv.config.models import AppConfig, TaskConfig
from purdue_rov_cv.config.probes import HardwareProbe, LinuxHardwareProbe


class ArtifactValidationError(RuntimeError):
    def __init__(self, issues: tuple[ConfigIssue, ...]):
        self.issues = issues
        super().__init__("; ".join(f"{issue.code} {issue.path}: {issue.message}" for issue in issues))


@dataclass(frozen=True)
class ArtifactValidator:
    """Validate one runner's artifact without probing unrelated task instances."""

    probe: HardwareProbe = LinuxHardwareProbe()
    load_probe: Callable[[TaskConfig], None] | None = None

    def validate(self, config: AppConfig, task_id: str) -> None:
        task = config.tasks[task_id]
        issues: list[ConfigIssue] = []
        if task.execution_target != config.device.execution_target:
            issues.append(
                ConfigIssue(
                    "TARGET_INCOMPATIBLE",
                    f"tasks.{task_id}.execution_target",
                    "task execution target does not match this host",
                )
            )
        scoped = config.model_copy(update={"tasks": {task_id: task}})
        issues.extend(self.probe.validate_runtime_and_artifact(scoped))
        if not issues and self.load_probe is not None:
            try:
                self.load_probe(task)
            except Exception as error:
                issues.append(
                    ConfigIssue(
                        "MODEL_LOAD_FAILED",
                        f"tasks.{task_id}.artifact",
                        f"artifact load probe failed: {type(error).__name__}: {error}",
                    )
                )
        if issues:
            raise ArtifactValidationError(tuple(issues))


__all__ = ["ArtifactValidationError", "ArtifactValidator"]
