"""Canonical task-module interface with no platform resource ownership."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import ClassVar, TypeAlias

import numpy as np
from google.protobuf.message import Message

from purdue_rov_cv.config.models import TaskConfig

NativeScalar: TypeAlias = int | float | bool | str | None
NativeValue: TypeAlias = NativeScalar | list["NativeValue"] | dict[str, "NativeValue"]
DynamicConfig: TypeAlias = dict[str, NativeValue]


@dataclass(frozen=True)
class Frame:
    """Process-private pixels plus the identity required on published results."""

    pixels: np.ndarray
    camera_id: str
    camera_session_id: bytes
    frame_number: int
    capture_time_unix_ns: int
    capture_monotonic_ns: int

    def __post_init__(self) -> None:
        if len(self.camera_session_id) != 16:
            raise ValueError("camera_session_id must be a 16-byte UUID")
        if self.frame_number < 0:
            raise ValueError("frame_number must be non-negative")
        if not isinstance(self.pixels, np.ndarray) or self.pixels.size == 0:
            raise ValueError("pixels must be a non-empty NumPy array")

    def private_copy(self) -> Frame:
        return Frame(
            np.array(self.pixels, copy=True),
            self.camera_id,
            bytes(self.camera_session_id),
            self.frame_number,
            self.capture_time_unix_ns,
            self.capture_monotonic_ns,
        )


@dataclass(frozen=True)
class ModuleContext:
    """The task-scoped configuration visible to a module implementation."""

    module_id: str
    task_id: str
    host_device_id: str
    camera_id: str
    task: TaskConfig


class CVModule(ABC):
    """Task computation only; the runner owns transport and lifecycle plumbing."""

    requires_artifact: ClassVar[bool] = True

    @abstractmethod
    def initialize(self, context: ModuleContext) -> None:
        """Load task-specific state before the runner becomes ready."""

    @abstractmethod
    def process(self, frame: Frame) -> list[Message]:
        """Process one private frame on the runner's single worker thread."""

    def apply_dynamic_config(self, config: DynamicConfig) -> None:
        del config

    def on_start(self) -> None:
        return None

    def on_stop(self) -> None:
        return None

    def shutdown(self) -> None:
        return None


__all__ = ["CVModule", "DynamicConfig", "Frame", "ModuleContext", "NativeScalar", "NativeValue"]
