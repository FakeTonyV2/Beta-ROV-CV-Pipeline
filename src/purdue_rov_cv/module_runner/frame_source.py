"""Phase 5 frame ingress backed by the canonical Phase 6 reader."""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Protocol

from purdue_rov_cv.frame_buffer import (
    ReadStatus,
    SharedMemoryFrameReader,
    SharedMemoryInvalid,
    shared_memory_name,
)
from purdue_rov_cv.modules.base import Frame
from purdue_rov_cv.runtime.metrics import RuntimeMetrics


class FrameSourceError(RuntimeError):
    pass


class FrameSourceInvalid(FrameSourceError):
    pass


class FrameSource(Protocol):
    """Produce process-private frame snapshots detached from source storage."""

    @property
    def attached(self) -> bool: ...

    def attach(self) -> bool: ...

    def read(self, timeout_seconds: float = 0.250) -> Frame | None: ...

    def close(self) -> None: ...


class SharedMemoryFrameSource:
    """Adapt the nonblocking Phase 6 reader to Phase 5's bounded poll API.

    The reader copies and validates the selected slot. This adapter performs no
    raw buffer access, closes only its consumer attachment, and reattaches when
    a camera process recreates the stable segment after restart.
    """

    def __init__(
        self,
        name: str,
        *,
        camera_id: str,
        expected_slot_capacity_bytes: int | None = None,
        metrics: RuntimeMetrics | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        reader: SharedMemoryFrameReader | None = None,
    ) -> None:
        expected_name = shared_memory_name(camera_id)
        if name != expected_name:
            raise ValueError(f"shared-memory name must be {expected_name!r} for camera {camera_id!r}")
        self.name = name
        self.camera_id = camera_id
        self._monotonic = monotonic
        self._sleep = sleep
        self._metrics = metrics
        self._ever_attached = False
        self._loss_active = False
        self._reader = reader or SharedMemoryFrameReader(
            camera_id,
            expected_slot_capacity_bytes=expected_slot_capacity_bytes,
            metrics=metrics,
        )
        if self._metrics is not None:
            self._metrics.set_gauge("input_source_present", False)

    @property
    def attached(self) -> bool:
        return self._reader.attached

    def attach(self) -> bool:
        try:
            attached = self._reader.attach()
        except SharedMemoryInvalid as error:
            raise FrameSourceInvalid(str(error)) from error
        if self._metrics is not None:
            self._metrics.set_gauge("input_source_present", attached)
        if attached:
            if self._loss_active and self._metrics is not None:
                self._metrics.increment("shared_memory_reattach_count")
            self._ever_attached = True
            self._loss_active = False
        return attached

    def read(self, timeout_seconds: float = 0.250) -> Frame | None:
        if not 0 <= timeout_seconds <= 0.250:
            raise ValueError("frame read timeout must be between zero and 250 ms")
        deadline = self._monotonic() + timeout_seconds
        while True:
            try:
                result = self._reader.read()
            except SharedMemoryInvalid as error:
                raise FrameSourceInvalid(str(error)) from error
            if result.status is ReadStatus.FRAME:
                assert result.frame is not None
                return result.frame
            if result.status is ReadStatus.NOT_ATTACHED:
                if self._ever_attached and not self._loss_active:
                    self._loss_active = True
                    if self._metrics is not None:
                        self._metrics.increment("shared_memory_disconnects")
                        self._metrics.set_gauge("input_source_present", False)
                return None
            remaining = deadline - self._monotonic()
            if remaining <= 0:
                return None
            self._sleep(min(0.005, remaining))

    def close(self) -> None:
        self._reader.close()
        if self._metrics is not None:
            self._metrics.set_gauge("input_source_present", False)


__all__ = [
    "FrameSource",
    "FrameSourceError",
    "FrameSourceInvalid",
    "SharedMemoryFrameSource",
]
