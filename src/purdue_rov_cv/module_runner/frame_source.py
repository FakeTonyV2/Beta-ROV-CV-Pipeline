"""Read-only consumer for the camera-owned shared-memory frame contract."""

from __future__ import annotations

import mmap
import os
import re
import struct
import time
from collections.abc import Callable
from pathlib import Path
from typing import BinaryIO, Protocol

import numpy as np

from purdue_rov_cv.modules.base import Frame

SHARED_FRAME_MAGIC = b"PROVCV1\0"
SHARED_FRAME_VERSION = 1
SHARED_FRAME_DTYPE_UINT8 = 1
# Writers use an odd sequence while mutating and publish the next even value.
SHARED_FRAME_HEADER = struct.Struct("<8sIQIIII16sQqq")
_SEQUENCE_OFFSET = struct.calcsize("<8sI")
_NAME_PATTERN = re.compile(r"^[A-Za-z0-9_.-]{1,128}$")


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
    """Attach to a POSIX shared-memory object without creating or unlinking it.

    The camera-service phase owns object creation and unlink.  This consumer
    opens ``/dev/shm/<name>`` read-only, validates the versioned header, copies
    pixels once into process-private memory, and closes only its mmap/file
    handle. Ownership of each returned snapshot transfers to the caller.
    """

    def __init__(
        self,
        name: str,
        *,
        camera_id: str,
        directory: Path = Path("/dev/shm"),
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if not _NAME_PATTERN.fullmatch(name):
            raise ValueError("shared-memory name contains invalid characters")
        self.path = directory / name
        self.camera_id = camera_id
        self._monotonic = monotonic
        self._sleep = sleep
        self._file: BinaryIO | None = None
        self._mapping: mmap.mmap | None = None
        self._last_sequence = 0

    @property
    def attached(self) -> bool:
        return self._mapping is not None

    def attach(self) -> bool:
        if self.attached:
            return True
        try:
            file = self.path.open("rb", buffering=0)
        except FileNotFoundError:
            return False
        try:
            size = os.fstat(file.fileno()).st_size
            if size < SHARED_FRAME_HEADER.size + 1:
                raise FrameSourceInvalid("shared-memory object is smaller than its header")
            mapping = mmap.mmap(file.fileno(), length=0, access=mmap.ACCESS_READ)
        except Exception:
            file.close()
            raise
        self._file = file
        self._mapping = mapping
        return True

    def _read_once(self) -> Frame | None:
        mapping = self._mapping
        if mapping is None:
            return None
        header = SHARED_FRAME_HEADER.unpack_from(mapping, 0)
        magic, version, sequence, width, height, channels, dtype_code, session_id, frame_number, unix_ns, mono_ns = (
            header
        )
        if magic != SHARED_FRAME_MAGIC or version != SHARED_FRAME_VERSION:
            raise FrameSourceInvalid("shared-memory magic/version is incompatible")
        if dtype_code != SHARED_FRAME_DTYPE_UINT8 or width == 0 or height == 0 or channels not in {1, 3, 4}:
            raise FrameSourceInvalid("shared-memory frame shape or dtype is invalid")
        payload_size = width * height * channels
        if SHARED_FRAME_HEADER.size + payload_size > len(mapping):
            raise FrameSourceInvalid("shared-memory frame exceeds the mapped object")
        if sequence == 0 or sequence % 2 == 1 or sequence == self._last_sequence:
            return None
        pixels = np.frombuffer(
            mapping,
            dtype=np.uint8,
            count=payload_size,
            offset=SHARED_FRAME_HEADER.size,
        ).reshape((height, width, channels))
        copied = np.array(pixels, copy=True)
        stable_sequence = struct.unpack_from("<Q", mapping, _SEQUENCE_OFFSET)[0]
        if stable_sequence != sequence or stable_sequence % 2 == 1:
            return None
        self._last_sequence = sequence
        return Frame(copied, self.camera_id, session_id, frame_number, unix_ns, mono_ns)

    def read(self, timeout_seconds: float = 0.250) -> Frame | None:
        if not 0 <= timeout_seconds <= 0.250:
            raise ValueError("frame read timeout must be between zero and 250 ms")
        deadline = self._monotonic() + timeout_seconds
        while True:
            frame = self._read_once()
            if frame is not None:
                return frame
            remaining = deadline - self._monotonic()
            if remaining <= 0:
                return None
            self._sleep(min(0.005, remaining))

    def close(self) -> None:
        mapping, file = self._mapping, self._file
        self._mapping = None
        self._file = None
        if mapping is not None:
            mapping.close()
        if file is not None:
            file.close()


__all__ = [
    "FrameSource",
    "FrameSourceError",
    "FrameSourceInvalid",
    "SHARED_FRAME_DTYPE_UINT8",
    "SHARED_FRAME_HEADER",
    "SHARED_FRAME_MAGIC",
    "SHARED_FRAME_VERSION",
    "SharedMemoryFrameSource",
]
