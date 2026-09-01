"""Real shared-memory triple-buffer ownership, publication, and reading."""

from __future__ import annotations

import errno
import os
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, replace
from enum import StrEnum
from multiprocessing import resource_tracker, shared_memory
from typing import Protocol

import numpy as np

from purdue_rov_cv.modules.base import Frame
from purdue_rov_cv.runtime.metrics import RuntimeMetrics

from .header import (
    GENERATION_OFFSET,
    GENERATION_STRUCT,
    HEADER_SIZE,
    MAX_UINT32,
    MAX_UINT64,
    OWNER_PID_OFFSET,
    OWNER_PID_STRUCT,
    FrameHeader,
    PixelFormat,
    SharedMemoryInvalid,
    bytes_per_pixel,
    segment_size,
    shared_memory_name,
    slot_start,
)

READ_ATTEMPTS = 3


class ProcessExists(Protocol):
    def __call__(self, process_id: int) -> bool: ...


def process_exists(process_id: int) -> bool:
    """Conservatively report permission-denied processes as alive."""
    if isinstance(process_id, bool) or not 0 < process_id <= MAX_UINT32:
        return False
    try:
        os.kill(process_id, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OverflowError:
        # The wire field is uint32 while POSIX pid_t is signed on the
        # reference platform. Values outside the OS PID domain cannot name a
        # live process and must not crash stale-owner arbitration.
        return False
    except OSError as error:
        if error.errno == errno.ESRCH:
            return False
        if error.errno == errno.EPERM:
            return True
        return True
    return True


class LiveOwnerError(RuntimeError):
    """A validated live process already owns the stable camera segment."""


class UnsafeStaleSegmentError(RuntimeError):
    """Malformed ownership metadata cannot safely authorize stale cleanup."""


def _header_bytes(memory: shared_memory.SharedMemory) -> bytes:
    buffer = memory.buf
    assert buffer is not None
    return bytes(buffer[:HEADER_SIZE])


@contextmanager
def _startup_lock() -> Iterator[None]:
    """Serialize shared-memory startup arbitration on the POSIX target.

    The lock protects only startup inspection/unlink/create. Frame publication
    and reading remain lock-free. Independent systemd service processes do not
    share a Python synchronization primitive, so a small advisory file lock is
    needed to prevent one replacement from unlinking another replacement's
    newly created segment.
    """

    if os.name != "posix":
        yield
        return
    import fcntl

    flags = os.O_CREAT | os.O_RDWR
    flags |= getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open("/tmp/purdue_rov_cv.startup.lock", flags, 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)


@dataclass(frozen=True, slots=True)
class FrameWrite:
    data: bytes
    width: int
    height: int
    stride_bytes: int
    pixel_format: PixelFormat
    frame_number: int
    capture_time_unix_ns: int
    capture_monotonic_ns: int

    def validate(self, slot_capacity_bytes: int) -> None:
        values = (self.width, self.height, self.stride_bytes)
        if any(isinstance(value, bool) or not 0 < value <= MAX_UINT32 for value in values):
            raise ValueError("frame dimensions and stride must be positive uint32 values")
        if isinstance(self.frame_number, bool) or not 0 <= self.frame_number <= MAX_UINT64:
            raise ValueError("frame number must be a uint64")
        for value, label in (
            (self.capture_time_unix_ns, "capture UNIX timestamp"),
            (self.capture_monotonic_ns, "capture monotonic timestamp"),
        ):
            if isinstance(value, bool) or not -(1 << 63) <= value < (1 << 63):
                raise ValueError(f"{label} must be an int64")
        pixel_format = PixelFormat(self.pixel_format)
        minimum_stride = self.width * bytes_per_pixel(pixel_format)
        if self.stride_bytes < minimum_stride:
            raise ValueError("frame stride is smaller than a packed row")
        expected_length = self.stride_bytes * self.height
        if len(self.data) != expected_length:
            raise ValueError("frame byte length must equal stride times height")
        if len(self.data) > slot_capacity_bytes:
            raise ValueError("frame exceeds the configured slot capacity")


class SharedMemoryFrameWriter:
    """Creator/owner for one camera's stable triple-buffer segment."""

    def __init__(
        self,
        camera_id: str,
        slot_capacity_bytes: int,
        camera_session_id: bytes,
        *,
        owner_process_id: int | None = None,
        pid_is_alive: ProcessExists = process_exists,
        metrics: RuntimeMetrics | None = None,
    ) -> None:
        self.camera_id = camera_id
        self.name = shared_memory_name(camera_id)
        self.slot_capacity_bytes = slot_capacity_bytes
        self.camera_session_id = bytes(camera_session_id)
        self.owner_process_id = os.getpid() if owner_process_id is None else owner_process_id
        self._pid_is_alive = pid_is_alive
        self._metrics = metrics
        self._memory: shared_memory.SharedMemory | None = None
        self._creator = False
        self._header: FrameHeader | None = None

    @property
    def created(self) -> bool:
        return self._creator and self._memory is not None

    @property
    def header(self) -> FrameHeader:
        if self._header is None:
            raise RuntimeError("shared-memory writer is not open")
        return self._header

    def _create_clean(self) -> None:
        header = FrameHeader.initial(self.slot_capacity_bytes, self.owner_process_id, self.camera_session_id)
        memory = shared_memory.SharedMemory(
            name=self.name,
            create=True,
            size=segment_size(self.slot_capacity_bytes),
        )
        try:
            buffer = memory.buf
            assert buffer is not None
            buffer[:] = bytes(len(buffer))
            buffer[:HEADER_SIZE] = header.encode()
        except BaseException:
            try:
                memory.unlink()
            finally:
                memory.close()
            raise
        self._memory = memory
        self._header = header
        self._creator = True

    def open(self) -> None:
        if self._memory is not None:
            return
        with _startup_lock():
            self._open_locked()

    def _open_locked(self) -> None:
        try:
            self._create_clean()
            return
        except FileExistsError:
            pass
        try:
            existing = shared_memory.SharedMemory(name=self.name, create=False)
        except FileNotFoundError:
            # The previous creator can unlink between our exclusive create and
            # attachment. The startup lock prevents competing replacements,
            # so retrying creation is safe.
            self._create_clean()
            return
        tracker_registered = True
        try:
            raw_header = _header_bytes(existing)
            owner_pid = 0
            if existing.size >= OWNER_PID_OFFSET + OWNER_PID_STRUCT.size:
                owner_pid = OWNER_PID_STRUCT.unpack_from(raw_header, OWNER_PID_OFFSET)[0]
            try:
                existing_header = FrameHeader.decode(raw_header, actual_segment_size=existing.size)
                owner_pid = existing_header.owner_process_id
            except SharedMemoryInvalid:
                if owner_pid <= 0:
                    raise UnsafeStaleSegmentError(
                        "existing malformed segment has no trustworthy positive owner PID"
                    ) from None
            if self._pid_is_alive(owner_pid):
                raise LiveOwnerError(f"camera segment {self.name!r} is owned by live PID {owner_pid}")
            existing.unlink()
            tracker_registered = False
        finally:
            if tracker_registered:
                _unregister_consumer(existing)
            existing.close()
        self._create_clean()

    def write(self, frame: FrameWrite) -> FrameHeader:
        memory = self._memory
        if memory is None or self._header is None:
            raise RuntimeError("shared-memory writer is not open")
        frame.validate(self.slot_capacity_bytes)
        buffer = memory.buf
        assert buffer is not None
        current = self._header
        next_slot = (current.active_slot_index + 1) % 3
        odd_generation = (current.generation + 1) & MAX_UINT64
        if odd_generation % 2 == 0:
            odd_generation = (odd_generation + 1) & MAX_UINT64
        even_generation = (odd_generation + 1) & MAX_UINT64
        GENERATION_STRUCT.pack_into(buffer, GENERATION_OFFSET, odd_generation)
        start = slot_start(next_slot, self.slot_capacity_bytes)
        buffer[start : start + len(frame.data)] = frame.data
        publishing = FrameHeader(
            slot_capacity_bytes=self.slot_capacity_bytes,
            active_slot_index=next_slot,
            generation=odd_generation,
            frame_number=frame.frame_number,
            capture_time_unix_ns=frame.capture_time_unix_ns,
            capture_monotonic_ns=frame.capture_monotonic_ns,
            width=frame.width,
            height=frame.height,
            stride_bytes=frame.stride_bytes,
            data_length_bytes=len(frame.data),
            pixel_format=PixelFormat(frame.pixel_format),
            owner_process_id=self.owner_process_id,
            camera_session_id=self.camera_session_id,
        )
        buffer[:HEADER_SIZE] = publishing.encode()
        GENERATION_STRUCT.pack_into(buffer, GENERATION_OFFSET, even_generation)
        committed = replace(publishing, generation=even_generation)
        self._header = committed
        if self._metrics is not None:
            self._metrics.increment("shared_memory_write_count")
        return committed

    def close(self, *, unlink: bool = True) -> None:
        memory = self._memory
        creator = self._creator
        self._memory = None
        self._header = None
        self._creator = False
        if memory is None:
            return
        if unlink and creator:
            try:
                memory.unlink()
            except FileNotFoundError:
                pass
        memory.close()


class ReadStatus(StrEnum):
    FRAME = "FRAME"
    NO_FRAME = "NO_FRAME"
    CONFLICT = "CONFLICT"
    NOT_ATTACHED = "NOT_ATTACHED"


@dataclass(frozen=True, slots=True)
class FrameReadResult:
    status: ReadStatus
    frame: Frame | None = None
    header: FrameHeader | None = None


def _unregister_consumer(memory: shared_memory.SharedMemory) -> None:
    """Prevent Python 3.12's reader tracker from unlinking camera-owned memory."""
    resource_tracker.unregister(memory._name, "shared_memory")  # type: ignore[attr-defined]


class SharedMemoryFrameReader:
    """Lock-free consumer returning process-private validated frame snapshots."""

    def __init__(
        self,
        camera_id: str,
        *,
        expected_slot_capacity_bytes: int | None = None,
        metrics: RuntimeMetrics | None = None,
        unregister_from_resource_tracker: bool = True,
    ) -> None:
        self.camera_id = camera_id
        self.name = shared_memory_name(camera_id)
        self.expected_slot_capacity_bytes = expected_slot_capacity_bytes
        self._metrics = metrics
        self._unregister = unregister_from_resource_tracker
        self._memory: shared_memory.SharedMemory | None = None
        self._last_generation: int | None = None

    @property
    def attached(self) -> bool:
        return self._memory is not None

    def attach(self) -> bool:
        if self._memory is not None and self._mapping_is_current():
            return True
        self.close()
        with _startup_lock():
            return self._attach_locked()

    def _attach_locked(self) -> bool:
        """Attach only after a creator finishes header initialization.

        POSIX makes a newly created shared-memory name visible before Python
        finishes zeroing and encoding its header. Sharing the short startup
        arbitration lock prevents a legitimate replacement from looking like
        stable malformed memory. Accepted frame reads remain lock-free.
        """

        try:
            memory = shared_memory.SharedMemory(name=self.name, create=False)
        except FileNotFoundError:
            return False
        if self._unregister:
            # Unregister before validation: a rejected consumer attachment is
            # still a consumer and must never unlink camera-owned memory when
            # its Python resource tracker exits.
            _unregister_consumer(memory)
        try:
            raw_header = _header_bytes(memory)
            header = FrameHeader.decode(raw_header, actual_segment_size=memory.size)
            if (
                self.expected_slot_capacity_bytes is not None
                and header.slot_capacity_bytes != self.expected_slot_capacity_bytes
            ):
                raise SharedMemoryInvalid("shared-memory slot capacity differs from configuration")
        except Exception:
            memory.close()
            raise
        self._memory = memory
        self._last_generation = None
        return True

    def _mapping_is_current(self) -> bool:
        memory = self._memory
        if memory is None:
            return False
        file_descriptor = getattr(memory, "_fd", None)
        if not isinstance(file_descriptor, int) or file_descriptor < 0:
            return True
        try:
            mapped = os.fstat(file_descriptor)
            visible = os.stat(f"/dev/shm/{self.name}")
        except FileNotFoundError:
            return False
        return mapped.st_ino == visible.st_ino and mapped.st_dev == visible.st_dev and mapped.st_nlink > 0

    def _stable_error(self, memory: shared_memory.SharedMemory, generation_1: int, error: Exception) -> bool:
        buffer = memory.buf
        assert buffer is not None
        generation_2 = GENERATION_STRUCT.unpack_from(buffer, GENERATION_OFFSET)[0]
        if generation_1 == generation_2 and generation_2 % 2 == 0:
            raise error
        return False

    def read(self) -> FrameReadResult:
        memory = self._memory
        if memory is None:
            return FrameReadResult(ReadStatus.NOT_ATTACHED)
        if not self._mapping_is_current():
            self.close()
            return FrameReadResult(ReadStatus.NOT_ATTACHED)
        buffer = memory.buf
        assert buffer is not None
        for _attempt in range(READ_ATTEMPTS):
            generation_1 = GENERATION_STRUCT.unpack_from(buffer, GENERATION_OFFSET)[0]
            if generation_1 % 2:
                continue
            raw_header = bytes(buffer[:HEADER_SIZE])
            try:
                header = FrameHeader.decode(raw_header, actual_segment_size=memory.size)
                if header.generation != generation_1:
                    continue
                if self.expected_slot_capacity_bytes is not None and (
                    header.slot_capacity_bytes != self.expected_slot_capacity_bytes
                ):
                    raise SharedMemoryInvalid("shared-memory slot capacity differs from configuration")
                if not header.published:
                    generation_2 = GENERATION_STRUCT.unpack_from(buffer, GENERATION_OFFSET)[0]
                    if generation_1 == generation_2 and generation_2 % 2 == 0:
                        return FrameReadResult(ReadStatus.NO_FRAME, header=header)
                    continue
                header.validate_frame()
            except SharedMemoryInvalid as error:
                self._stable_error(memory, generation_1, error)
                continue
            start = slot_start(header.active_slot_index, header.slot_capacity_bytes)
            private_data = bytes(buffer[start : start + header.data_length_bytes])
            generation_2 = GENERATION_STRUCT.unpack_from(buffer, GENERATION_OFFSET)[0]
            if generation_1 != generation_2 or generation_2 % 2:
                continue
            if generation_2 == self._last_generation:
                return FrameReadResult(ReadStatus.NO_FRAME, header=header)
            pixels = _pixels_from_private_bytes(private_data, header)
            frame = Frame(
                pixels=pixels,
                camera_id=self.camera_id,
                camera_session_id=header.camera_session_id,
                frame_number=header.frame_number,
                capture_time_unix_ns=header.capture_time_unix_ns,
                capture_monotonic_ns=header.capture_monotonic_ns,
            )
            self._last_generation = generation_2
            return FrameReadResult(ReadStatus.FRAME, frame, header)
        if self._metrics is not None:
            self._metrics.increment("shared_memory_read_conflicts")
        return FrameReadResult(ReadStatus.CONFLICT)

    def close(self) -> None:
        memory = self._memory
        self._memory = None
        self._last_generation = None
        if memory is not None:
            memory.close()


def _pixels_from_private_bytes(data: bytes, header: FrameHeader) -> np.ndarray:
    pixel_format = header.pixel_format
    bytes_per_sample = bytes_per_pixel(pixel_format)
    if pixel_format in {PixelFormat.BGR8, PixelFormat.RGB8}:
        view = np.ndarray(
            (header.height, header.width, 3),
            dtype=np.uint8,
            buffer=data,
            strides=(header.stride_bytes, bytes_per_sample, 1),
        )
    elif pixel_format is PixelFormat.GRAY8:
        view = np.ndarray(
            (header.height, header.width),
            dtype=np.uint8,
            buffer=data,
            strides=(header.stride_bytes, 1),
        )
    else:
        view = np.ndarray(
            (header.height, header.width),
            dtype=np.dtype("<u2"),
            buffer=data,
            strides=(header.stride_bytes, 2),
        )
    return np.array(view, copy=True)


__all__ = [
    "FrameReadResult",
    "FrameWrite",
    "LiveOwnerError",
    "READ_ATTEMPTS",
    "ReadStatus",
    "SharedMemoryFrameReader",
    "SharedMemoryFrameWriter",
    "UnsafeStaleSegmentError",
    "process_exists",
]
