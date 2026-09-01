"""Auditable v1 shared-memory frame header and layout helpers."""

from __future__ import annotations

import re
import struct
from dataclasses import dataclass, replace
from enum import IntEnum

MAGIC = b"PROVCV01"
HEADER_VERSION = 1
HEADER_SIZE = 128
SLOT_COUNT = 3
MAX_UINT32 = (1 << 32) - 1
MAX_UINT64 = (1 << 64) - 1

MAGIC_OFFSET = 0
VERSION_OFFSET = 8
SLOT_COUNT_OFFSET = 12
SLOT_CAPACITY_OFFSET = 16
ACTIVE_SLOT_OFFSET = 20
GENERATION_OFFSET = 24
FRAME_NUMBER_OFFSET = 32
CAPTURE_UNIX_NS_OFFSET = 40
CAPTURE_MONOTONIC_NS_OFFSET = 48
WIDTH_OFFSET = 56
HEIGHT_OFFSET = 60
STRIDE_OFFSET = 64
DATA_LENGTH_OFFSET = 68
PIXEL_FORMAT_OFFSET = 72
OWNER_PID_OFFSET = 76
SESSION_UUID_OFFSET = 80
RESERVED_OFFSET = 96
RESERVED_SIZE = 32

HEADER_STRUCT = struct.Struct("<8sIIIIQQqqIIIIII16s32s")
GENERATION_STRUCT = struct.Struct("<Q")
OWNER_PID_STRUCT = struct.Struct("<I")
CAMERA_ID_PATTERN = re.compile(r"^[a-z][a-z0-9_]{0,63}$")


class PixelFormat(IntEnum):
    BGR8 = 1
    RGB8 = 2
    GRAY8 = 3
    DEPTH16_MM = 4


def bytes_per_pixel(pixel_format: PixelFormat) -> int:
    return {
        PixelFormat.BGR8: 3,
        PixelFormat.RGB8: 3,
        PixelFormat.GRAY8: 1,
        PixelFormat.DEPTH16_MM: 2,
    }[PixelFormat(pixel_format)]


class SharedMemoryInvalid(ValueError):
    """The binary shared-memory contract is malformed or incompatible."""


def shared_memory_name(camera_id: str) -> str:
    if not CAMERA_ID_PATTERN.fullmatch(camera_id):
        raise ValueError("camera_id is incompatible with the shared-memory naming contract")
    return f"purdue_rov_cv_{camera_id}"


def segment_size(slot_capacity_bytes: int) -> int:
    if isinstance(slot_capacity_bytes, bool) or not 0 < slot_capacity_bytes <= MAX_UINT32:
        raise ValueError("slot capacity must be a positive uint32")
    return HEADER_SIZE + SLOT_COUNT * slot_capacity_bytes


def slot_start(slot_index: int, slot_capacity_bytes: int) -> int:
    if isinstance(slot_index, bool) or not 0 <= slot_index < SLOT_COUNT:
        raise ValueError("slot index is outside the triple buffer")
    size = segment_size(slot_capacity_bytes)
    start = HEADER_SIZE + slot_index * slot_capacity_bytes
    if not HEADER_SIZE <= start < size:
        raise ValueError("slot start is outside the segment")
    return start


def slot_end(slot_index: int, slot_capacity_bytes: int) -> int:
    end = slot_start(slot_index, slot_capacity_bytes) + slot_capacity_bytes
    if end > segment_size(slot_capacity_bytes):
        raise ValueError("slot end is outside the segment")
    return end


@dataclass(frozen=True, slots=True)
class FrameHeader:
    slot_capacity_bytes: int
    active_slot_index: int
    generation: int
    frame_number: int
    capture_time_unix_ns: int
    capture_monotonic_ns: int
    width: int
    height: int
    stride_bytes: int
    data_length_bytes: int
    pixel_format: PixelFormat
    owner_process_id: int
    camera_session_id: bytes

    @classmethod
    def initial(cls, slot_capacity_bytes: int, owner_process_id: int, camera_session_id: bytes) -> FrameHeader:
        header = cls(
            slot_capacity_bytes=slot_capacity_bytes,
            active_slot_index=0,
            generation=0,
            frame_number=0,
            capture_time_unix_ns=0,
            capture_monotonic_ns=0,
            width=0,
            height=0,
            stride_bytes=0,
            data_length_bytes=0,
            pixel_format=PixelFormat.BGR8,
            owner_process_id=owner_process_id,
            camera_session_id=bytes(camera_session_id),
        )
        header.validate_common(segment_size(slot_capacity_bytes))
        return header

    def encode(self) -> bytes:
        self.validate_common(segment_size(self.slot_capacity_bytes))
        encoded = HEADER_STRUCT.pack(
            MAGIC,
            HEADER_VERSION,
            SLOT_COUNT,
            self.slot_capacity_bytes,
            self.active_slot_index,
            self.generation,
            self.frame_number,
            self.capture_time_unix_ns,
            self.capture_monotonic_ns,
            self.width,
            self.height,
            self.stride_bytes,
            self.data_length_bytes,
            int(self.pixel_format),
            self.owner_process_id,
            self.camera_session_id,
            bytes(RESERVED_SIZE),
        )
        if len(encoded) != HEADER_SIZE:
            raise AssertionError("v1 frame header is not exactly 128 bytes")
        return encoded

    @classmethod
    def decode(cls, raw: bytes | bytearray | memoryview, *, actual_segment_size: int) -> FrameHeader:
        if len(raw) < HEADER_SIZE:
            raise SharedMemoryInvalid("segment is smaller than the 128-byte header")
        (
            magic,
            version,
            slot_count,
            capacity,
            active_slot,
            generation,
            frame_number,
            unix_ns,
            monotonic_ns,
            width,
            height,
            stride,
            data_length,
            pixel_format_value,
            owner_pid,
            session_id,
            reserved,
        ) = HEADER_STRUCT.unpack_from(raw, 0)
        if magic != MAGIC:
            raise SharedMemoryInvalid("shared-memory magic is incompatible")
        if version != HEADER_VERSION:
            raise SharedMemoryInvalid("shared-memory header version is incompatible")
        if slot_count != SLOT_COUNT:
            raise SharedMemoryInvalid("shared-memory slot count is not three")
        if reserved != bytes(RESERVED_SIZE):
            raise SharedMemoryInvalid("shared-memory reserved header bytes are nonzero")
        try:
            pixel_format = PixelFormat(pixel_format_value)
        except ValueError as error:
            raise SharedMemoryInvalid("shared-memory pixel format is unsupported") from error
        header = cls(
            capacity,
            active_slot,
            generation,
            frame_number,
            unix_ns,
            monotonic_ns,
            width,
            height,
            stride,
            data_length,
            pixel_format,
            owner_pid,
            bytes(session_id),
        )
        header.validate_common(actual_segment_size)
        return header

    def validate_common(self, actual_segment_size: int) -> None:
        try:
            expected_size = segment_size(self.slot_capacity_bytes)
        except ValueError as error:
            raise SharedMemoryInvalid(str(error)) from error
        if actual_segment_size != expected_size:
            raise SharedMemoryInvalid("shared-memory segment size does not match its header")
        if not 0 <= self.active_slot_index < SLOT_COUNT:
            raise SharedMemoryInvalid("active slot index is outside the triple buffer")
        if not 0 <= self.generation <= MAX_UINT64:
            raise SharedMemoryInvalid("generation is outside uint64")
        if not 0 <= self.frame_number <= MAX_UINT64:
            raise SharedMemoryInvalid("frame number is outside uint64")
        if not 0 < self.owner_process_id <= MAX_UINT32:
            raise SharedMemoryInvalid("owner process ID is invalid")
        if len(self.camera_session_id) != 16:
            raise SharedMemoryInvalid("camera session UUID must be exactly 16 bytes")

    @property
    def published(self) -> bool:
        # Generation zero is both the initial value and the valid even value
        # reached after uint64 rollover.  Data length is the unambiguous v1
        # discriminator because an initial header never contains frame data.
        return self.data_length_bytes != 0

    def validate_frame(self) -> None:
        if not self.published:
            raise SharedMemoryInvalid("shared memory contains no published frame")
        if self.generation % 2:
            raise SharedMemoryInvalid("shared-memory write is in progress")
        if self.width <= 0 or self.height <= 0:
            raise SharedMemoryInvalid("frame dimensions must be positive")
        minimum_stride = self.width * bytes_per_pixel(self.pixel_format)
        if self.stride_bytes < minimum_stride or self.stride_bytes > MAX_UINT32:
            raise SharedMemoryInvalid("frame stride is incompatible with its width and format")
        expected_length = self.stride_bytes * self.height
        if expected_length != self.data_length_bytes:
            raise SharedMemoryInvalid("frame data length does not match height and stride")
        if not 0 < self.data_length_bytes <= self.slot_capacity_bytes:
            raise SharedMemoryInvalid("frame data length exceeds slot capacity")
        start = slot_start(self.active_slot_index, self.slot_capacity_bytes)
        if start + self.data_length_bytes > slot_end(self.active_slot_index, self.slot_capacity_bytes):
            raise SharedMemoryInvalid("frame data extends outside its active slot")

    def with_generation(self, generation: int) -> FrameHeader:
        return replace(self, generation=generation)


assert HEADER_STRUCT.size == HEADER_SIZE


__all__ = [
    "ACTIVE_SLOT_OFFSET",
    "CAMERA_ID_PATTERN",
    "CAPTURE_MONOTONIC_NS_OFFSET",
    "CAPTURE_UNIX_NS_OFFSET",
    "DATA_LENGTH_OFFSET",
    "FRAME_NUMBER_OFFSET",
    "FrameHeader",
    "GENERATION_OFFSET",
    "GENERATION_STRUCT",
    "HEADER_SIZE",
    "HEADER_STRUCT",
    "HEADER_VERSION",
    "MAGIC",
    "MAX_UINT32",
    "MAX_UINT64",
    "OWNER_PID_OFFSET",
    "OWNER_PID_STRUCT",
    "PixelFormat",
    "RESERVED_OFFSET",
    "RESERVED_SIZE",
    "SESSION_UUID_OFFSET",
    "SLOT_CAPACITY_OFFSET",
    "SLOT_COUNT",
    "SharedMemoryInvalid",
    "bytes_per_pixel",
    "segment_size",
    "shared_memory_name",
    "slot_end",
    "slot_start",
]
