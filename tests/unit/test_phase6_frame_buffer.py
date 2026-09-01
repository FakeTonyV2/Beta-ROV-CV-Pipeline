"""Exact Phase 6 binary contract, ownership, reader, and writer tests."""

from __future__ import annotations

import struct
from dataclasses import replace
from multiprocessing import shared_memory
from uuid import uuid4

import numpy as np
import pytest

from purdue_rov_cv.frame_buffer import (
    HEADER_SIZE,
    HEADER_STRUCT,
    MAGIC,
    FrameHeader,
    FrameWrite,
    LiveOwnerError,
    PixelFormat,
    ReadStatus,
    SharedMemoryFrameReader,
    SharedMemoryFrameWriter,
    SharedMemoryInvalid,
    process_exists,
    segment_size,
    shared_memory_name,
    slot_end,
    slot_start,
)
from purdue_rov_cv.frame_buffer.header import (
    ACTIVE_SLOT_OFFSET,
    DATA_LENGTH_OFFSET,
    GENERATION_OFFSET,
    MAX_UINT64,
    OWNER_PID_OFFSET,
    RESERVED_OFFSET,
    SESSION_UUID_OFFSET,
)
from purdue_rov_cv.runtime.metrics import RuntimeMetrics


def _camera_id() -> str:
    return f"camera_{uuid4().hex}"


def _write(number: int, value: int, *, width: int = 4, height: int = 3) -> FrameWrite:
    stride = width * 3
    return FrameWrite(
        bytes([value]) * (stride * height),
        width,
        height,
        stride,
        PixelFormat.BGR8,
        number,
        1_000 + number,
        2_000 + number,
    )


def test_header_is_exact_little_endian_128_byte_contract() -> None:
    session = bytes(range(16))
    header = FrameHeader(
        0x01020304,
        2,
        0x0102030405060708,
        0x1112131415161718,
        -2,
        3,
        640,
        480,
        1_920,
        921_600,
        PixelFormat.BGR8,
        0x10203040,
        session,
    )
    encoded = header.encode()
    assert HEADER_STRUCT.size == HEADER_SIZE == len(encoded) == 128
    assert encoded[:8] == MAGIC
    assert encoded[8:12] == b"\x01\x00\x00\x00"
    assert encoded[12:16] == b"\x03\x00\x00\x00"
    assert encoded[16:20] == b"\x04\x03\x02\x01"
    assert encoded[ACTIVE_SLOT_OFFSET : ACTIVE_SLOT_OFFSET + 4] == b"\x02\x00\x00\x00"
    assert encoded[GENERATION_OFFSET : GENERATION_OFFSET + 8] == b"\x08\x07\x06\x05\x04\x03\x02\x01"
    assert encoded[OWNER_PID_OFFSET : OWNER_PID_OFFSET + 4] == b"\x40\x30\x20\x10"
    assert encoded[SESSION_UUID_OFFSET:RESERVED_OFFSET] == session
    assert encoded[RESERVED_OFFSET:] == bytes(32)
    assert FrameHeader.decode(encoded, actual_segment_size=segment_size(0x01020304)) == header


@pytest.mark.parametrize(
    ("offset", "value", "message"),
    [
        (0, b"BADMAGIC", "magic"),
        (8, struct.pack("<I", 2), "version"),
        (12, struct.pack("<I", 4), "slot count"),
        (ACTIVE_SLOT_OFFSET, struct.pack("<I", 3), "active slot"),
        (DATA_LENGTH_OFFSET, struct.pack("<I", 100), "data length"),
        (72, struct.pack("<I", 99), "pixel format"),
        (RESERVED_OFFSET, b"\x01", "reserved"),
    ],
)
def test_header_rejects_incompatible_or_unsafe_values(offset: int, value: bytes, message: str) -> None:
    raw = bytearray(FrameHeader.initial(64, 123, uuid4().bytes).encode())
    raw[offset : offset + len(value)] = value
    if offset == DATA_LENGTH_OFFSET:
        struct.pack_into("<Q", raw, GENERATION_OFFSET, 2)
        struct.pack_into("<II", raw, 56, 4, 3)
        struct.pack_into("<I", raw, 64, 12)
    with pytest.raises(SharedMemoryInvalid, match=message):
        header = FrameHeader.decode(raw, actual_segment_size=segment_size(64))
        header.validate_frame()


def test_layout_helpers_and_name_validation() -> None:
    assert shared_memory_name("front_camera") == "purdue_rov_cv_front_camera"
    assert segment_size(100) == 428
    assert [slot_start(index, 100) for index in range(3)] == [128, 228, 328]
    assert [slot_end(index, 100) for index in range(3)] == [228, 328, 428]
    with pytest.raises(ValueError):
        shared_memory_name("../front")
    with pytest.raises(ValueError):
        slot_start(3, 100)
    assert not process_exists((1 << 32) - 1)


def test_writer_rotates_slots_and_reader_returns_private_stable_copy() -> None:
    camera_id = _camera_id()
    metrics = RuntimeMetrics()
    writer = SharedMemoryFrameWriter(camera_id, 128, uuid4().bytes, metrics=metrics)
    reader = SharedMemoryFrameReader(camera_id, unregister_from_resource_tracker=False)
    try:
        writer.open()
        assert reader.attach()
        headers = [writer.write(_write(number, number + 1)) for number in range(4)]
        assert [header.active_slot_index for header in headers] == [1, 2, 0, 1]
        assert [header.generation for header in headers] == [2, 4, 6, 8]
        result = reader.read()
        assert result.status is ReadStatus.FRAME
        assert result.frame is not None
        private = result.frame.pixels
        assert result.frame.frame_number == 3
        assert np.all(private == 4)
        writer.write(_write(4, 9))
        assert np.all(private == 4)
        assert metrics.snapshot().values["shared_memory_write_count"] == 5
    finally:
        reader.close()
        writer.close()


def test_rejected_oversized_frame_preserves_previous_publication() -> None:
    camera_id = _camera_id()
    writer = SharedMemoryFrameWriter(camera_id, 36, uuid4().bytes)
    reader = SharedMemoryFrameReader(camera_id, unregister_from_resource_tracker=False)
    try:
        writer.open()
        writer.write(_write(0, 7))
        committed = writer.header
        with pytest.raises(ValueError, match="slot capacity"):
            writer.write(_write(1, 8, width=5, height=3))
        assert writer.header == committed
        assert reader.attach()
        result = reader.read()
        assert result.frame is not None and result.frame.frame_number == 0
        assert np.all(result.frame.pixels == 7)
    finally:
        reader.close()
        writer.close()


@pytest.mark.parametrize(
    "candidate",
    [
        FrameWrite(bytes(12), 0, 2, 6, PixelFormat.BGR8, 1, 1, 1),
        FrameWrite(bytes(12), 2, 2, 5, PixelFormat.BGR8, 1, 1, 1),
        FrameWrite(bytes(11), 2, 2, 6, PixelFormat.BGR8, 1, 1, 1),
        FrameWrite(bytes(12), 2, 2, 6, 99, 1, 1, 1),  # type: ignore[arg-type]
    ],
)
def test_rejected_candidate_matrix_preserves_previous_publication(candidate: FrameWrite) -> None:
    camera_id = _camera_id()
    writer = SharedMemoryFrameWriter(camera_id, 64, uuid4().bytes)
    reader = SharedMemoryFrameReader(camera_id, unregister_from_resource_tracker=False)
    try:
        writer.open()
        writer.write(_write(0, 17))
        committed = writer.header
        with pytest.raises(ValueError):
            writer.write(candidate)
        assert writer.header == committed
        assert reader.attach()
        result = reader.read()
        assert result.frame is not None
        assert result.frame.frame_number == 0
        assert np.all(result.frame.pixels == 17)
    finally:
        reader.close()
        writer.close()


@pytest.mark.parametrize(
    ("pixel_format", "data", "width", "height", "stride", "expected"),
    [
        (
            PixelFormat.BGR8,
            bytes([1, 2, 3, 4, 5, 6, 90, 91, 7, 8, 9, 10, 11, 12, 92, 93]),
            2,
            2,
            8,
            np.array([[[1, 2, 3], [4, 5, 6]], [[7, 8, 9], [10, 11, 12]]], dtype=np.uint8),
        ),
        (
            PixelFormat.RGB8,
            bytes([12, 11, 10, 9, 8, 7, 90, 91, 6, 5, 4, 3, 2, 1, 92, 93]),
            2,
            2,
            8,
            np.array([[[12, 11, 10], [9, 8, 7]], [[6, 5, 4], [3, 2, 1]]], dtype=np.uint8),
        ),
        (
            PixelFormat.GRAY8,
            bytes([1, 2, 90, 91, 3, 4, 92, 93]),
            2,
            2,
            4,
            np.array([[1, 2], [3, 4]], dtype=np.uint8),
        ),
        (
            PixelFormat.DEPTH16_MM,
            bytes([1, 0, 2, 0, 90, 91, 3, 0, 4, 0, 92, 93]),
            2,
            2,
            6,
            np.array([[1, 2], [3, 4]], dtype=np.uint16),
        ),
    ],
)
def test_reader_handles_padded_rows_for_every_pixel_format(
    pixel_format: PixelFormat,
    data: bytes,
    width: int,
    height: int,
    stride: int,
    expected: np.ndarray,
) -> None:
    camera_id = _camera_id()
    writer = SharedMemoryFrameWriter(camera_id, 64, uuid4().bytes)
    reader = SharedMemoryFrameReader(camera_id, unregister_from_resource_tracker=False)
    try:
        writer.open()
        writer.write(FrameWrite(data, width, height, stride, pixel_format, 0, 1, 2))
        assert reader.attach()
        result = reader.read()
        assert result.status is ReadStatus.FRAME
        assert result.frame is not None
        assert np.array_equal(result.frame.pixels, expected)
        assert result.frame.pixels.flags.owndata
    finally:
        reader.close()
        writer.close()


def test_uint64_generation_rollover_remains_a_published_frame() -> None:
    camera_id = _camera_id()
    writer = SharedMemoryFrameWriter(camera_id, 64, uuid4().bytes)
    reader = SharedMemoryFrameReader(camera_id, unregister_from_resource_tracker=False)
    try:
        writer.open()
        writer._header = replace(writer.header, generation=MAX_UINT64 - 1)  # type: ignore[attr-defined]
        committed = writer.write(_write(9, 23))
        assert committed.generation == 0
        assert committed.published
        assert reader.attach()
        result = reader.read()
        assert result.status is ReadStatus.FRAME
        assert result.frame is not None and result.frame.frame_number == 9
        assert np.all(result.frame.pixels == 23)
    finally:
        reader.close()
        writer.close()


def test_invalid_writer_identity_never_creates_a_segment() -> None:
    camera_id = _camera_id()
    writer = SharedMemoryFrameWriter(camera_id, 64, b"short")
    with pytest.raises(SharedMemoryInvalid, match="session UUID"):
        writer.open()
    with pytest.raises(FileNotFoundError):
        shared_memory.SharedMemory(name=shared_memory_name(camera_id), create=False)


def test_three_odd_generation_attempts_count_one_conflict() -> None:
    camera_id = _camera_id()
    metrics = RuntimeMetrics()
    writer = SharedMemoryFrameWriter(camera_id, 64, uuid4().bytes)
    reader = SharedMemoryFrameReader(camera_id, metrics=metrics, unregister_from_resource_tracker=False)
    raw: shared_memory.SharedMemory | None = None
    try:
        writer.open()
        assert reader.attach()
        raw = shared_memory.SharedMemory(name=shared_memory_name(camera_id), create=False)
        struct.pack_into("<Q", raw.buf, GENERATION_OFFSET, 1)
        assert reader.read().status is ReadStatus.CONFLICT
        assert metrics.snapshot().values["shared_memory_read_conflicts"] == 1
    finally:
        if raw is not None:
            raw.close()
        reader.close()
        writer.close()


def test_live_owner_rejected_dead_owner_recreated_and_reader_never_unlinks() -> None:
    camera_id = _camera_id()
    first = SharedMemoryFrameWriter(camera_id, 64, uuid4().bytes, owner_process_id=1234)
    first.open()
    live = SharedMemoryFrameWriter(camera_id, 64, uuid4().bytes, pid_is_alive=lambda _pid: True)
    with pytest.raises(LiveOwnerError):
        live.open()
    replacement = SharedMemoryFrameWriter(camera_id, 64, uuid4().bytes, pid_is_alive=lambda _pid: False)
    reader = SharedMemoryFrameReader(camera_id, unregister_from_resource_tracker=False)
    verifier: shared_memory.SharedMemory | None = None
    try:
        replacement.open()
        assert replacement.created
        assert replacement.header.camera_session_id != first.header.camera_session_id
        assert reader.attach()
        reader.close()
        verifier = shared_memory.SharedMemory(name=shared_memory_name(camera_id), create=False)
        assert verifier.size == segment_size(64)
    finally:
        if verifier is not None:
            verifier.close()
        replacement.close()
        first.close()


def test_malformed_live_owner_is_rejected_conservatively() -> None:
    camera_id = _camera_id()
    name = shared_memory_name(camera_id)
    malformed = shared_memory.SharedMemory(name=name, create=True, size=segment_size(64))
    try:
        malformed.buf[:] = bytes(len(malformed.buf))
        malformed.buf[:8] = b"INVALID!"
        struct.pack_into("<I", malformed.buf, OWNER_PID_OFFSET, 4321)
        candidate = SharedMemoryFrameWriter(camera_id, 64, uuid4().bytes, pid_is_alive=lambda pid: pid == 4321)
        with pytest.raises(LiveOwnerError, match="live PID 4321"):
            candidate.open()
    finally:
        malformed.close()
        malformed.unlink()


def test_creator_normal_close_unlinks_segment() -> None:
    camera_id = _camera_id()
    writer = SharedMemoryFrameWriter(camera_id, 64, uuid4().bytes)
    writer.open()
    writer.close()
    with pytest.raises(FileNotFoundError):
        shared_memory.SharedMemory(name=shared_memory_name(camera_id), create=False)
