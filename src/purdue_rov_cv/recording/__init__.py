"""Production structured and encoded-video recording services."""

from .disk import GIB, DiskSpaceGuard, DiskSpaceSnapshot
from .mcap_writer import (
    FLUSH_INTERVAL_NS,
    FLUSH_MESSAGE_COUNT,
    MESSAGE_ENVELOPE_SCHEMA_NAME,
    FlushPolicy,
    McapSessionWriter,
    StructuredRecord,
    StructuredWriterLoop,
)
from .service import RecorderService, RecorderSubscriber
from .video import EncodedMatroskaRecorder, VideoSegmentPaths

__all__ = [
    "DiskSpaceGuard",
    "DiskSpaceSnapshot",
    "EncodedMatroskaRecorder",
    "FLUSH_INTERVAL_NS",
    "FLUSH_MESSAGE_COUNT",
    "FlushPolicy",
    "GIB",
    "MESSAGE_ENVELOPE_SCHEMA_NAME",
    "McapSessionWriter",
    "RecorderService",
    "RecorderSubscriber",
    "StructuredRecord",
    "StructuredWriterLoop",
    "VideoSegmentPaths",
]
