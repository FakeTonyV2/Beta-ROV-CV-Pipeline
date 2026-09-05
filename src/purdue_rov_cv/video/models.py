"""Typed surface-video handoff and RTP correlation models."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class CorrelationQuality(StrEnum):
    EXACT = "EXACT"
    APPROXIMATE = "APPROXIMATE"
    UNMATCHED = "UNMATCHED"


@dataclass(frozen=True, slots=True)
class ResolvedFrameIdentity:
    camera_id: str
    camera_session_id: bytes
    frame_number: int
    capture_time_unix_ns: int


@dataclass(frozen=True, slots=True)
class FrameCorrelation:
    quality: CorrelationQuality
    identity: ResolvedFrameIdentity | None = None

    def __post_init__(self) -> None:
        if (self.quality is CorrelationQuality.UNMATCHED) != (self.identity is None):
            raise ValueError("UNMATCHED must omit identity and resolved correlations must include it")


@dataclass(frozen=True, slots=True)
class DecodedVideoFrame:
    pixels: bytes
    width: int
    height: int
    stride_bytes: int
    pixel_format: str
    presentation_timestamp_ns: int
    rtp_ssrc: int
    rtp_timestamp: int
    received_monotonic_ns: int


@dataclass(frozen=True, slots=True)
class ReceivedVideoFrame:
    frame: DecodedVideoFrame
    correlation: FrameCorrelation


@dataclass(frozen=True, slots=True)
class EncodedAccessUnit:
    data: bytes
    presentation_timestamp_ns: int
    is_delta_unit: bool


__all__ = [
    "CorrelationQuality",
    "DecodedVideoFrame",
    "EncodedAccessUnit",
    "FrameCorrelation",
    "ReceivedVideoFrame",
    "ResolvedFrameIdentity",
]
