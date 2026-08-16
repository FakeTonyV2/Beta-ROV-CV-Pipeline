"""Pure deterministic RTP/RTCP allocation for configured camera streams."""

from __future__ import annotations

from dataclasses import dataclass

RTP_PORT_BASE = 5000
RTP_PAYLOAD_TYPE_BASE = 96
RTP_PAYLOAD_TYPE_MIN = 96
RTP_PAYLOAD_TYPE_MAX = 127


@dataclass(frozen=True)
class StreamAllocation:
    camera_id: str
    stream_index: int
    rtp_port: int
    rtcp_port: int
    rtp_payload_type: int


def derive_stream_allocation(camera_id: str, stream_index: int) -> StreamAllocation:
    """Derive ports and dynamic RTP payload type from the single stream index source."""
    return StreamAllocation(
        camera_id=camera_id,
        stream_index=stream_index,
        rtp_port=RTP_PORT_BASE + (2 * stream_index),
        rtcp_port=RTP_PORT_BASE + (2 * stream_index) + 1,
        rtp_payload_type=RTP_PAYLOAD_TYPE_BASE + stream_index,
    )
