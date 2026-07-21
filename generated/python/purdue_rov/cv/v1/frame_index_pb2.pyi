from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class FrameIndex(_message.Message):
    __slots__ = ["camera_id", "camera_session_id", "capture_monotonic_ns", "capture_time_unix_ns", "frame_number", "rtp_payload_type", "rtp_ssrc", "rtp_timestamp"]
    CAMERA_ID_FIELD_NUMBER: _ClassVar[int]
    CAMERA_SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    CAPTURE_MONOTONIC_NS_FIELD_NUMBER: _ClassVar[int]
    CAPTURE_TIME_UNIX_NS_FIELD_NUMBER: _ClassVar[int]
    FRAME_NUMBER_FIELD_NUMBER: _ClassVar[int]
    RTP_PAYLOAD_TYPE_FIELD_NUMBER: _ClassVar[int]
    RTP_SSRC_FIELD_NUMBER: _ClassVar[int]
    RTP_TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    camera_id: str
    camera_session_id: bytes
    capture_monotonic_ns: int
    capture_time_unix_ns: int
    frame_number: int
    rtp_payload_type: int
    rtp_ssrc: int
    rtp_timestamp: int
    def __init__(self, camera_id: _Optional[str] = ..., camera_session_id: _Optional[bytes] = ..., frame_number: _Optional[int] = ..., capture_time_unix_ns: _Optional[int] = ..., capture_monotonic_ns: _Optional[int] = ..., rtp_timestamp: _Optional[int] = ..., rtp_ssrc: _Optional[int] = ..., rtp_payload_type: _Optional[int] = ...) -> None: ...
