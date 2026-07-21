from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class DebugSnapshot(_message.Message):
    __slots__ = ["camera_id", "camera_session_id", "capture_time_unix_ns", "frame_number", "height", "jpeg_data", "jpeg_quality", "width"]
    CAMERA_ID_FIELD_NUMBER: _ClassVar[int]
    CAMERA_SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    CAPTURE_TIME_UNIX_NS_FIELD_NUMBER: _ClassVar[int]
    FRAME_NUMBER_FIELD_NUMBER: _ClassVar[int]
    HEIGHT_FIELD_NUMBER: _ClassVar[int]
    JPEG_DATA_FIELD_NUMBER: _ClassVar[int]
    JPEG_QUALITY_FIELD_NUMBER: _ClassVar[int]
    WIDTH_FIELD_NUMBER: _ClassVar[int]
    camera_id: str
    camera_session_id: bytes
    capture_time_unix_ns: int
    frame_number: int
    height: int
    jpeg_data: bytes
    jpeg_quality: int
    width: int
    def __init__(self, camera_id: _Optional[str] = ..., camera_session_id: _Optional[bytes] = ..., frame_number: _Optional[int] = ..., capture_time_unix_ns: _Optional[int] = ..., width: _Optional[int] = ..., height: _Optional[int] = ..., jpeg_quality: _Optional[int] = ..., jpeg_data: _Optional[bytes] = ...) -> None: ...
