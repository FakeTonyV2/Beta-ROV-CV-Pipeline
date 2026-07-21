from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class TargetPoseResult(_message.Message):
    __slots__ = ["camera_id", "camera_session_id", "capture_time_unix_ns", "confidence", "coordinate_frame", "covariance", "frame_number", "pitch_rad", "roll_rad", "target_id", "x_m", "y_m", "yaw_rad", "z_m"]
    CAMERA_ID_FIELD_NUMBER: _ClassVar[int]
    CAMERA_SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    CAPTURE_TIME_UNIX_NS_FIELD_NUMBER: _ClassVar[int]
    CONFIDENCE_FIELD_NUMBER: _ClassVar[int]
    COORDINATE_FRAME_FIELD_NUMBER: _ClassVar[int]
    COVARIANCE_FIELD_NUMBER: _ClassVar[int]
    FRAME_NUMBER_FIELD_NUMBER: _ClassVar[int]
    PITCH_RAD_FIELD_NUMBER: _ClassVar[int]
    ROLL_RAD_FIELD_NUMBER: _ClassVar[int]
    TARGET_ID_FIELD_NUMBER: _ClassVar[int]
    X_M_FIELD_NUMBER: _ClassVar[int]
    YAW_RAD_FIELD_NUMBER: _ClassVar[int]
    Y_M_FIELD_NUMBER: _ClassVar[int]
    Z_M_FIELD_NUMBER: _ClassVar[int]
    camera_id: str
    camera_session_id: bytes
    capture_time_unix_ns: int
    confidence: float
    coordinate_frame: str
    covariance: _containers.RepeatedScalarFieldContainer[float]
    frame_number: int
    pitch_rad: float
    roll_rad: float
    target_id: str
    x_m: float
    y_m: float
    yaw_rad: float
    z_m: float
    def __init__(self, camera_id: _Optional[str] = ..., camera_session_id: _Optional[bytes] = ..., frame_number: _Optional[int] = ..., capture_time_unix_ns: _Optional[int] = ..., target_id: _Optional[str] = ..., coordinate_frame: _Optional[str] = ..., x_m: _Optional[float] = ..., y_m: _Optional[float] = ..., z_m: _Optional[float] = ..., roll_rad: _Optional[float] = ..., pitch_rad: _Optional[float] = ..., yaw_rad: _Optional[float] = ..., confidence: _Optional[float] = ..., covariance: _Optional[_Iterable[float]] = ...) -> None: ...
