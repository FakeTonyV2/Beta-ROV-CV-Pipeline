from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class BoundingBoxResult(_message.Message):
    __slots__ = ["camera_id", "camera_session_id", "capture_time_unix_ns", "detections", "frame_number"]
    CAMERA_ID_FIELD_NUMBER: _ClassVar[int]
    CAMERA_SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    CAPTURE_TIME_UNIX_NS_FIELD_NUMBER: _ClassVar[int]
    DETECTIONS_FIELD_NUMBER: _ClassVar[int]
    FRAME_NUMBER_FIELD_NUMBER: _ClassVar[int]
    camera_id: str
    camera_session_id: bytes
    capture_time_unix_ns: int
    detections: _containers.RepeatedCompositeFieldContainer[Detection]
    frame_number: int
    def __init__(self, camera_id: _Optional[str] = ..., camera_session_id: _Optional[bytes] = ..., frame_number: _Optional[int] = ..., capture_time_unix_ns: _Optional[int] = ..., detections: _Optional[_Iterable[_Union[Detection, _Mapping]]] = ...) -> None: ...

class Detection(_message.Message):
    __slots__ = ["class_id", "class_name", "confidence", "height", "tracking_id", "width", "x", "y"]
    CLASS_ID_FIELD_NUMBER: _ClassVar[int]
    CLASS_NAME_FIELD_NUMBER: _ClassVar[int]
    CONFIDENCE_FIELD_NUMBER: _ClassVar[int]
    HEIGHT_FIELD_NUMBER: _ClassVar[int]
    TRACKING_ID_FIELD_NUMBER: _ClassVar[int]
    WIDTH_FIELD_NUMBER: _ClassVar[int]
    X_FIELD_NUMBER: _ClassVar[int]
    Y_FIELD_NUMBER: _ClassVar[int]
    class_id: int
    class_name: str
    confidence: float
    height: float
    tracking_id: int
    width: float
    x: float
    y: float
    def __init__(self, class_id: _Optional[int] = ..., class_name: _Optional[str] = ..., confidence: _Optional[float] = ..., x: _Optional[float] = ..., y: _Optional[float] = ..., width: _Optional[float] = ..., height: _Optional[float] = ..., tracking_id: _Optional[int] = ...) -> None: ...
