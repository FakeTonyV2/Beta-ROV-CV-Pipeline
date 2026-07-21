from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ClassScore(_message.Message):
    __slots__ = ["class_id", "class_name", "confidence"]
    CLASS_ID_FIELD_NUMBER: _ClassVar[int]
    CLASS_NAME_FIELD_NUMBER: _ClassVar[int]
    CONFIDENCE_FIELD_NUMBER: _ClassVar[int]
    class_id: int
    class_name: str
    confidence: float
    def __init__(self, class_id: _Optional[int] = ..., class_name: _Optional[str] = ..., confidence: _Optional[float] = ...) -> None: ...

class ClassificationResult(_message.Message):
    __slots__ = ["camera_id", "camera_session_id", "capture_time_unix_ns", "classes", "frame_number"]
    CAMERA_ID_FIELD_NUMBER: _ClassVar[int]
    CAMERA_SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    CAPTURE_TIME_UNIX_NS_FIELD_NUMBER: _ClassVar[int]
    CLASSES_FIELD_NUMBER: _ClassVar[int]
    FRAME_NUMBER_FIELD_NUMBER: _ClassVar[int]
    camera_id: str
    camera_session_id: bytes
    capture_time_unix_ns: int
    classes: _containers.RepeatedCompositeFieldContainer[ClassScore]
    frame_number: int
    def __init__(self, camera_id: _Optional[str] = ..., camera_session_id: _Optional[bytes] = ..., frame_number: _Optional[int] = ..., capture_time_unix_ns: _Optional[int] = ..., classes: _Optional[_Iterable[_Union[ClassScore, _Mapping]]] = ...) -> None: ...
