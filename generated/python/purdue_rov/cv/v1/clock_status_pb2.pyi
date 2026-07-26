from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class ClockStatus(_message.Message):
    __slots__ = ["device_id", "offset_ms", "report_time_unix_ns", "synchronized"]
    DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    OFFSET_MS_FIELD_NUMBER: _ClassVar[int]
    REPORT_TIME_UNIX_NS_FIELD_NUMBER: _ClassVar[int]
    SYNCHRONIZED_FIELD_NUMBER: _ClassVar[int]
    device_id: str
    offset_ms: float
    report_time_unix_ns: int
    synchronized: bool
    def __init__(self, device_id: _Optional[str] = ..., report_time_unix_ns: _Optional[int] = ..., synchronized: bool = ..., offset_ms: _Optional[float] = ...) -> None: ...
