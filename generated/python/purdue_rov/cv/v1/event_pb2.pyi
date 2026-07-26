from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class SystemEvent(_message.Message):
    __slots__ = ["error_code", "event_time_unix_ns", "event_type", "message", "source_id"]
    ERROR_CODE_FIELD_NUMBER: _ClassVar[int]
    EVENT_TIME_UNIX_NS_FIELD_NUMBER: _ClassVar[int]
    EVENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    SOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    error_code: str
    event_time_unix_ns: int
    event_type: str
    message: str
    source_id: str
    def __init__(self, event_type: _Optional[str] = ..., source_id: _Optional[str] = ..., event_time_unix_ns: _Optional[int] = ..., error_code: _Optional[str] = ..., message: _Optional[str] = ...) -> None: ...
