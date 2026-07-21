from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

COMPONENT_STATE_UNSPECIFIED: ComponentState
DEGRADED: ComponentState
DESCRIPTOR: _descriptor.FileDescriptor
ERROR: ComponentState
READY: ComponentState
RUNNING: ComponentState
STARTING: ComponentState
STOPPED: ComponentState
STOPPING: ComponentState

class ModuleState(_message.Message):
    __slots__ = ["error_code", "message", "previous_state", "publisher_session_id", "source_id", "state", "state_changed_time_unix_ns"]
    ERROR_CODE_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    PREVIOUS_STATE_FIELD_NUMBER: _ClassVar[int]
    PUBLISHER_SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    STATE_CHANGED_TIME_UNIX_NS_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    error_code: str
    message: str
    previous_state: ComponentState
    publisher_session_id: bytes
    source_id: str
    state: ComponentState
    state_changed_time_unix_ns: int
    def __init__(self, source_id: _Optional[str] = ..., publisher_session_id: _Optional[bytes] = ..., state: _Optional[_Union[ComponentState, str]] = ..., previous_state: _Optional[_Union[ComponentState, str]] = ..., state_changed_time_unix_ns: _Optional[int] = ..., error_code: _Optional[str] = ..., message: _Optional[str] = ...) -> None: ...

class ComponentState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
