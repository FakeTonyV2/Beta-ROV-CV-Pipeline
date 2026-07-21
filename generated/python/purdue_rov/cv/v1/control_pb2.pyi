from google.protobuf import struct_pb2 as _struct_pb2
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Mapping as _Mapping, Optional as _Optional, Union as _Union

COMMAND_STATUS_ACCEPTED: CommandStatus
COMMAND_STATUS_COMPLETED: CommandStatus
COMMAND_STATUS_FAILED: CommandStatus
COMMAND_STATUS_OUTCOME_UNKNOWN: CommandStatus
COMMAND_STATUS_RECEIVED: CommandStatus
COMMAND_STATUS_REJECTED: CommandStatus
COMMAND_STATUS_UNSPECIFIED: CommandStatus
DESCRIPTOR: _descriptor.FileDescriptor

class CommandRequest(_message.Message):
    __slots__ = ["command_id", "get_command_status", "get_status", "issued_time_unix_ns", "request_debug_snapshot", "requested_timeout_ms", "reset", "set_dynamic_config", "set_mode", "start", "start_recording", "stop", "stop_recording", "target_id"]
    COMMAND_ID_FIELD_NUMBER: _ClassVar[int]
    GET_COMMAND_STATUS_FIELD_NUMBER: _ClassVar[int]
    GET_STATUS_FIELD_NUMBER: _ClassVar[int]
    ISSUED_TIME_UNIX_NS_FIELD_NUMBER: _ClassVar[int]
    REQUESTED_TIMEOUT_MS_FIELD_NUMBER: _ClassVar[int]
    REQUEST_DEBUG_SNAPSHOT_FIELD_NUMBER: _ClassVar[int]
    RESET_FIELD_NUMBER: _ClassVar[int]
    SET_DYNAMIC_CONFIG_FIELD_NUMBER: _ClassVar[int]
    SET_MODE_FIELD_NUMBER: _ClassVar[int]
    START_FIELD_NUMBER: _ClassVar[int]
    START_RECORDING_FIELD_NUMBER: _ClassVar[int]
    STOP_FIELD_NUMBER: _ClassVar[int]
    STOP_RECORDING_FIELD_NUMBER: _ClassVar[int]
    TARGET_ID_FIELD_NUMBER: _ClassVar[int]
    command_id: bytes
    get_command_status: GetCommandStatus
    get_status: GetStatus
    issued_time_unix_ns: int
    request_debug_snapshot: RequestDebugSnapshot
    requested_timeout_ms: int
    reset: Reset
    set_dynamic_config: SetDynamicConfig
    set_mode: SetMode
    start: Start
    start_recording: StartRecording
    stop: Stop
    stop_recording: StopRecording
    target_id: str
    def __init__(self, command_id: _Optional[bytes] = ..., target_id: _Optional[str] = ..., issued_time_unix_ns: _Optional[int] = ..., requested_timeout_ms: _Optional[int] = ..., get_status: _Optional[_Union[GetStatus, _Mapping]] = ..., start: _Optional[_Union[Start, _Mapping]] = ..., stop: _Optional[_Union[Stop, _Mapping]] = ..., set_mode: _Optional[_Union[SetMode, _Mapping]] = ..., set_dynamic_config: _Optional[_Union[SetDynamicConfig, _Mapping]] = ..., request_debug_snapshot: _Optional[_Union[RequestDebugSnapshot, _Mapping]] = ..., start_recording: _Optional[_Union[StartRecording, _Mapping]] = ..., stop_recording: _Optional[_Union[StopRecording, _Mapping]] = ..., reset: _Optional[_Union[Reset, _Mapping]] = ..., get_command_status: _Optional[_Union[GetCommandStatus, _Mapping]] = ...) -> None: ...

class CommandResponse(_message.Message):
    __slots__ = ["command_id", "error_code", "message", "response_time_unix_ns", "resulting_state", "status", "target_id"]
    COMMAND_ID_FIELD_NUMBER: _ClassVar[int]
    ERROR_CODE_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    RESPONSE_TIME_UNIX_NS_FIELD_NUMBER: _ClassVar[int]
    RESULTING_STATE_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    TARGET_ID_FIELD_NUMBER: _ClassVar[int]
    command_id: bytes
    error_code: str
    message: str
    response_time_unix_ns: int
    resulting_state: str
    status: CommandStatus
    target_id: str
    def __init__(self, command_id: _Optional[bytes] = ..., target_id: _Optional[str] = ..., status: _Optional[_Union[CommandStatus, str]] = ..., error_code: _Optional[str] = ..., message: _Optional[str] = ..., resulting_state: _Optional[str] = ..., response_time_unix_ns: _Optional[int] = ...) -> None: ...

class GetCommandStatus(_message.Message):
    __slots__ = ["target_command_id"]
    TARGET_COMMAND_ID_FIELD_NUMBER: _ClassVar[int]
    target_command_id: bytes
    def __init__(self, target_command_id: _Optional[bytes] = ...) -> None: ...

class GetStatus(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class RequestDebugSnapshot(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class Reset(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class SetDynamicConfig(_message.Message):
    __slots__ = ["fields"]
    FIELDS_FIELD_NUMBER: _ClassVar[int]
    fields: _struct_pb2.Struct
    def __init__(self, fields: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ...) -> None: ...

class SetMode(_message.Message):
    __slots__ = ["mode"]
    MODE_FIELD_NUMBER: _ClassVar[int]
    mode: str
    def __init__(self, mode: _Optional[str] = ...) -> None: ...

class Start(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class StartRecording(_message.Message):
    __slots__ = ["session_label"]
    SESSION_LABEL_FIELD_NUMBER: _ClassVar[int]
    session_label: str
    def __init__(self, session_label: _Optional[str] = ...) -> None: ...

class Stop(_message.Message):
    __slots__ = ["graceful"]
    GRACEFUL_FIELD_NUMBER: _ClassVar[int]
    graceful: bool
    def __init__(self, graceful: bool = ...) -> None: ...

class StopRecording(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class CommandStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
