from purdue_rov.cv.v1 import module_state_pb2 as _module_state_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ModuleRegistration(_message.Message):
    __slots__ = ["current_state", "host_device_id", "module_id", "module_session_id", "process_id", "supported_command_types", "task_id"]
    CURRENT_STATE_FIELD_NUMBER: _ClassVar[int]
    HOST_DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    MODULE_ID_FIELD_NUMBER: _ClassVar[int]
    MODULE_SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    PROCESS_ID_FIELD_NUMBER: _ClassVar[int]
    SUPPORTED_COMMAND_TYPES_FIELD_NUMBER: _ClassVar[int]
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    current_state: _module_state_pb2.ComponentState
    host_device_id: str
    module_id: str
    module_session_id: bytes
    process_id: int
    supported_command_types: _containers.RepeatedScalarFieldContainer[str]
    task_id: str
    def __init__(self, module_id: _Optional[str] = ..., task_id: _Optional[str] = ..., module_session_id: _Optional[bytes] = ..., supported_command_types: _Optional[_Iterable[str]] = ..., current_state: _Optional[_Union[_module_state_pb2.ComponentState, str]] = ..., process_id: _Optional[int] = ..., host_device_id: _Optional[str] = ...) -> None: ...

class ModuleRegistrationResponse(_message.Message):
    __slots__ = ["accepted", "error_code", "message"]
    ACCEPTED_FIELD_NUMBER: _ClassVar[int]
    ERROR_CODE_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    accepted: bool
    error_code: str
    message: str
    def __init__(self, accepted: bool = ..., error_code: _Optional[str] = ..., message: _Optional[str] = ...) -> None: ...
