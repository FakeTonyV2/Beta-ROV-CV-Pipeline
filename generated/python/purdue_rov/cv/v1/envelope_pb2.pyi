from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

CLOCK_STATUS: MessageType
CV_RESULT: MessageType
DEBUG_SNAPSHOT: MessageType
DESCRIPTOR: _descriptor.FileDescriptor
DIAGNOSTIC: MessageType
EVENT: MessageType
FRAME_INDEX: MessageType
MESSAGE_TYPE_UNSPECIFIED: MessageType
MODULE_STATE: MessageType

class MessageEnvelope(_message.Message):
    __slots__ = ["camera_id", "camera_session_id", "capture_time_unix_ns", "frame_number", "message_type", "payload", "payload_encoding", "payload_size_bytes", "payload_type", "publish_time_unix_ns", "publisher_session_id", "schema_version", "sequence_number", "source_id", "source_monotonic_ns", "task_id"]
    CAMERA_ID_FIELD_NUMBER: _ClassVar[int]
    CAMERA_SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    CAPTURE_TIME_UNIX_NS_FIELD_NUMBER: _ClassVar[int]
    FRAME_NUMBER_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_TYPE_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_ENCODING_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_SIZE_BYTES_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_TYPE_FIELD_NUMBER: _ClassVar[int]
    PUBLISHER_SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    PUBLISH_TIME_UNIX_NS_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_VERSION_FIELD_NUMBER: _ClassVar[int]
    SEQUENCE_NUMBER_FIELD_NUMBER: _ClassVar[int]
    SOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_MONOTONIC_NS_FIELD_NUMBER: _ClassVar[int]
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    camera_id: str
    camera_session_id: bytes
    capture_time_unix_ns: int
    frame_number: int
    message_type: MessageType
    payload: bytes
    payload_encoding: str
    payload_size_bytes: int
    payload_type: str
    publish_time_unix_ns: int
    publisher_session_id: bytes
    schema_version: int
    sequence_number: int
    source_id: str
    source_monotonic_ns: int
    task_id: str
    def __init__(self, message_type: _Optional[_Union[MessageType, str]] = ..., payload_type: _Optional[str] = ..., task_id: _Optional[str] = ..., source_id: _Optional[str] = ..., camera_id: _Optional[str] = ..., camera_session_id: _Optional[bytes] = ..., frame_number: _Optional[int] = ..., capture_time_unix_ns: _Optional[int] = ..., publish_time_unix_ns: _Optional[int] = ..., source_monotonic_ns: _Optional[int] = ..., publisher_session_id: _Optional[bytes] = ..., sequence_number: _Optional[int] = ..., schema_version: _Optional[int] = ..., payload_encoding: _Optional[str] = ..., payload_size_bytes: _Optional[int] = ..., payload: _Optional[bytes] = ...) -> None: ...

class MessageType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
