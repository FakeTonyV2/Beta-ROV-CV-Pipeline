from purdue_rov.cv.v1 import module_state_pb2 as _module_state_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class CameraMetrics(_message.Message):
    __slots__ = ["current_height", "current_pixel_format", "current_width", "frame_timeouts", "frames_per_second", "frames_received", "pipeline_restarts", "shared_memory_write_count", "usb_device_present"]
    CURRENT_HEIGHT_FIELD_NUMBER: _ClassVar[int]
    CURRENT_PIXEL_FORMAT_FIELD_NUMBER: _ClassVar[int]
    CURRENT_WIDTH_FIELD_NUMBER: _ClassVar[int]
    FRAMES_PER_SECOND_FIELD_NUMBER: _ClassVar[int]
    FRAMES_RECEIVED_FIELD_NUMBER: _ClassVar[int]
    FRAME_TIMEOUTS_FIELD_NUMBER: _ClassVar[int]
    PIPELINE_RESTARTS_FIELD_NUMBER: _ClassVar[int]
    SHARED_MEMORY_WRITE_COUNT_FIELD_NUMBER: _ClassVar[int]
    USB_DEVICE_PRESENT_FIELD_NUMBER: _ClassVar[int]
    current_height: int
    current_pixel_format: str
    current_width: int
    frame_timeouts: int
    frames_per_second: float
    frames_received: int
    pipeline_restarts: int
    shared_memory_write_count: int
    usb_device_present: bool
    def __init__(self, frames_received: _Optional[int] = ..., frames_per_second: _Optional[float] = ..., frame_timeouts: _Optional[int] = ..., pipeline_restarts: _Optional[int] = ..., shared_memory_write_count: _Optional[int] = ..., current_width: _Optional[int] = ..., current_height: _Optional[int] = ..., current_pixel_format: _Optional[str] = ..., usb_device_present: bool = ...) -> None: ...

class DiagnosticStatus(_message.Message):
    __slots__ = ["camera", "last_error_code", "last_error_message", "messaging", "module", "process_cpu_percent", "report_time_unix_ns", "resident_memory_bytes", "source_id", "state", "system", "thread_count", "uptime_seconds", "video"]
    CAMERA_FIELD_NUMBER: _ClassVar[int]
    LAST_ERROR_CODE_FIELD_NUMBER: _ClassVar[int]
    LAST_ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    MESSAGING_FIELD_NUMBER: _ClassVar[int]
    MODULE_FIELD_NUMBER: _ClassVar[int]
    PROCESS_CPU_PERCENT_FIELD_NUMBER: _ClassVar[int]
    REPORT_TIME_UNIX_NS_FIELD_NUMBER: _ClassVar[int]
    RESIDENT_MEMORY_BYTES_FIELD_NUMBER: _ClassVar[int]
    SOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    SYSTEM_FIELD_NUMBER: _ClassVar[int]
    THREAD_COUNT_FIELD_NUMBER: _ClassVar[int]
    UPTIME_SECONDS_FIELD_NUMBER: _ClassVar[int]
    VIDEO_FIELD_NUMBER: _ClassVar[int]
    camera: CameraMetrics
    last_error_code: str
    last_error_message: str
    messaging: MessagingMetrics
    module: ModuleMetrics
    process_cpu_percent: float
    report_time_unix_ns: int
    resident_memory_bytes: int
    source_id: str
    state: _module_state_pb2.ComponentState
    system: SystemMetrics
    thread_count: int
    uptime_seconds: float
    video: VideoMetrics
    def __init__(self, source_id: _Optional[str] = ..., report_time_unix_ns: _Optional[int] = ..., process_cpu_percent: _Optional[float] = ..., resident_memory_bytes: _Optional[int] = ..., thread_count: _Optional[int] = ..., uptime_seconds: _Optional[float] = ..., state: _Optional[_Union[_module_state_pb2.ComponentState, str]] = ..., last_error_code: _Optional[str] = ..., last_error_message: _Optional[str] = ..., camera: _Optional[_Union[CameraMetrics, _Mapping]] = ..., module: _Optional[_Union[ModuleMetrics, _Mapping]] = ..., messaging: _Optional[_Union[MessagingMetrics, _Mapping]] = ..., video: _Optional[_Union[VideoMetrics, _Mapping]] = ..., system: _Optional[_Union[SystemMetrics, _Mapping]] = ...) -> None: ...

class MessagingMetrics(_message.Message):
    __slots__ = ["invalid_messages", "messages_received", "messages_sent", "observed_sequence_gaps", "reconnect_count", "unknown_payload_types"]
    INVALID_MESSAGES_FIELD_NUMBER: _ClassVar[int]
    MESSAGES_RECEIVED_FIELD_NUMBER: _ClassVar[int]
    MESSAGES_SENT_FIELD_NUMBER: _ClassVar[int]
    OBSERVED_SEQUENCE_GAPS_FIELD_NUMBER: _ClassVar[int]
    RECONNECT_COUNT_FIELD_NUMBER: _ClassVar[int]
    UNKNOWN_PAYLOAD_TYPES_FIELD_NUMBER: _ClassVar[int]
    invalid_messages: int
    messages_received: int
    messages_sent: int
    observed_sequence_gaps: int
    reconnect_count: int
    unknown_payload_types: int
    def __init__(self, messages_sent: _Optional[int] = ..., messages_received: _Optional[int] = ..., invalid_messages: _Optional[int] = ..., unknown_payload_types: _Optional[int] = ..., observed_sequence_gaps: _Optional[int] = ..., reconnect_count: _Optional[int] = ...) -> None: ...

class ModuleMetrics(_message.Message):
    __slots__ = ["average_processing_ms", "frames_dropped_before_processing", "frames_processed", "frames_read", "p95_processing_ms", "processing_deadline_misses", "processing_exceptions", "results_dropped_local_queue", "results_published", "zmq_send_dropped"]
    AVERAGE_PROCESSING_MS_FIELD_NUMBER: _ClassVar[int]
    FRAMES_DROPPED_BEFORE_PROCESSING_FIELD_NUMBER: _ClassVar[int]
    FRAMES_PROCESSED_FIELD_NUMBER: _ClassVar[int]
    FRAMES_READ_FIELD_NUMBER: _ClassVar[int]
    P95_PROCESSING_MS_FIELD_NUMBER: _ClassVar[int]
    PROCESSING_DEADLINE_MISSES_FIELD_NUMBER: _ClassVar[int]
    PROCESSING_EXCEPTIONS_FIELD_NUMBER: _ClassVar[int]
    RESULTS_DROPPED_LOCAL_QUEUE_FIELD_NUMBER: _ClassVar[int]
    RESULTS_PUBLISHED_FIELD_NUMBER: _ClassVar[int]
    ZMQ_SEND_DROPPED_FIELD_NUMBER: _ClassVar[int]
    average_processing_ms: float
    frames_dropped_before_processing: int
    frames_processed: int
    frames_read: int
    p95_processing_ms: float
    processing_deadline_misses: int
    processing_exceptions: int
    results_dropped_local_queue: int
    results_published: int
    zmq_send_dropped: int
    def __init__(self, frames_read: _Optional[int] = ..., frames_processed: _Optional[int] = ..., frames_dropped_before_processing: _Optional[int] = ..., processing_exceptions: _Optional[int] = ..., processing_deadline_misses: _Optional[int] = ..., average_processing_ms: _Optional[float] = ..., p95_processing_ms: _Optional[float] = ..., results_published: _Optional[int] = ..., results_dropped_local_queue: _Optional[int] = ..., zmq_send_dropped: _Optional[int] = ...) -> None: ...

class SystemMetrics(_message.Message):
    __slots__ = ["clock_offset_ms", "clock_synchronized", "cpu_temperature_c", "disk_free_bytes", "memory_available_bytes", "tether_link_up"]
    CLOCK_OFFSET_MS_FIELD_NUMBER: _ClassVar[int]
    CLOCK_SYNCHRONIZED_FIELD_NUMBER: _ClassVar[int]
    CPU_TEMPERATURE_C_FIELD_NUMBER: _ClassVar[int]
    DISK_FREE_BYTES_FIELD_NUMBER: _ClassVar[int]
    MEMORY_AVAILABLE_BYTES_FIELD_NUMBER: _ClassVar[int]
    TETHER_LINK_UP_FIELD_NUMBER: _ClassVar[int]
    clock_offset_ms: float
    clock_synchronized: bool
    cpu_temperature_c: float
    disk_free_bytes: int
    memory_available_bytes: int
    tether_link_up: bool
    def __init__(self, cpu_temperature_c: _Optional[float] = ..., memory_available_bytes: _Optional[int] = ..., disk_free_bytes: _Optional[int] = ..., clock_synchronized: bool = ..., clock_offset_ms: _Optional[float] = ..., tether_link_up: bool = ...) -> None: ...

class VideoMetrics(_message.Message):
    __slots__ = ["decoded_frames", "frame_index_hits", "frame_index_misses", "last_frame_age_ms", "rtp_packets_lost", "rtp_packets_received", "stream_restarts"]
    DECODED_FRAMES_FIELD_NUMBER: _ClassVar[int]
    FRAME_INDEX_HITS_FIELD_NUMBER: _ClassVar[int]
    FRAME_INDEX_MISSES_FIELD_NUMBER: _ClassVar[int]
    LAST_FRAME_AGE_MS_FIELD_NUMBER: _ClassVar[int]
    RTP_PACKETS_LOST_FIELD_NUMBER: _ClassVar[int]
    RTP_PACKETS_RECEIVED_FIELD_NUMBER: _ClassVar[int]
    STREAM_RESTARTS_FIELD_NUMBER: _ClassVar[int]
    decoded_frames: int
    frame_index_hits: int
    frame_index_misses: int
    last_frame_age_ms: int
    rtp_packets_lost: int
    rtp_packets_received: int
    stream_restarts: int
    def __init__(self, rtp_packets_received: _Optional[int] = ..., rtp_packets_lost: _Optional[int] = ..., decoded_frames: _Optional[int] = ..., frame_index_hits: _Optional[int] = ..., frame_index_misses: _Optional[int] = ..., stream_restarts: _Optional[int] = ..., last_frame_age_ms: _Optional[int] = ...) -> None: ...
