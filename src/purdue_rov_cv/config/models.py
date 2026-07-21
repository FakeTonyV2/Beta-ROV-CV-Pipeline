"""Strict Pydantic models for the YAML application configuration."""

from pydantic import BaseModel, ConfigDict


class ConfigModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class DeviceConfig(ConfigModel):
    device_id: str
    execution_target: str


class NetworkConfig(ConfigModel):
    tether_interface: str
    rov_ip: str
    surface_ip: str


class ClockConfig(ConfigModel):
    server_ip: str
    maximum_offset_ms: int
    check_interval_seconds: int
    invalid_after_failures: int


class BrokerConfig(ConfigModel):
    publisher_endpoint: str
    subscriber_endpoint: str


class ControlConfig(ConfigModel):
    client_endpoint: str
    module_endpoint: str


class MessagingConfig(ConfigModel):
    broker: BrokerConfig
    control: ControlConfig
    max_message_bytes: int
    result_send_hwm: int
    result_receive_hwm: int


class DiagnosticsConfig(ConfigModel):
    publish_interval_ms: int
    log_level: str


class DebugSnapshotsConfig(ConfigModel):
    enabled: bool
    maximum_rate_hz: float
    maximum_width: int
    maximum_height: int
    jpeg_quality: int


class StructuredRecordingConfig(ConfigModel):
    chunk_size_bytes: int
    compression: str


class RecordingConfig(ConfigModel):
    enabled: bool
    directory: str
    video_segment_seconds: int
    minimum_free_space_gib: int
    structured: StructuredRecordingConfig


class CameraConfig(ConfigModel):
    adapter: str
    device_path: str
    device_path_kind: str
    format: str
    width: int
    height: int
    frame_rate: int
    stream_index: int
    stream_to_surface: bool
    cv_enabled: bool
    allow_software_encode: bool
    slot_capacity_bytes: int


class DynamicTaskConfig(ConfigModel):
    confidence_threshold: float


class ArtifactConfig(ConfigModel):
    format: str
    path: str
    sha256: str
    runtime: str


class TaskConfig(ConfigModel):
    module_class: str
    enabled: bool
    input_camera: str
    execution_target: str
    max_input_fps: int
    processing_deadline_ms: int
    publish_topic: str
    payload_type: str
    dynamic: DynamicTaskConfig
    artifact: ArtifactConfig


class CamerasConfig(ConfigModel):
    front_camera: CameraConfig


class TasksConfig(ConfigModel):
    gate_detection: TaskConfig


class AppConfig(ConfigModel):
    schema_version: int
    device: DeviceConfig
    network: NetworkConfig
    clock: ClockConfig
    messaging: MessagingConfig
    diagnostics: DiagnosticsConfig
    debug_snapshots: DebugSnapshotsConfig
    recording: RecordingConfig
    cameras: CamerasConfig
    tasks: TasksConfig
