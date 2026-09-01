"""Strict, typed representation of the authoritative mission YAML shape."""

from __future__ import annotations

import re
from enum import StrEnum
from ipaddress import IPv4Address
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, field_validator

IDENTIFIER_PATTERN = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
SUPPORTED_SCHEMA_VERSION = 1
DIAGNOSTICS_PUBLISH_INTERVAL_MIN_MS = 500
DIAGNOSTICS_PUBLISH_INTERVAL_MAX_MS = 5_000


def validate_identifier(value: str) -> str:
    if not IDENTIFIER_PATTERN.fullmatch(value):
        raise ValueError("must use lowercase letters, digits, and underscores; it must start with a letter")
    return value


def validate_absolute_linux_path(value: str | Path) -> Path:
    """Validate lexical Linux path safety without resolving any symlinks."""
    if not isinstance(value, (str, Path)):
        raise ValueError("must be a path string")
    raw = value.as_posix() if isinstance(value, Path) else value
    if not raw.startswith("/"):
        raise ValueError("must be an absolute path")
    if raw.endswith("/") or "\\" in raw:
        raise ValueError("must not contain a trailing separator or backslash")
    parts = raw.split("/")
    if any(part in {"", ".", ".."} for part in parts[1:]):
        raise ValueError("must not contain empty, dot, or parent path components")
    return Path(raw)


def validate_nonblank(value: str) -> str:
    if not value.strip():
        raise ValueError("must not be empty")
    return value


class ConfigModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class LogLevel(StrEnum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class CameraPathKind(StrEnum):
    BY_ID = "by_id"
    FALLBACK = "fallback"


class CameraAdapter(StrEnum):
    V4L2 = "v4l2"
    OAKD = "oakd"


class CameraFormat(StrEnum):
    H264 = "h264"
    MJPEG = "mjpeg"
    YUYV = "yuyv"
    NV12 = "nv12"


class ArtifactFormat(StrEnum):
    ONNX = "onnx"
    TENSORRT = "tensorrt"


class Runtime(StrEnum):
    ONNXRUNTIME = "onnxruntime"
    TENSORRT = "tensorrt"


class DeviceConfig(ConfigModel):
    device_id: str
    execution_target: str

    _validate_device_id = field_validator("device_id")(validate_identifier)
    _validate_execution_target = field_validator("execution_target")(validate_identifier)


class NetworkConfig(ConfigModel):
    tether_interface: str
    rov_ip: IPv4Address
    surface_ip: IPv4Address

    _validate_tether_interface = field_validator("tether_interface")(validate_identifier)

    @field_validator("rov_ip", "surface_ip", mode="before")
    @classmethod
    def _ipv4_from_yaml(cls, value: IPv4Address | str) -> IPv4Address | str:
        return IPv4Address(value) if isinstance(value, str) else value


class ClockConfig(ConfigModel):
    server_ip: IPv4Address
    maximum_offset_ms: int = Field(gt=0, le=60_000)
    check_interval_seconds: int = Field(gt=0, le=3_600)
    invalid_after_failures: int = Field(gt=0, le=100)

    @field_validator("server_ip", mode="before")
    @classmethod
    def _ipv4_from_yaml(cls, value: IPv4Address | str) -> IPv4Address | str:
        return IPv4Address(value) if isinstance(value, str) else value


class BrokerConfig(ConfigModel):
    publisher_endpoint: str
    subscriber_endpoint: str


class ControlConfig(ConfigModel):
    client_endpoint: str
    module_endpoint: str


class MessagingConfig(ConfigModel):
    broker: BrokerConfig
    control: ControlConfig
    max_message_bytes: int = Field(ge=1, le=4 * 1024 * 1024)
    result_send_hwm: int = Field(gt=0, le=10_000)
    result_receive_hwm: int = Field(gt=0, le=10_000)


class DiagnosticsConfig(ConfigModel):
    publish_interval_ms: int = Field(
        ge=DIAGNOSTICS_PUBLISH_INTERVAL_MIN_MS,
        le=DIAGNOSTICS_PUBLISH_INTERVAL_MAX_MS,
    )
    log_level: LogLevel

    @field_validator("log_level", mode="before")
    @classmethod
    def _log_level_from_yaml(cls, value: LogLevel | str) -> LogLevel | str:
        return LogLevel(value) if isinstance(value, str) else value


class DebugSnapshotsConfig(ConfigModel):
    enabled: bool
    maximum_rate_hz: float = Field(gt=0.0, le=60.0)
    maximum_width: int = Field(ge=1, le=640)
    maximum_height: int = Field(ge=1, le=360)
    jpeg_quality: int = Field(ge=1, le=95)


class StructuredRecordingConfig(ConfigModel):
    chunk_size_bytes: int = Field(gt=0)
    compression: str


class RecordingConfig(ConfigModel):
    enabled: bool
    directory: Path
    video_segment_seconds: int = Field(gt=0)
    minimum_free_space_gib: int = Field(ge=0)
    structured: StructuredRecordingConfig

    @field_validator("directory", mode="before")
    @classmethod
    def _absolute_directory(cls, value: str | Path) -> Path:
        return validate_absolute_linux_path(value)


class CameraConfig(ConfigModel):
    adapter: CameraAdapter
    device_path: Path
    device_path_kind: CameraPathKind
    format: CameraFormat
    width: int = Field(gt=0, le=7_680)
    height: int = Field(gt=0, le=4_320)
    frame_rate: int = Field(gt=0, le=240)
    stream_index: int = Field(ge=0)
    stream_to_surface: bool
    cv_enabled: bool
    allow_software_encode: bool
    slot_capacity_bytes: int = Field(gt=0)

    @field_validator("device_path", mode="before")
    @classmethod
    def _absolute_device_path(cls, value: str | Path) -> Path:
        return validate_absolute_linux_path(value)

    @field_validator("adapter", mode="before")
    @classmethod
    def _adapter_from_yaml(cls, value: CameraAdapter | str) -> CameraAdapter | str:
        return CameraAdapter(value) if isinstance(value, str) else value

    @field_validator("device_path_kind", mode="before")
    @classmethod
    def _path_kind_from_yaml(cls, value: CameraPathKind | str) -> CameraPathKind | str:
        return CameraPathKind(value) if isinstance(value, str) else value

    @field_validator("format", mode="before")
    @classmethod
    def _format_from_yaml(cls, value: CameraFormat | str) -> CameraFormat | str:
        return CameraFormat(value) if isinstance(value, str) else value


class CameraLimitsConfig(ConfigModel):
    maximum_configured: int = Field(gt=0)
    maximum_active: int = Field(gt=0)


class DynamicTaskConfig(ConfigModel):
    confidence_threshold: float = Field(ge=0.0, le=1.0)


class ArtifactConfig(ConfigModel):
    format: ArtifactFormat
    path: Path
    sha256: str
    runtime: Runtime

    @field_validator("path", mode="before")
    @classmethod
    def _absolute_artifact_path(cls, value: str | Path) -> Path:
        return validate_absolute_linux_path(value)

    @field_validator("sha256")
    @classmethod
    def _sha256(cls, value: str) -> str:
        if not SHA256_PATTERN.fullmatch(value):
            raise ValueError("must contain exactly 64 lowercase hexadecimal characters")
        return value

    @field_validator("format", mode="before")
    @classmethod
    def _format_from_yaml(cls, value: ArtifactFormat | str) -> ArtifactFormat | str:
        return ArtifactFormat(value) if isinstance(value, str) else value

    @field_validator("runtime", mode="before")
    @classmethod
    def _runtime_from_yaml(cls, value: Runtime | str) -> Runtime | str:
        return Runtime(value) if isinstance(value, str) else value


class TaskConfig(ConfigModel):
    module_class: str
    enabled: bool
    input_camera: str
    execution_target: str
    max_input_fps: int = Field(gt=0, le=240)
    processing_deadline_ms: int = Field(gt=0, le=60_000)
    publish_topic: str
    payload_type: str
    dynamic: DynamicTaskConfig
    artifact: ArtifactConfig

    _validate_input_camera = field_validator("input_camera")(validate_identifier)
    _validate_execution_target = field_validator("execution_target")(validate_identifier)
    _validate_module_class = field_validator("module_class")(validate_nonblank)


class AppConfig(ConfigModel):
    schema_version: int
    device: DeviceConfig
    network: NetworkConfig
    clock: ClockConfig
    messaging: MessagingConfig
    diagnostics: DiagnosticsConfig
    debug_snapshots: DebugSnapshotsConfig
    recording: RecordingConfig
    camera_limits: CameraLimitsConfig
    cameras: dict[str, CameraConfig]
    tasks: dict[str, TaskConfig]
