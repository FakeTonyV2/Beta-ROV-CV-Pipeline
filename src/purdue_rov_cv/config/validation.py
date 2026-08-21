"""Pure cross-field and semantic validation for an already parsed AppConfig."""

from __future__ import annotations

import re
from ipaddress import IPv4Address
from pathlib import Path

from purdue_rov_cv.wire.payloads import PAYLOAD_REGISTRY
from purdue_rov_cv.wire.topics import TopicKind, validate_topic

from .issues import ConfigIssue
from .models import (
    IDENTIFIER_PATTERN,
    SUPPORTED_SCHEMA_VERSION,
    AppConfig,
    ArtifactFormat,
    CameraPathKind,
)
from .ports import RTP_PAYLOAD_TYPE_MAX, RTP_PAYLOAD_TYPE_MIN, derive_stream_allocation

_VIDEO_DEVICE_RE = re.compile(r"^/dev/video\d+$")
_TCP_ENDPOINT_RE = re.compile(r"^tcp://(?P<host>[^:/]+):(?P<port>\d+)$")
_RUNTIME_COMPATIBILITY = {
    ArtifactFormat.ONNX: {"onnxruntime", "tensorrt"},
    ArtifactFormat.TENSORRT: {"tensorrt"},
}


def _issue(code: str, path: str, message: str, **values: object) -> ConfigIssue:
    return ConfigIssue(code, path, message, values)


def _parse_tcp_endpoint(value: str, path: str, issues: list[ConfigIssue]) -> tuple[str, int] | None:
    match = _TCP_ENDPOINT_RE.fullmatch(value)
    if match is None:
        issues.append(_issue("ENDPOINT_INVALID", path, "must be tcp://<IPv4-address>:<port>", value=value))
        return None
    host, port_text = match.group("host", "port")
    try:
        IPv4Address(host)
    except ValueError:
        issues.append(
            _issue("ENDPOINT_HOST_INVALID", path, "mission endpoint host must be an explicit IPv4 address", value=host)
        )
        return None
    port = int(port_text)
    if not 1 <= port <= 65_535:
        issues.append(_issue("PORT_OUT_OF_RANGE", path, "port must be in 1..65535", value=port))
        return None
    return host, port


def _bindings_conflict(first_host: str, second_host: str) -> bool:
    return first_host == second_host or "0.0.0.0" in {first_host, second_host}


def _register_port_binding(
    bindings: list[tuple[str, str, int, str]],
    host: str,
    protocol: str,
    port: int,
    path: str,
    issues: list[ConfigIssue],
) -> None:
    for existing_host, existing_protocol, existing_port, existing_path in bindings:
        if protocol == existing_protocol and port == existing_port and _bindings_conflict(host, existing_host):
            issues.append(
                _issue(
                    "PORT_COLLISION",
                    path,
                    f"endpoint conflicts with {existing_path}",
                    port=port,
                    conflicting_entry=existing_path,
                )
            )
            return
    bindings.append((host, protocol, port, path))


def _valid_ipc_endpoint(value: str) -> bool:
    if not value.startswith("ipc:///"):
        return False
    path = value.removeprefix("ipc://")
    if path == "/" or path.endswith("/") or "\\" in path:
        return False
    return all(part not in {"", ".", ".."} for part in path.split("/")[1:])


def _validate_camera_path(camera_id: str, path: Path, kind: CameraPathKind, issues: list[ConfigIssue]) -> None:
    value = path.as_posix()
    field_path = f"cameras.{camera_id}.device_path"
    if _VIDEO_DEVICE_RE.fullmatch(value):
        issues.append(_issue("CAMERA_PATH_ENUMERATION_DEPENDENT", field_path, "must not use /dev/videoN", value=value))
    if kind is CameraPathKind.BY_ID:
        if not value.startswith("/dev/v4l/by-id/"):
            issues.append(
                _issue(
                    "CAMERA_PATH_KIND_MISMATCH", field_path, "by_id paths must be under /dev/v4l/by-id/", value=value
                )
            )
        if value.startswith("/dev/purdue-rov-cv/"):
            issues.append(
                _issue(
                    "CAMERA_PATH_KIND_MISMATCH", field_path, "by_id paths must not use fallback directory", value=value
                )
            )
    elif kind is CameraPathKind.FALLBACK:
        if not value.startswith("/dev/purdue-rov-cv/"):
            issues.append(
                _issue(
                    "CAMERA_PATH_KIND_MISMATCH",
                    field_path,
                    "fallback paths must be under /dev/purdue-rov-cv/",
                    value=value,
                )
            )
        if value.startswith("/dev/v4l/by-id/"):
            issues.append(
                _issue(
                    "CAMERA_PATH_KIND_MISMATCH", field_path, "fallback paths must not use by-id directory", value=value
                )
            )
        if path.name != camera_id:
            issues.append(
                _issue(
                    "CAMERA_FALLBACK_NAME_MISMATCH",
                    field_path,
                    "fallback filename must equal camera ID",
                    expected=camera_id,
                    value=path.name,
                )
            )


def validate_static_config(config: AppConfig) -> tuple[ConfigIssue, ...]:
    """Return deterministic semantic errors without reading hardware or network state."""
    issues: list[ConfigIssue] = []
    if config.schema_version != SUPPORTED_SCHEMA_VERSION:
        issues.append(
            _issue(
                "UNSUPPORTED_SCHEMA_VERSION",
                "schema_version",
                f"only version {SUPPORTED_SCHEMA_VERSION} is supported",
                value=config.schema_version,
            )
        )

    for section_name, mapping in (("cameras", config.cameras), ("tasks", config.tasks)):
        for identifier in mapping:
            if not IDENTIFIER_PATTERN.fullmatch(identifier):
                issues.append(
                    _issue(
                        "IDENTIFIER_INVALID",
                        f"{section_name}.{identifier}",
                        "mapping key is not a valid identifier",
                        value=identifier,
                    )
                )
    if not config.cameras:
        issues.append(_issue("CAMERAS_EMPTY", "cameras", "at least one camera must be configured"))
    if len(config.cameras) > config.camera_limits.maximum_configured:
        issues.append(
            _issue(
                "CAMERA_LIMIT_EXCEEDED",
                "cameras",
                "configured camera count exceeds maximum_configured",
                count=len(config.cameras),
                limit=config.camera_limits.maximum_configured,
            )
        )
    if config.camera_limits.maximum_active > config.camera_limits.maximum_configured:
        issues.append(
            _issue(
                "CAMERA_LIMIT_INVALID",
                "camera_limits.maximum_active",
                "maximum_active must not exceed maximum_configured",
                maximum_active=config.camera_limits.maximum_active,
                maximum_configured=config.camera_limits.maximum_configured,
            )
        )
    active_cameras = [
        camera_id for camera_id, camera in config.cameras.items() if camera.cv_enabled or camera.stream_to_surface
    ]
    if len(active_cameras) > config.camera_limits.maximum_active:
        issues.append(
            _issue(
                "ACTIVE_CAMERA_LIMIT_EXCEEDED",
                "cameras",
                "active camera count exceeds maximum_active",
                cameras=active_cameras,
                limit=config.camera_limits.maximum_active,
            )
        )

    allocations_by_index: dict[int, str] = {}
    bound_ports: list[tuple[str, str, int, str]] = []
    for camera_id, camera in sorted(config.cameras.items()):
        _validate_camera_path(camera_id, camera.device_path, camera.device_path_kind, issues)
        allocation = derive_stream_allocation(camera_id, camera.stream_index)
        if camera.stream_index in allocations_by_index:
            issues.append(
                _issue(
                    "STREAM_INDEX_DUPLICATE",
                    f"cameras.{camera_id}.stream_index",
                    "stream_index is already claimed",
                    conflicting_camera=allocations_by_index[camera.stream_index],
                    value=camera.stream_index,
                )
            )
        else:
            allocations_by_index[camera.stream_index] = camera_id
        if not RTP_PAYLOAD_TYPE_MIN <= allocation.rtp_payload_type <= RTP_PAYLOAD_TYPE_MAX:
            issues.append(
                _issue(
                    "RTP_PAYLOAD_TYPE_OUT_OF_RANGE",
                    f"cameras.{camera_id}.stream_index",
                    "derived RTP payload type is outside dynamic range 96..127",
                    value=allocation.rtp_payload_type,
                )
            )
        for port, label in ((allocation.rtp_port, "rtp"), (allocation.rtcp_port, "rtcp")):
            if not 1 <= port <= 65_535:
                issues.append(
                    _issue(
                        "PORT_OUT_OF_RANGE",
                        f"cameras.{camera_id}.stream_index",
                        f"derived {label} port is outside 1..65535",
                        value=port,
                    )
                )
                continue
            _register_port_binding(
                bound_ports,
                str(config.network.rov_ip),
                "udp",
                port,
                f"cameras.{camera_id}.stream_index",
                issues,
            )

    endpoints = (
        ("messaging.broker.publisher_endpoint", config.messaging.broker.publisher_endpoint),
        ("messaging.broker.subscriber_endpoint", config.messaging.broker.subscriber_endpoint),
        ("messaging.control.client_endpoint", config.messaging.control.client_endpoint),
    )
    for path, endpoint in endpoints:
        parsed = _parse_tcp_endpoint(endpoint, path, issues)
        if parsed is None:
            continue
        host, port = parsed
        _register_port_binding(bound_ports, host, "tcp", port, path, issues)
    if not _valid_ipc_endpoint(config.messaging.control.module_endpoint):
        issues.append(
            _issue(
                "ENDPOINT_INVALID",
                "messaging.control.module_endpoint",
                "must be an absolute ipc:/// path",
                value=config.messaging.control.module_endpoint,
            )
        )

    for task_id, task in sorted(config.tasks.items()):
        path = f"tasks.{task_id}"
        task_camera = config.cameras.get(task.input_camera)
        if task_camera is None:
            issues.append(
                _issue(
                    "TASK_CAMERA_MISSING",
                    f"{path}.input_camera",
                    "task references an unknown camera",
                    value=task.input_camera,
                )
            )
        elif task.enabled and not task_camera.cv_enabled:
            issues.append(
                _issue(
                    "TASK_CAMERA_INCOMPATIBLE",
                    f"{path}.input_camera",
                    "enabled task requires a CV-enabled camera",
                    value=task.input_camera,
                )
            )
        if task.execution_target != config.device.execution_target:
            issues.append(
                _issue(
                    "EXECUTION_TARGET_UNKNOWN",
                    f"{path}.execution_target",
                    "must match this host configuration's execution_target",
                    value=task.execution_target,
                    expected=config.device.execution_target,
                )
            )
        topic = validate_topic(task.publish_topic)
        if not topic.valid or topic.kind is not TopicKind.CV_RESULT:
            issues.append(
                _issue(
                    "TASK_TOPIC_INVALID",
                    f"{path}.publish_topic",
                    "must be a cv.result.<task_id>.<camera_id> topic",
                    value=task.publish_topic,
                )
            )
        elif topic.identifiers["task_id"] != task_id or topic.identifiers["camera_id"] != task.input_camera:
            issues.append(
                _issue(
                    "TASK_TOPIC_MISMATCH",
                    f"{path}.publish_topic",
                    "topic task_id and camera_id must match task configuration",
                    value=task.publish_topic,
                )
            )
        payload = PAYLOAD_REGISTRY.get(task.payload_type)
        if payload is None or payload.message_type != 1:
            issues.append(
                _issue(
                    "TASK_PAYLOAD_INVALID",
                    f"{path}.payload_type",
                    "must identify a registered CV result payload",
                    value=task.payload_type,
                )
            )
        if str(task.artifact.runtime) not in _RUNTIME_COMPATIBILITY[task.artifact.format]:
            issues.append(
                _issue(
                    "ARTIFACT_RUNTIME_INCOMPATIBLE",
                    f"{path}.artifact.runtime",
                    "runtime is incompatible with artifact format",
                    artifact_format=str(task.artifact.format),
                    runtime=str(task.artifact.runtime),
                )
            )
    return tuple(sorted(issues, key=lambda issue: (issue.path, issue.code, issue.message)))
