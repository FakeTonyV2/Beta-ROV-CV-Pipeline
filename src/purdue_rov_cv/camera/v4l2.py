"""Stable V4L2 identity resolution and exact capability probing."""

from __future__ import annotations

import re
import stat
import subprocess
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from fractions import Fraction
from pathlib import Path

from purdue_rov_cv.config.models import (
    CameraConfig,
    CameraFormat,
    CameraPathKind,
    CameraResolutionTier,
)

FOURCC_TO_FORMAT: dict[str, CameraFormat] = {
    "H264": CameraFormat.H264,
    "MJPG": CameraFormat.MJPEG,
    "JPEG": CameraFormat.MJPEG,
    "YUYV": CameraFormat.YUYV,
    "YUY2": CameraFormat.YUYV,
    "NV12": CameraFormat.NV12,
}
FORMAT_TO_GSTREAMER: dict[CameraFormat, tuple[str, str | None]] = {
    CameraFormat.H264: ("video/x-h264", None),
    CameraFormat.MJPEG: ("image/jpeg", None),
    CameraFormat.YUYV: ("video/x-raw", "YUY2"),
    CameraFormat.NV12: ("video/x-raw", "NV12"),
}

_FORMAT_LINE = re.compile(r"^\s*\[\d+\]:\s+'(?P<fourcc>[^']+)'")
_SIZE_LINE = re.compile(r"^\s*Size:\s+Discrete\s+(?P<width>\d+)x(?P<height>\d+)")
_FPS_PAREN = re.compile(r"\((?P<fps>\d+(?:\.\d+)?)\s+fps\)")
_INTERVAL_FRACTION = re.compile(r"Interval:\s+Discrete\s+(?P<num>\d+)\s*/\s*(?P<den>\d+)")


class V4L2ProbeError(RuntimeError):
    """A deployment or tool error while validating a physical camera."""


class V4L2Disconnected(V4L2ProbeError):
    """The configured stable identity is currently absent."""


class V4L2ConfigurationError(V4L2ProbeError):
    """The configured identity or capture tuple is incompatible (exit 78)."""


class V4L2DeviceInvalid(V4L2ConfigurationError):
    """A configured path exists but is not a usable V4L2 capture device."""


class V4L2IdentityMismatch(V4L2ConfigurationError):
    """The resolved device does not match the configured stable identity."""


class V4L2ModeUnsupported(V4L2ConfigurationError):
    """The exact configured V4L2 tuple is not advertised by the device."""


class H264ProfileStatus(StrEnum):
    VERIFIED = "VERIFIED"
    UNVALIDATED = "UNVALIDATED"
    FAILED = "FAILED"


@dataclass(frozen=True, slots=True, order=True)
class V4L2Mode:
    pixel_format: CameraFormat
    width: int
    height: int
    frame_rate: Fraction

    def describe(self) -> str:
        fps = str(self.frame_rate.numerator)
        if self.frame_rate.denominator != 1:
            fps += f"/{self.frame_rate.denominator}"
        return f"{self.pixel_format.value} {self.width}x{self.height}@{fps}"


@dataclass(frozen=True, slots=True)
class ResolvedV4L2Device:
    camera_id: str
    configured_path: Path
    resolved_path: Path
    path_kind: CameraPathKind
    resolution_tier: CameraResolutionTier
    stable_identity: str
    properties: dict[str, str]


@dataclass(frozen=True, slots=True)
class V4L2ProbeReport:
    device: ResolvedV4L2Device
    modes: tuple[V4L2Mode, ...]
    configured_mode: V4L2Mode
    mode_opened: bool
    open_detail: str = ""


def _run(command: Sequence[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, check=False, capture_output=True, text=True, timeout=8)


def _is_video_character_device(path: Path) -> bool:
    try:
        return stat.S_ISCHR(path.stat().st_mode) and re.fullmatch(r"video\d+", path.name) is not None
    except OSError:
        return False


def _fps_fraction(text: str) -> Fraction | None:
    try:
        return Fraction(Decimal(text))
    except (InvalidOperation, ValueError, ZeroDivisionError):
        return None


def parse_v4l2_modes(listing: str) -> tuple[V4L2Mode, ...]:
    """Parse discrete advertised tuples without combining independent fields.

    Decimal FPS text is converted to an exact rational. Thus 30.000 equals
    configured 30, while 29.970 remains 2997/100 and is not silently rounded.
    """

    current_format: CameraFormat | None = None
    current_size: tuple[int, int] | None = None
    modes: set[V4L2Mode] = set()
    for line in listing.splitlines():
        if match := _FORMAT_LINE.match(line):
            current_format = FOURCC_TO_FORMAT.get(match["fourcc"].upper())
            current_size = None
            continue
        if match := _SIZE_LINE.match(line):
            current_size = (int(match["width"]), int(match["height"]))
            continue
        if current_format is None or current_size is None or "Interval:" not in line:
            continue
        fps: Fraction | None = None
        if match := _FPS_PAREN.search(line):
            fps = _fps_fraction(match["fps"])
        elif match := _INTERVAL_FRACTION.search(line):
            numerator = int(match["num"])
            denominator = int(match["den"])
            if numerator:
                fps = Fraction(denominator, numerator)
        if fps is not None and fps > 0:
            modes.add(V4L2Mode(current_format, current_size[0], current_size[1], fps))
    return tuple(sorted(modes))


def configured_v4l2_mode(camera: CameraConfig) -> V4L2Mode:
    return V4L2Mode(camera.format, camera.width, camera.height, Fraction(camera.frame_rate, 1))


def exact_mode_supported(camera: CameraConfig, modes: Sequence[V4L2Mode]) -> bool:
    return configured_v4l2_mode(camera) in modes


def nearby_modes(camera: CameraConfig, modes: Sequence[V4L2Mode], limit: int = 6) -> tuple[V4L2Mode, ...]:
    ranked = sorted(
        modes,
        key=lambda mode: (
            mode.pixel_format != camera.format,
            abs(mode.width - camera.width) + abs(mode.height - camera.height),
            abs(mode.frame_rate - camera.frame_rate),
        ),
    )
    return tuple(ranked[:limit])


def exact_caps(camera: CameraConfig) -> str:
    media_type, raw_format = FORMAT_TO_GSTREAMER[camera.format]
    fields = [media_type]
    if raw_format is not None:
        fields.append(f"format={raw_format}")
    fields.extend(
        (
            f"width={camera.width}",
            f"height={camera.height}",
            f"framerate={camera.frame_rate}/1",
        )
    )
    return ",".join(fields)


class V4L2DeviceProbe:
    """Resolve a configured stable link and validate one exact capture mode."""

    def __init__(
        self,
        *,
        command_runner: Callable[[Sequence[str]], subprocess.CompletedProcess[str]] = _run,
        video_device_check: Callable[[Path], bool] = _is_video_character_device,
        symlink_check: Callable[[Path], bool] = lambda path: path.is_symlink(),
        v4l2_ctl: str = "v4l2-ctl",
        gst_launch: str = "gst-launch-1.0",
    ) -> None:
        self.command_runner = command_runner
        self.video_device_check = video_device_check
        self.symlink_check = symlink_check
        self.v4l2_ctl = v4l2_ctl
        self.gst_launch = gst_launch

    def _command(self, command: Sequence[str], label: str) -> subprocess.CompletedProcess[str]:
        try:
            result = self.command_runner(command)
        except FileNotFoundError as error:
            raise V4L2ConfigurationError(f"{command[0]} is not installed") from error
        except PermissionError as error:
            raise V4L2ConfigurationError(f"{label} is not permitted: {error}") from error
        except (OSError, subprocess.TimeoutExpired) as error:
            raise V4L2ProbeError(f"{label} failed: {error}") from error
        if result.returncode != 0:
            detail = result.stderr.strip() or result.stdout.strip() or f"exit status {result.returncode}"
            normalized = detail.casefold()
            if any(
                marker in normalized
                for marker in (
                    "no such file or directory",
                    "no such device",
                    "cannot open device",
                    "device or resource busy",
                )
            ):
                raise V4L2Disconnected(f"{label} failed: {detail}")
            if "permission denied" in normalized or "operation not permitted" in normalized:
                raise V4L2ConfigurationError(f"{label} is not permitted: {detail}")
            raise V4L2ProbeError(f"{label} failed: {detail}")
        return result

    def _properties(self, resolved: Path) -> dict[str, str]:
        result = self._command(
            ("udevadm", "info", "--query=property", "--name", str(resolved)),
            "udev property query",
        )
        return {
            key: value for line in result.stdout.splitlines() if "=" in line for key, value in (line.split("=", 1),)
        }

    def resolve(self, camera_id: str, camera: CameraConfig) -> ResolvedV4L2Device:
        if camera.device_path is None or camera.device_path_kind is None or camera.resolution_tier is None:
            raise V4L2ConfigurationError("gstreamer_v4l2 identity fields are incomplete")
        path = camera.device_path
        if not path.exists():
            raise V4L2Disconnected(f"configured stable camera path is absent: {path}")
        if not self.symlink_check(path):
            raise V4L2DeviceInvalid(f"configured stable camera path is not a symlink: {path}")
        try:
            resolved = path.resolve(strict=True)
        except (OSError, RuntimeError) as error:
            raise V4L2Disconnected(f"configured camera path cannot resolve: {path}: {error}") from error
        if not self.video_device_check(resolved):
            raise V4L2DeviceInvalid(f"{path} resolves to {resolved}, not a V4L2 video character device")

        properties = self._properties(resolved)
        capabilities = properties.get("ID_V4L_CAPABILITIES", "")
        if ":capture:" not in capabilities:
            raise V4L2DeviceInvalid(f"{path} resolves to {resolved}, but udev does not identify it as capture-capable")
        if camera.device_path_kind is CameraPathKind.BY_ID:
            tier = CameraResolutionTier.BY_ID
            devlinks = set(properties.get("DEVLINKS", "").split())
            if str(path) not in devlinks:
                raise V4L2IdentityMismatch(
                    f"by-id target mismatch: {resolved} does not advertise configured devlink {path}"
                )
            identity = properties.get("ID_SERIAL") or path.name
        else:
            camera_marker = properties.get("PURDUE_ROV_CV_CAMERA_ID")
            tier_text = properties.get("PURDUE_ROV_CV_TIER")
            identity = properties.get("PURDUE_ROV_CV_IDENTITY", "")
            try:
                tier = CameraResolutionTier(tier_text or "")
            except ValueError as error:
                raise V4L2IdentityMismatch("fallback device lacks valid Purdue ROV provisioning metadata") from error
            if camera_marker != camera_id:
                raise V4L2IdentityMismatch(
                    f"fallback identity mismatch: configured camera {camera_id!r}, provisioned {camera_marker!r}"
                )
            if identity != camera.stable_identity:
                raise V4L2IdentityMismatch(
                    f"fallback stable identity mismatch: configured {camera.stable_identity!r}, actual {identity!r}"
                )
            if tier is CameraResolutionTier.PHYSICAL_PORT:
                port_label = properties.get("PURDUE_ROV_CV_PORT_LABEL")
                if port_label != camera.physical_port_label:
                    raise V4L2IdentityMismatch(
                        f"physical-port label mismatch: configured {camera.physical_port_label!r}, "
                        f"actual {port_label!r}"
                    )
        if tier is not camera.resolution_tier:
            raise V4L2IdentityMismatch(
                f"resolution tier mismatch: configured {camera.resolution_tier.value}, actual {tier.value}"
            )
        return ResolvedV4L2Device(
            camera_id,
            path,
            resolved,
            camera.device_path_kind,
            tier,
            identity,
            properties,
        )

    def enumerate_modes(self, device: ResolvedV4L2Device) -> tuple[V4L2Mode, ...]:
        result = self._command(
            (self.v4l2_ctl, "--device", str(device.resolved_path), "--list-formats-ext"),
            "v4l2-ctl capability enumeration",
        )
        if not re.search(r"^\s*Type:\s+Video Capture(?: Multiplanar)?\s*$", result.stdout, re.MULTILINE):
            raise V4L2DeviceInvalid(f"{device.resolved_path} does not advertise V4L2 capture capability")
        modes = parse_v4l2_modes(result.stdout)
        if not modes:
            raise V4L2DeviceInvalid(f"{device.resolved_path} did not advertise discrete Video Capture modes")
        return modes

    def _mode_open_command(self, device: ResolvedV4L2Device, camera: CameraConfig) -> tuple[str, ...]:
        command = [
            self.gst_launch,
            "-q",
            "v4l2src",
            f"device={device.resolved_path}",
            "num-buffers=1",
            "!",
            exact_caps(camera),
        ]
        if camera.format is CameraFormat.H264:
            command.extend(("!", "h264parse"))
        command.extend(
            (
                "!",
                "queue",
                "max-size-buffers=1",
                "max-size-bytes=0",
                "max-size-time=0",
                "leaky=downstream",
            )
        )
        if camera.format is CameraFormat.H264:
            command.extend(("!", "avdec_h264"))
        elif camera.format is CameraFormat.MJPEG:
            command.extend(("!", "jpegdec"))
        command.extend(
            (
                "!",
                "videoconvert",
                "!",
                "video/x-raw,format=BGR",
                "!",
                "fakesink",
                "sync=false",
                "async=false",
            )
        )
        return tuple(command)

    def validate(self, camera_id: str, camera: CameraConfig, *, open_mode: bool) -> V4L2ProbeReport:
        device = self.resolve(camera_id, camera)
        modes = self.enumerate_modes(device)
        configured = configured_v4l2_mode(camera)
        if configured not in modes:
            nearby = ", ".join(mode.describe() for mode in nearby_modes(camera, modes)) or "none"
            raise V4L2ModeUnsupported(
                f"CAMERA_MODE_UNSUPPORTED configured={configured.describe()} path={device.configured_path} "
                f"resolved={device.resolved_path} nearby=[{nearby}]"
            )
        if not open_mode:
            return V4L2ProbeReport(device, modes, configured, False, "mode open was not requested")
        command = self._mode_open_command(device, camera)
        try:
            result = self._command(command, "configured GStreamer mode-open probe")
        except V4L2ConfigurationError:
            raise
        except V4L2ProbeError as error:
            return V4L2ProbeReport(device, modes, configured, False, str(error))
        return V4L2ProbeReport(device, modes, configured, result.returncode == 0, "production decode path opened")


__all__ = [
    "FORMAT_TO_GSTREAMER",
    "FOURCC_TO_FORMAT",
    "H264ProfileStatus",
    "ResolvedV4L2Device",
    "V4L2ConfigurationError",
    "V4L2DeviceInvalid",
    "V4L2DeviceProbe",
    "V4L2Disconnected",
    "V4L2IdentityMismatch",
    "V4L2Mode",
    "V4L2ModeUnsupported",
    "V4L2ProbeError",
    "V4L2ProbeReport",
    "configured_v4l2_mode",
    "exact_caps",
    "exact_mode_supported",
    "nearby_modes",
    "parse_v4l2_modes",
]
