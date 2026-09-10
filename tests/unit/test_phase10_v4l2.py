"""Phase 10 stable identity, exact mode, pipeline, and provisioning tests."""

from __future__ import annotations

import subprocess
from fractions import Fraction
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

import purdue_rov_cv.camera.entrypoints as camera_entrypoints
import purdue_rov_cv.video.entrypoints as video_entrypoints
from purdue_rov_cv.camera.backend import (
    PRODUCTION_RTP_MTU,
    CaptureBackendError,
    GStreamerCaptureBackend,
    SurfaceRtpStream,
    V4L2CaptureBackend,
)
from purdue_rov_cv.camera.v4l2 import (
    H264ProfileStatus,
    ResolvedV4L2Device,
    V4L2ConfigurationError,
    V4L2DeviceInvalid,
    V4L2DeviceProbe,
    V4L2Disconnected,
    V4L2IdentityMismatch,
    V4L2Mode,
    V4L2ModeUnsupported,
    exact_mode_supported,
    parse_v4l2_modes,
)
from purdue_rov_cv.config import load_config, parse_config_data
from purdue_rov_cv.config.issues import ConfigStaticValidationError
from purdue_rov_cv.config.models import CameraFormat, CameraPathKind, CameraResolutionTier
from scripts.configure_udev import Candidate, build_assignments, render_rules

ROOT = Path(__file__).parents[2]

LISTING = """\
ioctl: VIDIOC_ENUM_FMT
 Type: Video Capture
 [0]: 'H264' (H.264)
   Size: Discrete 1920x1080
     Interval: Discrete 0.033s (30.000 fps)
     Interval: Discrete 0.033s (29.970 fps)
 [1]: 'MJPG' (Motion-JPEG)
   Size: Discrete 1280x720
     Interval: Discrete 1/60
 [2]: 'YUYV' (YUYV 4:2:2)
   Size: Discrete 640x480
     Interval: Discrete 0.067s (15.000 fps)
"""


def _config():
    return load_config(ROOT / "config" / "mission.yaml", environ={})


def _camera(**changes):
    return _config().cameras["front_camera"].model_copy(update=changes)


def _completed(command, stdout: str = "", returncode: int = 0, stderr: str = ""):
    return subprocess.CompletedProcess(command, returncode, stdout, stderr)


def test_parser_returns_exact_tuples_and_normalizes_fourcc_and_rational_fps() -> None:
    modes = parse_v4l2_modes(LISTING)
    assert V4L2Mode(CameraFormat.H264, 1920, 1080, Fraction(30, 1)) in modes
    assert V4L2Mode(CameraFormat.H264, 1920, 1080, Fraction(2997, 100)) in modes
    assert V4L2Mode(CameraFormat.MJPEG, 1280, 720, Fraction(60, 1)) in modes
    assert V4L2Mode(CameraFormat.YUYV, 640, 480, Fraction(15, 1)) in modes
    assert parse_v4l2_modes("malformed\nSize: Discrete no") == ()


def test_tuple_match_never_combines_independent_fields() -> None:
    listing = """\
[0]: 'H264'
 Size: Discrete 1920x720
  Interval: Discrete 0.033s (30.000 fps)
 Size: Discrete 1280x1080
  Interval: Discrete 0.017s (60.000 fps)
"""
    assert not exact_mode_supported(_camera(width=1920, height=1080, frame_rate=60), parse_v4l2_modes(listing))


@pytest.mark.parametrize(
    "changes",
    [
        {"format": CameraFormat.MJPEG},
        {"width": 1280},
        {"height": 720},
        {"frame_rate": 60},
    ],
)
def test_exact_mode_validation_never_substitutes_a_nearby_field(changes) -> None:
    modes = (V4L2Mode(CameraFormat.H264, 1920, 1080, Fraction(30, 1)),)
    assert not exact_mode_supported(_camera(**changes), modes)


def test_by_id_resolution_missing_and_wrong_target(tmp_path: Path) -> None:
    link = tmp_path / "by-id-camera"
    target = tmp_path / "video4"
    target.write_bytes(b"device-double")
    link.symlink_to(target)
    camera = _camera(device_path=link)

    def runner(command):
        return _completed(
            command,
            f"DEVLINKS={link}\nID_SERIAL=hardware-serial\nID_V4L_CAPABILITIES=:capture:\n",
        )

    probe = V4L2DeviceProbe(command_runner=runner, video_device_check=lambda path: path == target)
    resolved = probe.resolve("front_camera", camera)
    assert resolved.resolved_path == target
    assert resolved.resolution_tier.value == "by_id"

    missing = _camera(device_path=tmp_path / "absent")
    with pytest.raises(V4L2Disconnected):
        probe.resolve("front_camera", missing)
    with pytest.raises(V4L2ConfigurationError, match="not a V4L2"):
        V4L2DeviceProbe(command_runner=runner, video_device_check=lambda path: False).resolve("front_camera", camera)

    def no_capture_runner(command):
        return _completed(command, f"DEVLINKS={link}\nID_SERIAL=hardware-serial\n")

    with pytest.raises(V4L2DeviceInvalid, match="capture-capable"):
        V4L2DeviceProbe(
            command_runner=no_capture_runner,
            video_device_check=lambda path: path == target,
        ).resolve("front_camera", camera)


def test_by_id_resolution_rejects_an_altered_symlink_target(tmp_path: Path) -> None:
    link = tmp_path / "configured-camera"
    target = tmp_path / "video9"
    target.write_bytes(b"device-double")
    link.symlink_to(target)
    camera = _camera(device_path=link)

    def runner(command):
        return _completed(
            command,
            "DEVLINKS=/dev/v4l/by-id/usb-different-camera-video-index0\n"
            "ID_SERIAL=different-camera\nID_V4L_CAPABILITIES=:capture:\n",
        )

    with pytest.raises(V4L2IdentityMismatch, match="by-id target mismatch"):
        V4L2DeviceProbe(command_runner=runner, video_device_check=lambda path: path == target).resolve(
            "front_camera", camera
        )


def test_fallback_metadata_detects_tier_and_wrong_device_substitution(tmp_path: Path) -> None:
    target = tmp_path / "video7"
    target.write_bytes(b"device-double")
    link = tmp_path / "front_camera"
    link.symlink_to(target)
    camera = (
        _config()
        .cameras["front_camera"]
        .model_copy(
            update={
                "device_path": link,
                "device_path_kind": CameraPathKind.FALLBACK,
                "resolution_tier": CameraResolutionTier.ID_PATH,
                "stable_identity": "usb-path-a",
            }
        )
    )

    def runner(command):
        return _completed(
            command,
            "ID_V4L_CAPABILITIES=:capture:\n"
            "PURDUE_ROV_CV_CAMERA_ID=front_camera\n"
            "PURDUE_ROV_CV_TIER=id_path\n"
            "PURDUE_ROV_CV_IDENTITY=usb-path-a\n",
        )

    probe = V4L2DeviceProbe(command_runner=runner, video_device_check=lambda path: True)
    assert probe.resolve("front_camera", camera).stable_identity == "usb-path-a"

    def wrong_runner(command):
        return _completed(
            command,
            "ID_V4L_CAPABILITIES=:capture:\n"
            "PURDUE_ROV_CV_CAMERA_ID=other_camera\n"
            "PURDUE_ROV_CV_TIER=physical_port\n"
            "PURDUE_ROV_CV_IDENTITY=usb-path-b\n",
        )

    with pytest.raises(V4L2ConfigurationError, match="identity mismatch"):
        V4L2DeviceProbe(command_runner=wrong_runner, video_device_check=lambda path: True).resolve(
            "front_camera", camera
        )


def test_mode_open_probe_uses_the_production_decode_chain(tmp_path: Path) -> None:
    target = tmp_path / "video4"
    target.write_bytes(b"device-double")
    link = tmp_path / "camera-link"
    link.symlink_to(target)
    commands: list[tuple[str, ...]] = []

    def runner(command):
        command = tuple(command)
        commands.append(command)
        if command[0] == "udevadm":
            return _completed(
                command,
                f"DEVLINKS={link}\nID_SERIAL=hardware-serial\nID_V4L_CAPABILITIES=:capture:\n",
            )
        if command[0] == "v4l2-ctl":
            return _completed(command, LISTING)
        return _completed(command)

    camera = _camera(device_path=link, format=CameraFormat.H264)
    report = V4L2DeviceProbe(command_runner=runner, video_device_check=lambda path: path == target).validate(
        "front_camera", camera, open_mode=True
    )
    assert report.mode_opened
    gst_command = next(command for command in commands if command[0] == "gst-launch-1.0")
    assert "h264parse" in gst_command
    assert "avdec_h264" in gst_command
    assert "videoconvert" in gst_command
    assert "video/x-raw,format=BGR" in gst_command
    assert "max-size-buffers=1" in gst_command


def test_missing_v4l2_tool_is_a_deployment_configuration_error(tmp_path: Path) -> None:
    target = tmp_path / "video4"
    target.write_bytes(b"device-double")
    link = tmp_path / "camera-link"
    link.symlink_to(target)

    def runner(_command):
        raise FileNotFoundError("missing")

    with pytest.raises(V4L2ConfigurationError, match="udevadm is not installed"):
        V4L2DeviceProbe(command_runner=runner, video_device_check=lambda _path: True).resolve(
            "front_camera", _camera(device_path=link)
        )


def test_raw_surface_requires_per_camera_opt_in() -> None:
    data = yaml.safe_load((ROOT / "config" / "mission.yaml").read_text(encoding="utf-8"))
    data["cameras"]["front_camera"].update({"format": "yuyv", "allow_software_encode": False})
    with pytest.raises(ConfigStaticValidationError, match="CAMERA_RAW_SURFACE_REQUIRES_SOFTWARE_ENCODE_OPT_IN"):
        parse_config_data(data)
    data["cameras"]["front_camera"].update({"stream_to_surface": False, "cv_enabled": True})
    assert parse_config_data(data).cameras["front_camera"].format is CameraFormat.YUYV
    data["cameras"]["front_camera"].update({"stream_to_surface": True, "allow_software_encode": True})
    assert parse_config_data(data).cameras["front_camera"].allow_software_encode


def _surface() -> SurfaceRtpStream:
    return SurfaceRtpStream("front_camera", b"0" * 16, "192.168.50.1", 5000, 96, 123, lambda value: None)


def test_physical_stream_rejects_noncanonical_mtu_and_invalid_ssrc() -> None:
    with pytest.raises(ValueError, match="MTU"):
        SurfaceRtpStream("front_camera", b"0" * 16, "192.168.50.1", 5000, 96, 123, lambda value: None, mtu=1400)
    with pytest.raises(ValueError, match="SSRC"):
        SurfaceRtpStream("front_camera", b"0" * 16, "192.168.50.1", 5000, 96, -1, lambda value: None)
    assert _surface().mtu == PRODUCTION_RTP_MTU


@pytest.mark.parametrize(
    ("camera", "required"),
    [
        (
            _camera(format=CameraFormat.H264),
            ("video/x-h264", "h264parse", "rtph264pay", "config-interval=1", "avdec_h264"),
        ),
        (
            _camera(format=CameraFormat.MJPEG),
            ("image/jpeg", "rtpjpegpay", "jpegdec"),
        ),
        (
            _camera(format=CameraFormat.YUYV, allow_software_encode=True),
            ("video/x-raw,format=YUY2", "x264enc", "rtph264pay"),
        ),
    ],
)
def test_physical_pipeline_profiles_are_exact_and_bounded(camera, required) -> None:
    description = V4L2CaptureBackend("front_camera", camera, surface_stream=_surface()).pipeline_description()
    assert f"width={camera.width},height={camera.height},framerate={camera.frame_rate}/1" in description
    assert "max-size-buffers=2 max-size-bytes=0 max-size-time=0 leaky=downstream" in description
    assert "max-size-buffers=1 max-size-bytes=0 max-size-time=0 leaky=downstream" in description
    assert "pt=96" in description and "mtu=1200" in description
    assert "max-buffers=1 drop=true sync=false" in description
    assert "host=192.168.50.1" in description
    assert all(item in description for item in required)


class _CapsStructure:
    def __init__(self, profile: str | None) -> None:
        self.values = {
            "width": 1920,
            "height": 1080,
            "framerate": SimpleNamespace(num=30, denom=1),
            "profile": profile,
        }
        self.media_type = "video/x-h264"

    def get_value(self, name: str):
        return self.values[name]

    def get_name(self) -> str:
        return self.media_type


class _Caps:
    def __init__(self, profile: str | None) -> None:
        self.structure = _CapsStructure(profile)

    def get_size(self) -> int:
        return 1

    def get_structure(self, _index: int) -> _CapsStructure:
        return self.structure


class _Source:
    def __init__(self, profile: str | None) -> None:
        self.caps = _Caps(profile)

    def get_static_pad(self, _name: str):
        return self

    def get_current_caps(self) -> _Caps:
        return self.caps


class _Pipeline:
    def __init__(self, profile: str | None) -> None:
        self.source = _Source(profile)

    def get_by_name(self, _name: str) -> _Source:
        return self.source


def test_direct_h264_profile_remains_unvalidated_without_complete_stream_evidence() -> None:
    backend = V4L2CaptureBackend("front_camera", _camera(format=CameraFormat.H264))
    backend._pipeline = _Pipeline("baseline")  # type: ignore[assignment]
    backend._verify_negotiated_mode()
    assert backend.h264_profile_status is H264ProfileStatus.UNVALIDATED
    assert "B-frames, keyframe interval, and bitrate were not verified" in backend.h264_profile_detail

    backend._pipeline = _Pipeline("unsupported")  # type: ignore[assignment]
    with pytest.raises(CaptureBackendError, match="unsupported negotiated profile"):
        backend._verify_negotiated_mode()
    assert backend.h264_profile_status is H264ProfileStatus.FAILED


def test_negotiated_caps_require_explicit_frame_rate_and_exact_raw_format() -> None:
    backend = V4L2CaptureBackend("front_camera", _camera(format=CameraFormat.H264))
    pipeline = _Pipeline("baseline")
    pipeline.source.caps.structure.values["framerate"] = None
    backend._pipeline = pipeline  # type: ignore[assignment]
    with pytest.raises(CaptureBackendError, match="without an exact frame rate"):
        backend._verify_negotiated_mode()

    raw = V4L2CaptureBackend(
        "front_camera",
        _camera(
            format=CameraFormat.YUYV,
            stream_to_surface=False,
            allow_software_encode=False,
        ),
    )
    pipeline = _Pipeline(None)
    pipeline.source.caps.structure.media_type = "video/x-raw"
    pipeline.source.caps.structure.values["format"] = "NV12"
    raw._pipeline = pipeline  # type: ignore[assignment]
    with pytest.raises(CaptureBackendError, match="expected 'YUY2'"):
        raw._verify_negotiated_mode()


class _Element:
    def __init__(self, **properties) -> None:
        self.properties = properties

    def get_property(self, name: str):
        return self.properties[name]


def _instantiated_pipeline(backend: V4L2CaptureBackend, *, ssrc: int = 123):
    assert backend.camera.device_path is not None
    backend.resolved_device = ResolvedV4L2Device(
        "front_camera",
        backend.camera.device_path,
        Path("/dev/video4"),
        CameraPathKind.BY_ID,
        CameraResolutionTier.BY_ID,
        "camera-serial",
        {},
    )
    elements = {
        "device_source": _Element(device="/dev/video4", **{"do-timestamp": True}),
        "cv_queue": _Element(**{"max-size-buffers": 1, "max-size-bytes": 0, "max-size-time": 0, "leaky": 2}),
        "sink": _Element(**{"max-buffers": 1, "drop": True, "sync": False}),
        "rtp_queue": _Element(**{"max-size-buffers": 2, "max-size-bytes": 0, "max-size-time": 0, "leaky": 2}),
        "pay": _Element(pt=96, mtu=1200, ssrc=ssrc, **{"config-interval": 1}),
        "udp_sink": _Element(host="192.168.50.1", port=5000, sync=False, **{"async": False}),
    }
    backend._pipeline = SimpleNamespace(get_by_name=lambda name: elements.get(name))  # type: ignore[assignment]


def test_instantiated_pipeline_properties_include_device_queues_payloader_and_ssrc() -> None:
    backend = V4L2CaptureBackend("front_camera", _camera(format=CameraFormat.H264), surface_stream=_surface())
    _instantiated_pipeline(backend)
    backend._verify_instantiated_properties()
    _instantiated_pipeline(backend, ssrc=999)
    with pytest.raises(CaptureBackendError, match="ssrc"):
        backend._verify_instantiated_properties()


def test_appsink_rejects_a_short_bgr_stride() -> None:
    backend = GStreamerCaptureBackend(4, 3, 30)
    buffer = SimpleNamespace(
        pts=7,
        map=lambda _flags: (True, SimpleNamespace(data=bytes(12))),
        unmap=lambda _mapping: None,
    )
    structure = SimpleNamespace(get_value=lambda name: {"width": 4, "height": 3, "format": "BGR"}[name])
    caps = SimpleNamespace(get_size=lambda: 1, get_structure=lambda _index: structure)
    sample = SimpleNamespace(get_buffer=lambda: buffer, get_caps=lambda: caps)
    backend._gst = SimpleNamespace(MapFlags=SimpleNamespace(READ=1))
    backend._pipeline = object()
    backend._sink = SimpleNamespace(emit=lambda _name, _timeout: sample)
    backend._raise_bus_failure = lambda: None  # type: ignore[method-assign]
    backend._timestamps[7] = (1, 2, None)
    with pytest.raises(CaptureBackendError, match="stride"):
        backend.poll(0.0)


def test_v4l2_start_tears_down_when_post_start_verification_fails(monkeypatch) -> None:
    backend = V4L2CaptureBackend("front_camera", _camera(format=CameraFormat.H264))
    resolved = ResolvedV4L2Device(
        "front_camera",
        backend.camera.device_path,  # type: ignore[arg-type]
        Path("/dev/video4"),
        CameraPathKind.BY_ID,
        CameraResolutionTier.BY_ID,
        "camera-serial",
        {},
    )
    backend.device_probe = SimpleNamespace(validate=lambda *_args, **_kwargs: SimpleNamespace(device=resolved))

    def fake_start(self):
        self._pipeline = object()

    def fake_stop(self):
        self._pipeline = None

    monkeypatch.setattr(GStreamerCaptureBackend, "start", fake_start)
    monkeypatch.setattr(GStreamerCaptureBackend, "stop", fake_stop)
    monkeypatch.setattr(
        backend,
        "_verify_instantiated_properties",
        lambda: (_ for _ in ()).throw(CaptureBackendError("invalid instantiated property")),
    )
    with pytest.raises(CaptureBackendError, match="invalid instantiated property"):
        backend.start()
    assert not backend.running
    assert backend.resolved_device is None


def test_camera_entrypoint_reports_mode_error_with_canonical_code(monkeypatch, capsys) -> None:
    def reject(_argv):
        raise V4L2ModeUnsupported("configured exact tuple is absent")

    monkeypatch.setattr(camera_entrypoints, "camera_main", reject)
    assert camera_entrypoints.camera_entrypoint([]) == 78
    assert "CAMERA_MODE_UNSUPPORTED" in capsys.readouterr().err


def test_mjpeg_surface_recording_is_rejected_without_mislabeling_the_receiver(
    monkeypatch,
    capsys,
) -> None:
    config = _config()
    cameras = dict(config.cameras)
    cameras["front_camera"] = cameras["front_camera"].model_copy(update={"format": CameraFormat.MJPEG})
    monkeypatch.setattr(
        video_entrypoints,
        "load_config",
        lambda _path: config.model_copy(update={"cameras": cameras}),
    )
    result = video_entrypoints.video_receiver_entrypoint(["--camera", "front_camera", "--record-session", "phase10"])
    assert result == 78
    assert "MJPEG surface receive is supported" in capsys.readouterr().err


def test_provisioning_rule_is_narrow_and_carries_runtime_metadata() -> None:
    config = load_config(ROOT / "config" / "development.yaml", environ={})
    candidate = Candidate(
        "/dev/video2",
        "",
        "pci-0000:00:14.0-usb-0:1.2:1.0",
        "046d",
        "0825",
        "HD_Webcam",
    )
    assignments = build_assignments(config, (candidate,))
    rules = render_rules(assignments)
    assert assignments[0].resolution_tier == "id_path"
    assert 'ENV{ID_PATH}=="pci-0000:00:14.0-usb-0:1.2:1.0"' in rules
    assert 'ATTR{index}=="0"' in rules
    assert 'ENV{ID_VENDOR_ID}=="046d"' in rules
    assert 'ENV{ID_MODEL_ID}=="0825"' in rules
    assert 'ENV{PURDUE_ROV_CV_CAMERA_ID}="usb_webcam"' in rules
    assert 'SYMLINK+="purdue-rov-cv/usb_webcam"' in rules
    assert "/dev/video2" not in rules


def test_provisioning_rejects_fallback_when_by_id_identity_exists() -> None:
    config = load_config(ROOT / "config" / "development.yaml", environ={})
    candidate = Candidate(
        "/dev/video2",
        "serial-123",
        "pci-0000:00:14.0-usb-0:1.2:1.0",
        "046d",
        "0825",
        "HD_Webcam",
        by_id=("/dev/v4l/by-id/usb-HD_Webcam_serial-123-video-index0",),
    )
    with pytest.raises(ValueError, match="stronger by-id identity is available"):
        build_assignments(config, (candidate,))


def test_physical_port_rule_carries_index_and_auditable_port_label() -> None:
    config = load_config(ROOT / "config" / "development.yaml", environ={})
    original = config.cameras["usb_webcam"]
    camera = original.model_copy(
        update={
            "resolution_tier": CameraResolutionTier.PHYSICAL_PORT,
            "physical_port_label": "ROV hub port 3",
        }
    )
    configured = config.model_copy(update={"cameras": {"usb_webcam": camera}})
    candidate = Candidate(
        "/dev/video6",
        "",
        "pci-0000:00:14.0-usb-0:1.2:1.0",
        "046d",
        "0825",
        "HD_Webcam",
        video_index="0",
    )
    rules = render_rules(build_assignments(configured, (candidate,)))
    assert 'ATTR{index}=="0"' in rules
    assert 'ENV{PURDUE_ROV_CV_TIER}="physical_port"' in rules
    assert 'ENV{PURDUE_ROV_CV_PORT_LABEL}="ROV hub port 3"' in rules


def test_fallback_uses_the_unique_capture_interface_index_and_rejects_ambiguity() -> None:
    config = load_config(ROOT / "config" / "development.yaml", environ={})
    identity = config.cameras["usb_webcam"].stable_identity
    assert identity is not None
    interface = Candidate(
        "/dev/video7",
        "",
        identity,
        "046d",
        "0825",
        "HD_Webcam",
        video_index="1",
    )
    rules = render_rules(build_assignments(config, (interface,)))
    assert 'ATTR{index}=="1"' in rules

    second = Candidate(
        "/dev/video8",
        "",
        identity,
        "046d",
        "0825",
        "HD_Webcam",
        video_index="2",
    )
    with pytest.raises(ValueError, match="matched 2 capture devices"):
        build_assignments(config, (interface, second))


def test_backend_specific_identity_rejects_legacy_and_irrelevant_fields() -> None:
    data = yaml.safe_load((ROOT / "config" / "mission.yaml").read_text(encoding="utf-8"))
    data["cameras"]["front_camera"]["adapter"] = "v4l2"
    with pytest.raises(Exception, match="adapter"):
        parse_config_data(data)
    camera = data["cameras"]["front_camera"]
    camera.update(
        {
            "adapter": "depthai",
            "device_path": None,
            "device_path_kind": None,
            "resolution_tier": None,
            "mxid": "MXID-001",
        }
    )
    assert parse_config_data(data).cameras["front_camera"].mxid == "MXID-001"
