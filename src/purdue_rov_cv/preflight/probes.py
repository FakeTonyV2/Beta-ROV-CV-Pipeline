"""Injectable production and deterministic simulated preflight probes."""

from __future__ import annotations

import subprocess
import time
from collections.abc import Callable
from pathlib import Path
from typing import Protocol
from uuid import uuid4

import psutil
import zmq
from purdue_rov.cv.v1 import control_pb2, diagnostics_pb2

from purdue_rov_cv.config.models import AppConfig
from purdue_rov_cv.config.probes import CameraProbeResult, HardwareProbe, LinuxHardwareProbe, sha256_file
from purdue_rov_cv.frame_buffer import ReadStatus, SharedMemoryFrameReader
from purdue_rov_cv.messaging.client import ControlClient
from purdue_rov_cv.recording.disk import GIB, DiskSpaceGuard
from purdue_rov_cv.runtime.envelope import EnvelopeBuilder, ReceivedMultipartValidator
from purdue_rov_cv.runtime.metrics import RuntimeMetrics
from purdue_rov_cv.runtime.publisher import PublisherSequence
from purdue_rov_cv.runtime.state import ComponentState, from_wire_component_state

from .checks import CameraMeasurement, CorrelationMeasurement, EvidenceKind, ProbeSnapshot
from .clock import ChronyClockProbe, ClockMonitor, ClockProbe, ClockSample


class PreflightProbe(Protocol):
    def collect(self, config: AppConfig, *, camera_duration_seconds: float) -> ProbeSnapshot: ...


def _required_component_states(config: AppConfig) -> dict[str, ComponentState]:
    states = {
        "network": ComponentState.RUNNING,
        "chronyd": ComponentState.RUNNING,
        "broker": ComponentState.RUNNING,
        "control_router": ComponentState.RUNNING,
        "recorder": ComponentState.RUNNING,
        "operator": ComponentState.RUNNING,
    }
    states.update({f"camera:{key}": ComponentState.RUNNING for key in config.cameras})
    states.update({f"module:{key}": ComponentState.RUNNING for key, task in config.tasks.items() if task.enabled})
    states.update(
        {
            f"video_receiver:{key}": ComponentState.RUNNING
            for key, camera in config.cameras.items()
            if camera.stream_to_surface
        }
    )
    return states


class SimulatedPreflightProbe:
    """Coherent nominal evidence with narrowly scoped deterministic faults."""

    SCENARIOS = frozenset(
        {
            "success",
            "invalid_model_hash",
            "invalid_camera_mode",
            "unsynchronized_clock",
            "missing_component",
            "component_error",
        }
    )

    def __init__(
        self,
        scenario: str = "success",
        *,
        monotonic: Callable[[], float] = time.monotonic,
        wait: Callable[[float], None] = time.sleep,
    ) -> None:
        if scenario not in self.SCENARIOS:
            raise ValueError(f"unknown simulated preflight scenario: {scenario}")
        self.scenario = scenario
        self._monotonic = monotonic
        self._wait = wait

    def collect(self, config: AppConfig, *, camera_duration_seconds: float) -> ProbeSnapshot:
        started = self._monotonic()
        self._wait(camera_duration_seconds)
        now = self._monotonic()
        model_hashes = {
            key: (task.artifact.sha256, task.artifact.sha256) for key, task in config.tasks.items() if task.enabled
        }
        cameras = {
            key: CameraMeasurement(
                camera.frame_rate,
                camera.frame_rate * 0.98,
                min(500.0, 1_000.0 / camera.frame_rate * 1.1),
                True,
                max(camera_duration_seconds, now - started),
            )
            for key, camera in config.cameras.items()
        }
        usb = {key: True for key in config.cameras}
        streams = {key: True for key, camera in config.cameras.items() if camera.stream_to_surface}
        correlations = {key: CorrelationMeasurement(100, 96) for key in streams}
        control = {key: True for key, task in config.tasks.items() if task.enabled}
        module_frames = {key: 10 for key in control}
        component_states = _required_component_states(config)
        missing: tuple[str, ...] = ()
        clock = ClockSample(True, True, 1.0, now, "simulated synchronized clock")
        if self.scenario == "invalid_model_hash" and model_hashes:
            key = sorted(model_hashes)[0]
            expected, _actual = model_hashes[key]
            model_hashes[key] = (expected, "f" * 64 if expected != "f" * 64 else "e" * 64)
        elif self.scenario == "invalid_camera_mode" and cameras:
            key = sorted(cameras)[0]
            cameras[key] = CameraMeasurement(
                cameras[key].configured_fps,
                0.0,
                float("inf"),
                False,
                0.0,
            )
        elif self.scenario == "unsynchronized_clock":
            clock = ClockSample(False, False, 25.0, now, "injected clock loss")
        elif self.scenario == "missing_component":
            key = sorted(component_states)[0]
            component_states.pop(key)
            missing = (key,)
        elif self.scenario == "component_error":
            key = sorted(component_states)[0]
            component_states[key] = ComponentState.ERROR
        return ProbeSnapshot(
            model_hashes,
            True,
            True,
            True,
            control,
            clock,
            usb,
            cameras,
            streams,
            correlations,
            40.0,
            55.0,
            False,
            20 * GIB,
            module_frames,
            component_states,
            missing,
            now,
            evidence_by_check={f"PFL-{number:03d}": EvidenceKind.SIMULATED for number in range(2, 21)},
        )


class BrokerRuntimeEvidenceProvider:
    """Measure live camera and canonical health traffic over one bounded window."""

    def __init__(
        self,
        *,
        monotonic: Callable[[], float] = time.monotonic,
        wait: Callable[[float], None] = time.sleep,
    ) -> None:
        self._monotonic = monotonic
        self._wait = wait

    @staticmethod
    def _temperature() -> float | None:
        try:
            groups = psutil.sensors_temperatures()
        except (AttributeError, OSError):
            return None
        values = [entry.current for entries in groups.values() for entry in entries if entry.current is not None]
        return max(values) if values else None

    @staticmethod
    def _thermally_throttled() -> bool | None:
        path = Path("/sys/devices/platform/soc/soc:firmware/get_throttled")
        try:
            raw = path.read_text(encoding="ascii").strip().lower()
        except OSError:
            return None
        return raw not in {"0", "0x0"}

    def __call__(self, config: AppConfig, duration_seconds: float) -> dict[str, object]:
        context = zmq.Context()
        socket: zmq.Socket[bytes] = context.socket(zmq.SUB)
        socket.setsockopt(zmq.LINGER, 0)
        socket.setsockopt(zmq.SUBSCRIBE, b"cv.health.")
        socket.connect(config.messaging.broker.subscriber_endpoint)
        validator = ReceivedMultipartValidator(RuntimeMetrics())
        readers = {
            camera_id: SharedMemoryFrameReader(
                camera_id,
                expected_slot_capacity_bytes=camera.slot_capacity_bytes,
            )
            for camera_id, camera in config.cameras.items()
        }
        arrivals: dict[str, list[float]] = {camera_id: [] for camera_id in readers}
        last_frame_numbers: dict[str, int] = {}
        health: dict[str, tuple[diagnostics_pb2.DiagnosticStatus, float]] = {}
        cpu_samples: list[float] = []
        temperatures: list[float] = []
        psutil.cpu_percent(interval=None)
        try:
            for reader in readers.values():
                try:
                    reader.attach()
                except Exception:
                    continue
            # Establish that every required source is producing before starting
            # the normative measurement interval. This is a bounded readiness
            # probe, not part of the measured ten-second soak.
            warmup_deadline = self._monotonic() + 2.0
            while len(last_frame_numbers) < len(readers) and self._monotonic() < warmup_deadline:
                for camera_id, reader in readers.items():
                    try:
                        if not reader.attached:
                            reader.attach()
                        result = reader.read()
                        if result.status is ReadStatus.FRAME and result.header is not None:
                            last_frame_numbers[camera_id] = result.header.frame_number
                    except Exception:
                        reader.close()
                self._wait(0.005)
            started = self._monotonic()
            deadline = started + duration_seconds
            next_resource_sample = started
            while self._monotonic() < deadline:
                now = self._monotonic()
                for camera_id, reader in readers.items():
                    try:
                        if not reader.attached:
                            reader.attach()
                        result = reader.read()
                        if (
                            result.status is ReadStatus.FRAME
                            and result.header is not None
                            and result.header.frame_number != last_frame_numbers.get(camera_id)
                        ):
                            last_frame_numbers[camera_id] = result.header.frame_number
                            arrivals[camera_id].append(now)
                    except Exception:
                        reader.close()
                while socket.poll(0, zmq.POLLIN):
                    validated = validator.validate(socket.recv_multipart())
                    if not validated.valid or validated.envelope is None:
                        continue
                    if validated.envelope.payload_type != "diagnostic_status_v1":
                        continue
                    payload = diagnostics_pb2.DiagnosticStatus.FromString(validated.envelope.payload)
                    health[payload.source_id] = (payload, now)
                if now >= next_resource_sample:
                    cpu_samples.append(float(psutil.cpu_percent(interval=None)))
                    temperature = self._temperature()
                    if temperature is not None:
                        temperatures.append(temperature)
                    next_resource_sample = now + 0.1
                self._wait(0.005)
        finally:
            for reader in readers.values():
                reader.close()
            socket.close(linger=0)
            context.term()
        ended = self._monotonic()
        elapsed = ended - started
        cameras: dict[str, CameraMeasurement] = {}
        for camera_id, values in arrivals.items():
            boundaries = [started, *values, ended]
            maximum_gap_ms = (
                max((right - left) * 1_000.0 for left, right in zip(boundaries, boundaries[1:], strict=False))
                if values
                else float("inf")
            )
            cameras[camera_id] = CameraMeasurement(
                config.cameras[camera_id].frame_rate,
                len(values) / elapsed if elapsed > 0 else 0.0,
                maximum_gap_ms,
                camera_id in last_frame_numbers and bool(values),
                elapsed if camera_id in last_frame_numbers and bool(values) else 0.0,
            )
        freshness = max(2.0, 2 * config.diagnostics.publish_interval_ms / 1_000.0)
        fresh_health = {
            source_id: status
            for source_id, (status, observed) in health.items()
            if 0.0 <= ended - observed <= freshness
        }
        states: dict[str, ComponentState] = {}
        module_frames: dict[str, int] = {}
        for task_id, task in config.tasks.items():
            if not task.enabled or (status := fresh_health.get(task_id)) is None:
                continue
            states[f"module:{task_id}"] = from_wire_component_state(status.state)
            module_frames[task_id] = int(status.module.frames_processed)
        streams: dict[str, bool] = {}
        correlations: dict[str, CorrelationMeasurement] = {}
        for camera_id, camera in config.cameras.items():
            if cameras[camera_id].opened:
                states[f"camera:{camera_id}"] = ComponentState.RUNNING
            if not camera.stream_to_surface:
                continue
            status = fresh_health.get(f"video_receiver_{camera.stream_index}")
            if status is None:
                continue
            states[f"video_receiver:{camera_id}"] = from_wire_component_state(status.state)
            streams[camera_id] = int(status.video.rtp_packets_received) > 0
            correlations[camera_id] = CorrelationMeasurement(
                int(status.video.decoded_frames),
                int(status.video.frame_index_hits),
            )
        recorder = fresh_health.get("recorder")
        if recorder is not None:
            states["recorder"] = from_wire_component_state(recorder.state)
        unavailable: dict[str, str] = {}
        if not temperatures:
            unavailable["PFL-016"] = "no production temperature sensor was available during the sample window"
        throttled = self._thermally_throttled()
        if throttled is None:
            unavailable["PFL-017"] = "the production thermal-throttle status file is unavailable"
        return {
            "cameras": cameras,
            "component_states": states,
            "rtp_streams": streams,
            "correlations": correlations,
            "module_processed_frames": module_frames,
            "average_cpu_percent": sum(cpu_samples) / len(cpu_samples) if cpu_samples else 0.0,
            "maximum_temperature_c": max(temperatures) if temperatures else 0.0,
            "thermally_throttled": bool(throttled),
            "unavailable_checks": unavailable,
        }


class LocalSystemPreflightProbe:
    """Best-effort Linux probe for a running deployment.

    Runtime camera/video/module evidence is intentionally supplied through the
    health/metrics provider when one is configured. This keeps hardware access
    behind an injectable boundary and makes missing telemetry fail explicitly.
    """

    def __init__(
        self,
        *,
        clock_probe: ClockProbe | None = None,
        hardware_probe: HardwareProbe | None = None,
        disk_guard: DiskSpaceGuard | None = None,
        runtime_evidence: Callable[[AppConfig, float], dict[str, object]] | None = None,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.clock_probe = clock_probe or ChronyClockProbe(monotonic=monotonic)
        self.hardware_probe = hardware_probe or LinuxHardwareProbe()
        self.disk_guard = disk_guard or DiskSpaceGuard()
        self.runtime_evidence = runtime_evidence
        self._monotonic = monotonic

    @staticmethod
    def _hashes(config: AppConfig) -> dict[str, tuple[str, str]]:
        result: dict[str, tuple[str, str]] = {}
        for key, task in config.tasks.items():
            if not task.enabled:
                continue
            try:
                actual = sha256_file(task.artifact.path)
            except OSError:
                actual = "<unreadable>"
            result[key] = (task.artifact.sha256, actual)
        return result

    @staticmethod
    def _interface_up(name: str) -> bool:
        try:
            return Path(f"/sys/class/net/{name}/operstate").read_text(encoding="ascii").strip() == "up"
        except OSError:
            return False

    @staticmethod
    def _surface_responds(address: str) -> bool:
        try:
            result = subprocess.run(
                ["ping", "-n", "-c", "1", "-W", "1", address],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=2.0,
                check=False,
            )
            return result.returncode == 0
        except (OSError, subprocess.SubprocessError):
            return False

    @staticmethod
    def _broker_round_trip(config: AppConfig) -> bool:
        context = zmq.Context()
        pub = context.socket(zmq.PUB)
        sub = context.socket(zmq.SUB)
        pub.setsockopt(zmq.LINGER, 0)
        sub.setsockopt(zmq.LINGER, 0)
        sub.setsockopt(zmq.SUBSCRIBE, b"system.health.preflight_probe")
        pub.connect(config.messaging.broker.publisher_endpoint)
        sub.connect(config.messaging.broker.subscriber_endpoint)
        builder = EnvelopeBuilder(PublisherSequence())
        validator = ReceivedMultipartValidator(RuntimeMetrics())
        deadline = time.monotonic() + 2.0
        try:
            time.sleep(0.1)
            while time.monotonic() < deadline:
                built = builder.build(
                    topic="system.health.preflight_probe",
                    payload_type="diagnostic_status_v1",
                    payload=diagnostics_pb2.DiagnosticStatus(source_id="preflight_probe"),
                    task_id="",
                    source_id="preflight_probe",
                )
                pub.send_multipart(built.frames)
                if sub.poll(100, zmq.POLLIN) and validator.validate(sub.recv_multipart()).valid:
                    return True
            return False
        finally:
            pub.close(linger=0)
            sub.close(linger=0)
            context.term()

    @staticmethod
    def _control_status(config: AppConfig) -> dict[str, bool]:
        result: dict[str, bool] = {}
        with ControlClient(config.messaging.control.client_endpoint) as client:
            for task_id, task in config.tasks.items():
                if not task.enabled:
                    continue
                request = control_pb2.CommandRequest(
                    command_id=uuid4().bytes,
                    target_id=task_id,
                    issued_time_unix_ns=time.time_ns(),
                )
                request.get_status.SetInParent()
                response = client.send_command(request)
                result[task_id] = response.status == control_pb2.COMMAND_STATUS_COMPLETED
        return result

    def collect(self, config: AppConfig, *, camera_duration_seconds: float) -> ProbeSnapshot:
        now = self._monotonic()
        unavailable: dict[str, str] = {}
        camera_probe: dict[str, CameraProbeResult] = {}
        for key, camera in config.cameras.items():
            try:
                camera_probe[key] = self.hardware_probe.probe_camera(key, camera)
            except Exception as error:
                unavailable["PFL-008"] = f"USB probe could not execute: {type(error).__name__}: {error}"
                unavailable["PFL-009"] = f"camera-mode probe could not execute: {type(error).__name__}: {error}"
        if self.runtime_evidence is None:
            runtime: dict[str, object] = {}
            detail = "no authoritative runtime health/metrics provider was configured"
            for check_id in (
                "PFL-010",
                "PFL-011",
                "PFL-012",
                "PFL-013",
                "PFL-014",
                "PFL-015",
                "PFL-016",
                "PFL-017",
                "PFL-019",
                "PFL-020",
            ):
                unavailable[check_id] = detail
        else:
            try:
                runtime = self.runtime_evidence(config, camera_duration_seconds)
            except Exception as error:
                runtime = {}
                detail = f"runtime health/metrics collection failed: {type(error).__name__}: {error}"
                for check_id in (
                    "PFL-010",
                    "PFL-011",
                    "PFL-012",
                    "PFL-013",
                    "PFL-014",
                    "PFL-015",
                    "PFL-016",
                    "PFL-017",
                    "PFL-019",
                    "PFL-020",
                ):
                    unavailable[check_id] = detail
        runtime_unavailable = runtime.get("unavailable_checks", {})
        if isinstance(runtime_unavailable, dict):
            unavailable.update(
                {
                    str(check_id): str(detail)
                    for check_id, detail in runtime_unavailable.items()
                    if isinstance(check_id, str) and isinstance(detail, str)
                }
            )
        cameras = runtime.get("cameras", {})
        states = runtime.get("component_states", {})
        rtp_streams = runtime.get("rtp_streams", {})
        correlations = runtime.get("correlations", {})
        module_frames = runtime.get("module_processed_frames", {})
        cpu = runtime.get("average_cpu_percent")
        temperature = runtime.get("maximum_temperature_c")
        state_mapping = dict(states) if isinstance(states, dict) else {}
        runtime_cameras = dict(cameras) if isinstance(cameras, dict) else {}
        for key, result in camera_probe.items():
            measurement = runtime_cameras.get(key)
            if isinstance(measurement, CameraMeasurement):
                runtime_cameras[key] = CameraMeasurement(
                    measurement.configured_fps,
                    measurement.achieved_fps,
                    measurement.maximum_gap_ms,
                    result.capture_tuple_supported,
                    measurement.simultaneous_seconds,
                )
            else:
                runtime_cameras[key] = CameraMeasurement(
                    config.cameras[key].frame_rate,
                    0.0,
                    float("inf"),
                    result.capture_tuple_supported,
                    0.0,
                )
        try:
            free = self.disk_guard.inspect(config.recording.directory).free_bytes
        except (OSError, ValueError) as error:
            free = 0
            unavailable["PFL-018"] = f"disk probe could not execute: {type(error).__name__}: {error}"
        try:
            clock = self.clock_probe.check()
        except Exception as error:
            clock = ClockSample(False, False, float("inf"), now, "clock probe unavailable")
            unavailable["PFL-007"] = f"clock probe could not execute: {type(error).__name__}: {error}"
        try:
            broker_round_trip = self._broker_round_trip(config)
        except Exception as error:
            broker_round_trip = False
            unavailable["PFL-005"] = f"broker probe could not execute: {type(error).__name__}: {error}"
        try:
            control_status = self._control_status(config)
        except Exception as error:
            control_status = {}
            unavailable["PFL-006"] = f"control probe could not execute: {type(error).__name__}: {error}"
        interface_up = self._interface_up(config.network.tether_interface)
        surface_responds = self._surface_responds(str(config.network.surface_ip))
        observed_now = self._monotonic()
        if interface_up:
            state_mapping["network"] = ComponentState.RUNNING
        if (
            ClockMonitor(
                maximum_offset_ms=config.clock.maximum_offset_ms,
                monotonic=lambda: observed_now,
            )
            .observe(clock)
            .synchronized
        ):
            state_mapping["chronyd"] = ComponentState.RUNNING
        if broker_round_trip:
            state_mapping["broker"] = ComponentState.RUNNING
        enabled_tasks = {task_id for task_id, task in config.tasks.items() if task.enabled}
        if enabled_tasks <= {task_id for task_id, passed in control_status.items() if passed}:
            state_mapping["control_router"] = ComponentState.RUNNING
            state_mapping["operator"] = ComponentState.RUNNING
        expected_components = set(_required_component_states(config))
        missing = tuple(sorted(expected_components - set(state_mapping)))
        hardware_evidence = {
            check_id: EvidenceKind.HARDWARE_VERIFIED
            for check_id in ("PFL-008", "PFL-009", "PFL-016", "PFL-017")
            if check_id not in unavailable
        }
        return ProbeSnapshot(
            self._hashes(config),
            interface_up,
            surface_responds,
            broker_round_trip,
            control_status,
            clock,
            {
                key: value.path_exists and value.resolves_to_video_device and value.path_kind_matches
                for key, value in camera_probe.items()
            },
            runtime_cameras,
            dict(rtp_streams) if isinstance(rtp_streams, dict) else {},
            dict(correlations) if isinstance(correlations, dict) else {},
            float(cpu) if isinstance(cpu, (int, float)) else 0.0,
            float(temperature) if isinstance(temperature, (int, float)) else 0.0,
            bool(runtime.get("thermally_throttled", False)),
            int(free),
            dict(module_frames) if isinstance(module_frames, dict) else {},
            state_mapping,
            missing,
            observed_now,
            unavailable,
            hardware_evidence,
        )


__all__ = [
    "BrokerRuntimeEvidenceProvider",
    "LocalSystemPreflightProbe",
    "PreflightProbe",
    "SimulatedPreflightProbe",
]
