"""End-to-end real-process fixture for simulated full-system preflight."""

from __future__ import annotations

import hashlib
import os
import socket
import subprocess
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass, replace
from pathlib import Path
from typing import TextIO

import yaml
import zmq
from purdue_rov.cv.v1 import bounding_box_pb2, diagnostics_pb2

from purdue_rov_cv.config.issues import ConfigIssue
from purdue_rov_cv.config.loader import load_config
from purdue_rov_cv.config.models import IDENTIFIER_PATTERN, AppConfig, CameraConfig
from purdue_rov_cv.config.probes import CameraProbeResult
from purdue_rov_cv.frame_buffer import ReadStatus, SharedMemoryFrameReader
from purdue_rov_cv.messaging.client import ControlClient
from purdue_rov_cv.recording.disk import DiskSpaceGuard
from purdue_rov_cv.replay.structured import IndexedMcapSource
from purdue_rov_cv.runtime.envelope import ReceivedMultipartValidator
from purdue_rov_cv.runtime.metrics import RuntimeMetrics
from purdue_rov_cv.runtime.state import ComponentState, from_wire_component_state
from purdue_rov_cv.wire.errors import ErrorCode

from .checks import EvidenceKind, PreflightReport, ProbeSnapshot
from .clock import ClockMonitor, ClockSample
from .health import ComponentHealth, MissionEnableGate, SystemHealthAggregator
from .operator import OperatorResult, SurfaceOperator
from .probes import BrokerRuntimeEvidenceProvider, LocalSystemPreflightProbe
from .runner import run_preflight


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as candidate:
        candidate.bind(("127.0.0.1", 0))
        return int(candidate.getsockname()[1])


def _free_stream_index() -> int:
    """Choose an unoccupied canonical RTP/RTCP pair for this test process."""

    start = os.getpid() % 32
    for offset in range(32):
        index = (start + offset) % 32
        sockets: list[socket.socket] = []
        try:
            for port in (5_000 + 2 * index, 5_001 + 2 * index):
                candidate = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                candidate.bind(("127.0.0.1", port))
                sockets.append(candidate)
            return index
        except OSError:
            continue
        finally:
            for candidate in sockets:
                candidate.close()
    raise RuntimeError("no isolated canonical RTP/RTCP port pair is available")


def _active_interface(sysfs_root: Path = Path("/sys/class/net")) -> str:
    active: list[str] = []
    rejected: list[str] = []
    for candidate in sorted(sysfs_root.iterdir()):
        try:
            if (candidate / "operstate").read_text(encoding="ascii").strip() == "up":
                if IDENTIFIER_PATTERN.fullmatch(candidate.name):
                    active.append(candidate.name)
                else:
                    rejected.append(candidate.name)
        except OSError:
            continue
    if active:
        return next((name for name in active if name != "lo"), active[0])
    detail = f"; schema-invalid active interfaces={rejected}" if rejected else ""
    raise RuntimeError(f"no schema-valid network interface reports operstate UP{detail}")


@dataclass(slots=True)
class ManagedProcess:
    name: str
    process: subprocess.Popen[str]
    log_path: Path
    log_file: TextIO


class _HealthCollector:
    """Consume canonical diagnostic envelopes without inventing component state."""

    def __init__(self, endpoint: str) -> None:
        self.context = zmq.Context()
        self.socket: zmq.Socket[bytes] = self.context.socket(zmq.SUB)
        self.socket.setsockopt(zmq.LINGER, 0)
        self.socket.setsockopt(zmq.SUBSCRIBE, b"cv.health.")
        self.socket.connect(endpoint)
        self.validator = ReceivedMultipartValidator(RuntimeMetrics())
        self.latest: dict[str, tuple[diagnostics_pb2.DiagnosticStatus, float]] = {}

    def drain(self) -> None:
        while self.socket.poll(0, zmq.POLLIN):
            validated = self.validator.validate(self.socket.recv_multipart())
            if not validated.valid or validated.envelope is None:
                continue
            if validated.envelope.payload_type != "diagnostic_status_v1":
                continue
            payload = diagnostics_pb2.DiagnosticStatus.FromString(validated.envelope.payload)
            self.latest[payload.source_id] = (payload, time.monotonic())

    def get(self, source_id: str, *, maximum_age_seconds: float = 2.0) -> diagnostics_pb2.DiagnosticStatus | None:
        self.drain()
        observed = self.latest.get(source_id)
        if observed is None or time.monotonic() - observed[1] > maximum_age_seconds:
            return None
        return observed[0]

    def close(self) -> None:
        self.socket.close(linger=0)
        self.context.term()


class _HarnessPreflightProbe:
    def __init__(self, harness: Phase9ProcessHarness) -> None:
        self.harness = harness

    def collect(self, config: AppConfig, *, camera_duration_seconds: float) -> ProbeSnapshot:
        return self.harness.collect_actual_evidence(config, camera_duration_seconds)


class _SimulatedHardwareProbe:
    def probe_camera(self, camera_id: str, camera: CameraConfig) -> CameraProbeResult:
        return CameraProbeResult(True, True, True, True, f"simulated device for {camera_id}")

    def validate_runtime_and_artifact(self, config: AppConfig) -> tuple[ConfigIssue, ...]:
        return ()

    def validate_port_availability(self, config: AppConfig) -> tuple[ConfigIssue, ...]:
        return ()


class _HarnessRuntimeEvidence:
    def __init__(self) -> None:
        self.production = BrokerRuntimeEvidenceProvider()

    def __call__(self, config: AppConfig, duration_seconds: float) -> dict[str, object]:
        evidence = self.production(config, duration_seconds)
        evidence["maximum_temperature_c"] = 55.0
        evidence["thermally_throttled"] = False
        unavailable = evidence.get("unavailable_checks")
        if isinstance(unavailable, dict):
            unavailable.pop("PFL-016", None)
            unavailable.pop("PFL-017", None)
        return evidence


class _SynchronizedClockProbe:
    def check(self) -> ClockSample:
        return ClockSample(True, True, 1.0, time.monotonic(), "simulated chrony service")


class Phase9ProcessHarness:
    """Launch real repository services with isolated ports, IPC, SHM, and files."""

    def __init__(self, root: Path, *, python: str = sys.executable) -> None:
        self.root = root.absolute()
        self.python = python
        self.root.mkdir(parents=True, exist_ok=True)
        self.config_path = self.root / "mission.yaml"
        self.marker_path = self.root / "subscriber.result"
        self.second_marker_path = self.root / "subscriber-second.result"
        self.recording_root = self.root / "recordings"
        self.recording_path = self.recording_root / "phase9" / "structured.mcap"
        self.artifact_path = self.root / "echo.onnx"
        self.module_socket = self.root / "module-control.sock"
        self.publisher_endpoint = f"tcp://127.0.0.1:{_free_port()}"
        self.subscriber_endpoint = f"tcp://127.0.0.1:{_free_port()}"
        self.control_endpoint = f"tcp://127.0.0.1:{_free_port()}"
        self.processes: list[ManagedProcess] = []
        self.stream_index = _free_stream_index()
        self.tether_interface = _active_interface()
        self.startup_order: list[str] = []
        self.shutdown_elapsed: float | None = None
        self.shutdown_durations: dict[str, float] = {}
        self.preflight_report: PreflightReport | None = None
        self.last_probe_snapshot: ProbeSnapshot | None = None
        self.gate = MissionEnableGate()
        self._health = _HealthCollector(self.subscriber_endpoint)
        self._shutdown_complete = False
        self._camera_probe_detail = "not attempted"
        self._module_probe_detail = "not attempted"
        self._write_config()

    def _write_config(self) -> None:
        artifact = b"phase9 deterministic simulated model artifact\n"
        self.artifact_path.write_bytes(artifact)
        artifact_hash = hashlib.sha256(artifact).hexdigest()
        config = {
            "schema_version": 1,
            "device": {"device_id": "rov_pi5", "execution_target": "rov_pi5"},
            "network": {
                "tether_interface": self.tether_interface,
                "rov_ip": "127.0.0.1",
                "surface_ip": "127.0.0.1",
            },
            "clock": {
                "server_ip": "127.0.0.1",
                "maximum_offset_ms": 10,
                "check_interval_seconds": 5,
                "invalid_after_failures": 3,
            },
            "messaging": {
                "broker": {
                    "publisher_endpoint": self.publisher_endpoint,
                    "subscriber_endpoint": self.subscriber_endpoint,
                },
                "control": {
                    "client_endpoint": self.control_endpoint,
                    "module_endpoint": f"ipc://{self.module_socket}",
                },
                "max_message_bytes": 4_194_304,
                "result_send_hwm": 5,
                "result_receive_hwm": 5,
            },
            "diagnostics": {"publish_interval_ms": 500, "log_level": "INFO"},
            "debug_snapshots": {
                "enabled": False,
                "maximum_rate_hz": 1.0,
                "maximum_width": 64,
                "maximum_height": 48,
                "jpeg_quality": 70,
            },
            "recording": {
                "enabled": True,
                "directory": self.recording_root.as_posix(),
                "video_segment_seconds": 300,
                "minimum_free_space_gib": 10,
                "structured": {"chunk_size_bytes": 1_048_576, "compression": "zstd"},
            },
            "camera_limits": {"maximum_configured": 16, "maximum_active": 8},
            "cameras": {
                "front_camera": {
                    "adapter": "gstreamer_v4l2",
                    "device_path": "/dev/v4l/by-id/phase9-simulated-camera",
                    "device_path_kind": "by_id",
                    "resolution_tier": "by_id",
                    "format": "h264",
                    "width": 64,
                    "height": 48,
                    "frame_rate": 20,
                    "stream_index": self.stream_index,
                    "stream_to_surface": True,
                    "cv_enabled": True,
                    "allow_software_encode": False,
                    "slot_capacity_bytes": 9_216,
                }
            },
            "tasks": {
                "echo": {
                    "module_class": "purdue_rov_cv.modules.echo.EchoModule",
                    "enabled": True,
                    "input_camera": "front_camera",
                    "execution_target": "rov_pi5",
                    "max_input_fps": 20,
                    "processing_deadline_ms": 100,
                    "publish_topic": "cv.result.echo.front_camera",
                    "payload_type": "bounding_boxes_v1",
                    "dynamic": {"confidence_threshold": 0.5},
                    "artifact": {
                        "format": "onnx",
                        "path": self.artifact_path.as_posix(),
                        "sha256": artifact_hash,
                        "runtime": "onnxruntime",
                    },
                }
            },
        }
        self.config_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")

    def _launch(self, name: str, *arguments: str) -> ManagedProcess:
        log_path = self.root / f"{name}.log"
        log_file = log_path.open("w", encoding="utf-8")
        environment = dict(os.environ)
        repository = Path(__file__).parents[3]
        python_path = [str(repository / "src"), str(repository / "generated" / "python")]
        if environment.get("PYTHONPATH"):
            python_path.append(environment["PYTHONPATH"])
        environment["PYTHONPATH"] = os.pathsep.join(python_path)
        process = subprocess.Popen(
            [self.python, "-m", "purdue_rov_cv.preflight.process_role", *arguments],
            stdout=log_file,
            stderr=subprocess.STDOUT,
            text=True,
            env=environment,
        )
        managed = ManagedProcess(name, process, log_path, log_file)
        self.processes.append(managed)
        return managed

    @staticmethod
    def _wait_until(
        predicate: Callable[[], bool],
        description: str,
        timeout: float = 60.0,
        *,
        process: ManagedProcess | None = None,
    ) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return
            if process is not None and process.process.poll() is not None:
                process.log_file.flush()
                try:
                    detail = process.log_path.read_text(encoding="utf-8").strip()
                except OSError as error:
                    detail = f"log unavailable: {error}"
                raise RuntimeError(
                    f"{process.name} exited before {description}: returncode={process.process.returncode}; log={detail}"
                )
            time.sleep(0.1)
        raise TimeoutError(f"timed out waiting for {description}")

    @staticmethod
    def _tcp_ready(endpoint: str) -> bool:
        host_port = endpoint.removeprefix("tcp://")
        host, port = host_port.rsplit(":", 1)
        try:
            with socket.create_connection((host, int(port)), timeout=0.1):
                return True
        except OSError:
            return False

    @staticmethod
    def _module_status(operator: SurfaceOperator) -> OperatorResult | None:
        return operator.get_status("echo")

    def _broker_ready(self) -> bool:
        if not self._tcp_ready(self.publisher_endpoint):
            return False
        return LocalSystemPreflightProbe._broker_round_trip(load_config(self.config_path, environ={}))

    def _router_ready(self) -> bool:
        try:
            with ControlClient(self.control_endpoint, acknowledgement_timeout_seconds=0.2) as client:
                response = SurfaceOperator(client).get_status("missing")
            return response.error_code == ErrorCode.TARGET_UNAVAILABLE
        except Exception:
            return False

    def _health_running(self, source_id: str) -> bool:
        health = self._health.get(source_id)
        return health is not None and from_wire_component_state(health.state) is ComponentState.RUNNING

    def health_status(self, source_id: str) -> diagnostics_pb2.DiagnosticStatus | None:
        return self._health.get(source_id)

    def _camera_has_frame(self) -> bool:
        reader = SharedMemoryFrameReader("front_camera", expected_slot_capacity_bytes=9_216)
        try:
            if not reader.attach():
                self._camera_probe_detail = "shared memory not present"
                return False
            result = reader.read()
            self._camera_probe_detail = f"read status={result.status} header={result.header}"
            return result.status is ReadStatus.FRAME
        except Exception as error:
            self._camera_probe_detail = f"{type(error).__name__}: {error}"
            return False
        finally:
            reader.close()

    def start(
        self,
        *,
        disconnect_after_frames: int | None = None,
        slow_subscriber: bool = False,
        complete_preflight: bool = True,
    ) -> None:
        try:
            if not LocalSystemPreflightProbe._interface_up(self.tether_interface):
                raise RuntimeError(f"network interface {self.tether_interface} did not become ready")
            self.startup_order.append("network")
            simulated_clock = ClockMonitor(monotonic=lambda: 100.0).observe(ClockSample(True, True, 1.0, 100.0))
            if not simulated_clock.synchronized:
                raise RuntimeError("simulated chrony service did not become ready")
            self.startup_order.append("chronyd")
            broker_process = self._launch("broker", "broker", "--config", str(self.config_path))
            self._wait_until(self._broker_ready, "broker data-plane round trip", process=broker_process)
            self.startup_order.append("broker")
            router_process = self._launch("control-router", "router", "--config", str(self.config_path))
            self._wait_until(self._router_ready, "control router request/response", process=router_process)
            self.startup_order.append("control_router")
            camera_args = ["camera", "--config", str(self.config_path), "--camera", "front_camera"]
            if disconnect_after_frames is not None:
                camera_args.extend(["--disconnect-after-frames", str(disconnect_after_frames)])
            camera_process = self._launch("camera", *camera_args)
            try:
                self._wait_until(self._camera_has_frame, "simulated camera frame", process=camera_process)
            except TimeoutError as error:
                raise TimeoutError(
                    f"{error}; last probe={self._camera_probe_detail}; camera exit={camera_process.process.poll()}"
                ) from error
            self.startup_order.append("cameras")
            module_process = self._launch("module", "module", "--config", str(self.config_path), "--task", "echo")
            with ControlClient(self.control_endpoint, acknowledgement_timeout_seconds=0.2) as client:
                operator = SurfaceOperator(client)

                def ready() -> bool:
                    status = self._module_status(operator)
                    self._module_probe_detail = repr(status)
                    return status is not None and status.resulting_state == ComponentState.READY.value

                try:
                    self._wait_until(ready, "module readiness", process=module_process)
                except TimeoutError as error:
                    raise TimeoutError(
                        f"{error}; last status={self._module_probe_detail}; module exit={module_process.process.poll()}"
                    ) from error
                self.startup_order.append("modules")
                started = operator.start("echo")
                if not started.succeeded or started.resulting_state != ComponentState.RUNNING.value:
                    raise RuntimeError(f"module start failed: {started}")
                video_process = self._launch(
                    "video-receiver",
                    "video-receiver",
                    "--config",
                    str(self.config_path),
                    "--camera",
                    "front_camera",
                )
                self._wait_until(
                    lambda: self._health_running(f"video_receiver_{self.stream_index}"),
                    "video receiver decoded-frame readiness",
                    process=video_process,
                )
                recorder_process = self._launch(
                    "subscriber-primary",
                    "subscriber",
                    "--endpoint",
                    self.subscriber_endpoint,
                    "--topic",
                    "cv.result.echo.front_camera",
                    "--marker",
                    str(self.marker_path),
                    "--delay-seconds",
                    "0",
                )
                self._launch(
                    "subscriber-secondary",
                    "subscriber",
                    "--endpoint",
                    self.subscriber_endpoint,
                    "--topic",
                    "cv.result.echo.front_camera",
                    "--marker",
                    str(self.second_marker_path),
                    "--delay-seconds",
                    "0.2" if slow_subscriber else "0",
                )
                self.startup_order.append("video_receivers")
                self._launch(
                    "recorder",
                    "recorder",
                    "--config",
                    str(self.config_path),
                    "--session",
                    "phase9",
                )
                self._wait_until(
                    lambda: self.recording_path.exists() and self._health_running("recorder"),
                    "recorder health readiness",
                    process=recorder_process,
                )
                self.startup_order.append("recorder_operator")
                self._wait_until(self.marker_path.exists, "parsed CV result")
                self._wait_until(self.second_marker_path.exists, "second subscriber parsed CV result")
            if complete_preflight:
                self._complete_preflight_and_enable()
        except BaseException:
            self.shutdown(raise_on_failure=False)
            raise

    def _process_alive(self, name: str) -> bool:
        return any(item.name == name and item.process.poll() is None for item in self.processes)

    def collect_actual_evidence(self, config: AppConfig, duration_seconds: float) -> ProbeSnapshot:
        probe = LocalSystemPreflightProbe(
            clock_probe=_SynchronizedClockProbe(),
            hardware_probe=_SimulatedHardwareProbe(),
            disk_guard=DiskSpaceGuard(lambda _path: 20 * 1024**3),
            runtime_evidence=_HarnessRuntimeEvidence(),
        )
        snapshot = probe.collect(config, camera_duration_seconds=duration_seconds)
        simulated = {
            "PFL-003",
            "PFL-004",
            "PFL-007",
            "PFL-008",
            "PFL-009",
            "PFL-010",
            "PFL-011",
            "PFL-012",
            "PFL-013",
            "PFL-014",
            "PFL-015",
            "PFL-016",
            "PFL-017",
            "PFL-018",
            "PFL-020",
        }
        snapshot = replace(
            snapshot,
            evidence_by_check={check_id: EvidenceKind.SIMULATED for check_id in simulated},
        )
        self.last_probe_snapshot = snapshot
        return snapshot

    def _complete_preflight_and_enable(self) -> None:
        report = self.run_actual_preflight()
        self.preflight_report = report
        self.startup_order.append("preflight")
        snapshot = self.last_probe_snapshot
        if snapshot is None:
            raise RuntimeError("actual process evidence was not retained")
        now = snapshot.observed_monotonic
        assert now is not None
        clock_status = ClockMonitor(monotonic=lambda: now).observe(snapshot.clock)
        required = tuple(snapshot.component_states) + snapshot.missing_required_components
        aggregator = SystemHealthAggregator(required_components=required, monotonic=lambda: now)
        for component_id, state in snapshot.component_states.items():
            aggregator.update(ComponentHealth(component_id, component_id.split(":", 1)[0], state))
        health = aggregator.snapshot(
            clock=clock_status,
            preflight_completed=True,
            preflight_passed=report.exit_code == 0,
            preflight_exit_code=report.exit_code,
            preflight_run_id=report.run_id,
        )
        if not self.gate.authorize(health, startup_dependencies_satisfied=not snapshot.missing_required_components):
            raise RuntimeError(f"mission gate rejected successful fixture: {self.gate.reason}")
        self.startup_order.append("mission_enabled")

    def run_actual_preflight(self) -> PreflightReport:
        return run_preflight(self.config_path, _HarnessPreflightProbe(self))

    def exercise_control(self) -> tuple[OperatorResult, OperatorResult]:
        with ControlClient(self.control_endpoint, acknowledgement_timeout_seconds=0.2) as client:
            operator = SurfaceOperator(client)
            stopped = operator.stop("echo")
            started = operator.start("echo")
        return stopped, started

    def recorded_result_count(self) -> int:
        count = 0
        for record in IndexedMcapSource(self.recording_path).records():
            if record.topic != "cv.result.echo.front_camera":
                continue
            payload = bounding_box_pb2.BoundingBoxResult.FromString(record.envelope.payload)
            if payload.camera_id != "front_camera" or not payload.detections:
                raise ValueError("recorded echo result payload is incomplete")
            count += 1
        return count

    def log_text(self) -> str:
        chunks: list[str] = []
        for managed in self.processes:
            try:
                text = managed.log_path.read_text(encoding="utf-8")
            except OSError:
                text = "<unavailable>"
            chunks.append(f"===== {managed.name} =====\n{text}")
        return "\n".join(chunks)

    def resource_cleanup(self) -> dict[str, bool]:
        ports_released = not any(
            self._tcp_ready(endpoint)
            for endpoint in (self.publisher_endpoint, self.subscriber_endpoint, self.control_endpoint)
        )
        rtp_ports_released = True
        for port in (5_000 + 2 * self.stream_index, 5_001 + 2 * self.stream_index):
            candidate = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            try:
                candidate.bind(("127.0.0.1", port))
            except OSError:
                rtp_ports_released = False
            finally:
                candidate.close()
        return {
            "children_exited": all(item.process.poll() is not None for item in self.processes),
            "shared_memory_removed": not Path("/dev/shm/purdue_rov_cv_front_camera").exists(),
            "control_socket_removed": not self.module_socket.exists(),
            "tcp_ports_released": ports_released,
            "rtp_ports_released": rtp_ports_released,
            "subscriber_temporaries_removed": not any(self.root.glob("*.result.tmp")),
        }

    def shutdown(self, *, raise_on_failure: bool = True) -> None:
        if self._shutdown_complete:
            return
        alive = [item for item in self.processes if item.process.poll() is None]
        started = time.monotonic()
        for item in reversed(alive):
            item.process.terminate()
        deadline = started + 5.0
        pending = set(item.name for item in alive)
        while pending and time.monotonic() < deadline:
            now = time.monotonic()
            for item in alive:
                if item.name in pending and item.process.poll() is not None:
                    self.shutdown_durations[item.name] = now - started
                    pending.remove(item.name)
            time.sleep(0.025)
        timed_out = [item for item in alive if item.process.poll() is None]
        for item in timed_out:
            item.process.kill()
        for item in alive:
            item.process.wait(timeout=1.0)
            self.shutdown_durations.setdefault(item.name, time.monotonic() - started)
        self.shutdown_elapsed = time.monotonic() - started
        for item in self.processes:
            item.log_file.close()
        self._health.close()
        self._shutdown_complete = True
        bad_exit = [item for item in alive if item not in timed_out and item.process.returncode != 0]
        failure = bool(timed_out or bad_exit or self.shutdown_elapsed >= 5.0)
        if failure and raise_on_failure:
            details = self.log_text()
            raise AssertionError(
                f"process shutdown failure: timed_out={[x.name for x in timed_out]} "
                f"bad_exit={[(x.name, x.process.returncode) for x in bad_exit]} "
                f"elapsed={self.shutdown_elapsed:.3f}s\n{details}"
            )

    def __enter__(self) -> Phase9ProcessHarness:
        self.start()
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        try:
            self.shutdown()
        except Exception:
            if exc is None:
                raise


__all__ = ["ManagedProcess", "Phase9ProcessHarness"]
