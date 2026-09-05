# Beta ROV CV Pipeline

The repository includes the Phase 1–3 wire, configuration, and runtime contracts,
Phase 4's real ZeroMQ data broker and control router, and Phase 5's isolated
production module runner and Echo reference task.
It also includes Phase 6's simulated GStreamer camera service and canonical
lock-free shared-memory triple buffer, plus Phase 7's per-camera surface RTP
receiver, validated `FrameIndex` subscriber, and bounded frame-correlation
fan-out. Surface-enabled camera services use the same source-boundary identity
for shared memory, H.264/RTP, and brokered `FrameIndex` messages.

Reference platforms:

- Raspberry Pi 5: ARM64, Ubuntu Server 24.04 LTS, Python 3.12.x, systemd, tethered Ethernet, GStreamer >= 1.22, active cooling.
- Surface computer: x86-64, Ubuntu 24.04 LTS, Python 3.12.x, GStreamer >= 1.22.

## Bootstrap

On Ubuntu 24.04, install host packages and then create the isolated environment:

```bash
sudo ./scripts/setup_system_deps.sh
./scripts/setup_venv.sh
source .venv/bin/activate
```

The Python dependency set is locked in `requirements.lock`. `pip check` and the
import smoke test are part of CI. The venv uses `--system-site-packages` only so
Python 3.12 can load Ubuntu 24.04's ABI-matched `python3-gi` and
`python3-gst-1.0`; PyGObject is intentionally not installed from PyPI. Protobuf
generation is pinned by `.protoc-version`:

```bash
./scripts/generate_proto.sh
```

## Linting with Ruff

Run the linter from the repository root after activating the virtual environment:

```bash
.venv/bin/python -m ruff check src tests
```

Ruff can apply safe automatic fixes with:

```bash
.venv/bin/python -m ruff check --fix src tests
```

Review the resulting diff, then rerun the first command to confirm the tree is clean.

Run the configured static type check with:

```bash
.venv/bin/python -m mypy
```

Check formatting with the same command used by CI:

```bash
.venv/bin/python -m ruff format --check src tests
```

Run all tests, or the Phase 4/5 process integrations specifically, with:

```bash
.venv/bin/python -m pytest
.venv/bin/python -m pytest tests/integration/test_phase4_processes.py
.venv/bin/python -m pytest tests/integration/test_phase5_module_runner_processes.py
.venv/bin/python -m pytest tests/integration/test_phase6_processes.py
.venv/bin/python -m pytest tests/integration/test_phase7_gstreamer.py
```

The installed service entry points load the mission configuration and
translate failures to the supervisor exit-code contract:

```bash
purdue-cv-broker --config config/mission.yaml
purdue-cv-control-router --config config/mission.yaml
purdue-cv-module-runner --task gate_detection --config config/mission.yaml
purdue-cv-camera --camera front_camera --config config/mission.yaml
purdue-cv-video-receiver --camera front_camera --config config/mission.yaml
```

See [docs/broker-control-routing.md](docs/broker-control-routing.md) for control
framing, registration, heartbeat, cache, metric, and socket-ownership details.
See [docs/module-runner.md](docs/module-runner.md) for module, shared-memory,
threading, publication, watchdog, and shutdown contracts.
See [docs/shared-memory-frame-buffer.md](docs/shared-memory-frame-buffer.md) for
the exact 128-byte header, ownership/recovery policy, and simulated camera
lifecycle.
See [docs/surface-video-receiver.md](docs/surface-video-receiver.md) for the
RTP identity, correlation, rebuild, fan-out, metrics, and deferred-recorder
contracts.

Before a mission, run the Pi preflight with the deployed camera paths, for example:

```bash
python scripts/verify_platform.py --tether eth0 --camera /dev/video0
```

The platform preflight exits non-zero for unsafe thermal, memory, storage, camera,
or tether conditions. The platform-specific checks are intentionally separate from
the laptop test suite.
