# Beta ROV CV Pipeline

Phase 0 establishes the reproducible runtime contract for the modular CV pipeline.

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
import smoke test are part of CI. Protobuf generation is pinned by `.protoc-version`:

```bash
./scripts/generate_proto.sh
```

Before a mission, run the Pi preflight with the deployed camera paths, for example:

```bash
python scripts/verify_platform.py --tether eth0 --camera /dev/video0
```

The platform preflight exits non-zero for unsafe thermal, memory, storage, camera,
or tether conditions. The platform-specific checks are intentionally separate from
the laptop test suite.
