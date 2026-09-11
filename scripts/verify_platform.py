"""Fail-fast mission preflight checks for the reference Raspberry Pi platform."""

from __future__ import annotations

import argparse
import platform
import shutil
import subprocess
import sys
from pathlib import Path

import psutil


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tether", default="eth0")
    parser.add_argument("--local-ip", default="192.168.50.2")
    parser.add_argument("--peer-ip", default="192.168.50.1")
    parser.add_argument("--camera", action="append", default=[])
    args = parser.parse_args()
    failures: list[str] = []

    def fail(message: str) -> None:
        print(f"FAIL: {message}")
        failures.append(message)

    if platform.machine() != "aarch64":
        fail(f"architecture is {platform.machine()}, expected aarch64")
    model = next(
        (
            path.read_text(encoding="ascii").rstrip("\x00\n")
            for path in (Path("/sys/firmware/devicetree/base/model"), Path("/proc/device-tree/model"))
            if path.is_file()
        ),
        "unavailable",
    )
    if "raspberry pi 5" not in model.lower():
        fail(f"hardware model is {model!r}, expected Raspberry Pi 5")
    release = Path("/etc/os-release").read_text(encoding="utf-8") if Path("/etc/os-release").exists() else ""
    if "ID=ubuntu" not in release or 'VERSION_ID="24.04"' not in release:
        fail("operating system is not Ubuntu 24.04")
    if sys.version_info[:2] != (3, 12):
        fail(f"Python is {platform.python_version()}, expected 3.12.x")
    if not Path("/run/systemd/system").exists():
        fail("systemd is not the active init/supervisor")

    temperature = Path("/sys/class/thermal/thermal_zone0/temp")
    if temperature.exists():
        temp_c = int(temperature.read_text().strip()) / 1000
        print(f"OK: CPU temperature {temp_c:.1f} C")
        if temp_c >= 80:
            print(f"WARNING: CPU temperature is {temp_c:.1f} C (diagnostic only)")
    throttled = Path("/sys/devices/platform/soc/soc:firmware/get_throttled")
    if throttled.exists() and throttled.read_text().strip() not in {"0", "0x0"}:
        print("WARNING: OS reports thermal throttling (diagnostic only)")
    if psutil.virtual_memory().available < 512 * 1024**2:
        fail("available memory is below 512 MiB")
    if shutil.disk_usage("/").free < 2 * 1024**3:
        fail("root filesystem has less than 2 GiB free")
    interface = Path(f"/sys/class/net/{args.tether}")
    if not interface.exists():
        fail(f"tether interface {args.tether!r} is unavailable")
    else:
        operstate = (interface / "operstate").read_text(encoding="ascii").strip().lower()
        if operstate != "up":
            fail(f"tether interface {args.tether!r} is {operstate}, expected up")
        address = subprocess.run(
            ["ip", "-json", "address", "show", "dev", args.tether],
            capture_output=True,
            text=True,
            check=False,
        )
        if f'"local":"{args.local_ip}"' not in address.stdout.replace(" ", ""):
            fail(f"tether interface {args.tether!r} does not own {args.local_ip}")
        peer = subprocess.run(
            ["ping", "-n", "-c", "1", "-W", "1", args.peer_ip],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        if peer.returncode != 0:
            fail(f"tether peer {args.peer_ip} is unreachable")
    for camera in args.camera:
        if not Path(camera).exists():
            fail(f"camera device is missing: {camera}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
