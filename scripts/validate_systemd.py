#!/usr/bin/env python3
"""Validate the invariant policy and optional native syntax of project units."""

from __future__ import annotations

import argparse
import shutil
import subprocess
import tempfile
from pathlib import Path


def validate_units(directory: Path) -> list[str]:
    failures: list[str] = []
    services = sorted(directory.glob("*.service"))
    required = {
        "Type=simple",
        "Restart=on-failure",
        "RestartSec=2",
        "TimeoutStopSec=5",
        "KillSignal=SIGTERM",
        "RestartPreventExitStatus=78",
        "RuntimeDirectoryPreserve=yes",
        "StartLimitIntervalSec=60",
        "StartLimitBurst=5",
    }
    for path in services:
        text = path.read_text(encoding="utf-8")
        missing = sorted(item for item in required if item not in text)
        failures.extend(f"{path.name}: missing {item}" for item in missing)
        unit, _, service = text.partition("[Service]")
        if "StartLimitIntervalSec=60" not in unit or "StartLimitBurst=5" not in unit:
            failures.append(f"{path.name}: StartLimit directives must be in [Unit]")
        if "StartLimitIntervalSec=60" in service or "StartLimitBurst=5" in service:
            failures.append(f"{path.name}: StartLimit directives found in [Service]")
        if "ExecStart=/opt/purdue-rov-cv/.venv/bin/" not in text:
            failures.append(f"{path.name}: ExecStart is not an explicit deployed path")
        if "/etc/purdue-rov-cv/mission.yaml" not in text:
            failures.append(f"{path.name}: production configuration path is absent")
    for template in ("purdue-cv-camera@.service", "purdue-cv-module@.service", "purdue-cv-video-receiver@.service"):
        if not (directory / template).is_file():
            failures.append(f"missing template {template}")
    return failures


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", type=Path, nargs="?", default=Path("systemd"))
    parser.add_argument("--native", action="store_true", help="also run systemd-analyze verify when available")
    args = parser.parse_args()
    failures = validate_units(args.directory)
    if args.native:
        analyzer = shutil.which("systemd-analyze")
        if analyzer is None:
            failures.append("systemd-analyze is unavailable")
        else:
            # Verify syntax from a Linux-native temporary directory. A source
            # checkout on a Windows mount has synthetic executable/world-write
            # bits, and the explicit /opt binaries correctly do not exist on a
            # developer host. Structural validation above covers the real path.
            with tempfile.TemporaryDirectory(prefix="purdue-cv-systemd-") as temporary:
                staged: list[str] = []
                for source in sorted(args.directory.glob("*")):
                    if not source.is_file():
                        continue
                    target = Path(temporary) / source.name
                    content = source.read_text(encoding="utf-8")
                    lines = [
                        "ExecStart=/bin/true" if line.startswith("ExecStart=") else line
                        for line in content.splitlines()
                    ]
                    target.write_text("\n".join(lines) + "\n", encoding="utf-8")
                    target.chmod(0o644)
                    staged.append(str(target))
                result = subprocess.run(
                    [analyzer, "verify", *staged],
                    capture_output=True,
                    text=True,
                    check=False,
                )
            if result.returncode:
                failures.append(result.stderr.strip() or result.stdout.strip())
    for failure in failures:
        print(f"FAIL: {failure}")
    if not failures:
        print(f"PASS: validated {len(list(args.directory.glob('*.service')))} service units")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
