#!/usr/bin/env python3
"""Controlled child used only by the Phase 11 systemd acceptance harness."""

from __future__ import annotations

import argparse
import signal
from pathlib import Path
from threading import Event


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("run", "fail", "exit78", "fail-once"))
    parser.add_argument("--marker", type=Path)
    args = parser.parse_args()
    if args.mode == "fail":
        return 75
    if args.mode == "exit78":
        return 78
    if args.mode == "fail-once":
        if args.marker is None:
            raise SystemExit("--marker is required for fail-once")
        if not args.marker.exists():
            args.marker.write_text("failed once\n", encoding="ascii")
            return 75
    shutdown = Event()

    def stop(_signum: int, _frame: object) -> None:
        if args.marker is not None:
            args.marker.write_text("SIGTERM\n", encoding="ascii")
        shutdown.set()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    shutdown.wait()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
