"""Verify Python dependencies and discover the GStreamer executable."""

import shutil
import subprocess
import sys


def main() -> int:
    if sys.version_info[:2] != (3, 12):
        print(f"error: Python 3.12.x required, found {sys.version}", file=sys.stderr)
        return 1
    result = subprocess.run([sys.executable, "-m", "pip", "check"], check=False)
    if result.returncode:
        return result.returncode
    gst = shutil.which("gst-launch-1.0")
    if gst is None:
        print("error: gst-launch-1.0 not found", file=sys.stderr)
        return 1
    print(subprocess.check_output([gst, "--version"], text=True).splitlines()[0])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
