"""Verify Python dependencies and discover the GStreamer executable."""

import shutil
import subprocess
import sys
from importlib import import_module
from pathlib import Path

from check_locked_dependencies import check_locked_dependencies


def main() -> int:
    if sys.version_info[:2] != (3, 12):
        print(f"error: Python 3.12.x required, found {sys.version}", file=sys.stderr)
        return 1
    lock_path = Path(__file__).resolve().parents[1] / "requirements.lock"
    dependency_errors = check_locked_dependencies(lock_path)
    if dependency_errors:
        for error in dependency_errors:
            print(f"error: {error}", file=sys.stderr)
        return 1
    gst = shutil.which("gst-launch-1.0")
    if gst is None:
        print("error: gst-launch-1.0 not found", file=sys.stderr)
        return 1
    cli_version = subprocess.check_output([gst, "--version"], text=True).splitlines()[0]
    try:
        gi = import_module("gi")
        gi.require_version("Gst", "1.0")
        gst_module = import_module("gi.repository.Gst")
        gst_module.init(None)
    except (ImportError, AttributeError, ValueError) as error:
        print(
            "error: Python 3.12 cannot import the apt-provided PyGObject/GStreamer bindings; "
            "recreate .venv with scripts/setup_venv.sh after scripts/setup_system_deps.sh: "
            f"{type(error).__name__}: {error}",
            file=sys.stderr,
        )
        return 1
    print(cli_version)
    print(gst_module.version_string())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
