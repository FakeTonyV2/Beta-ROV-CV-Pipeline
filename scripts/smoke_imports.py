"""Verify the installed runtime and generated bindings are importable."""

import sys
from importlib import import_module
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "generated" / "python"))
sys.path.insert(0, str(ROOT / "src"))

MODULES = (
    "google.protobuf",
    "zmq",
    "mcap",
    "mcap_protobuf.decoder",
    "numpy",
    "psutil",
    "pydantic",
    "yaml",
    "purdue_rov.cv.v1.envelope_pb2",
    "purdue_rov_cv.config",
    "purdue_rov_cv.camera",
    "purdue_rov_cv.frame_buffer",
    "purdue_rov_cv.messaging",
    "purdue_rov_cv.module_runner",
    "purdue_rov_cv.modules",
    "purdue_rov_cv.runtime",
    "purdue_rov_cv.video",
)


def main() -> None:
    for name in MODULES:
        import_module(name)
        print(f"ok: {name}")
    gi = import_module("gi")
    gi.require_version("Gst", "1.0")
    gi.require_version("GstRtp", "1.0")
    gst = import_module("gi.repository.Gst")
    import_module("gi.repository.GstRtp")
    gst.init(None)
    print(f"ok: gi.repository.Gst ({gst.version_string()})")


if __name__ == "__main__":
    main()
