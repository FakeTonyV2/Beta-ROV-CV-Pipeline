"""Verify the installed runtime and generated bindings are importable."""

from importlib import import_module
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "generated" / "python"))
sys.path.insert(0, str(ROOT / "src"))

MODULES = ("google.protobuf", "zmq", "mcap", "mcap_protobuf.decoder", "numpy", "psutil", "pydantic", "yaml", "purdue_rov.cv.v1.envelope_pb2", "purdue_rov_cv.config")


def main() -> None:
    for name in MODULES:
        import_module(name)
        print(f"ok: {name}")


if __name__ == "__main__":
    main()
