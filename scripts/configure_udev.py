"""Inspect UVC identities and generate narrowly scoped Purdue ROV udev rules."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path

from purdue_rov_cv.config import load_config
from purdue_rov_cv.config.issues import ConfigurationError
from purdue_rov_cv.config.models import AppConfig, CameraAdapter, CameraPathKind, CameraResolutionTier

RULE_PATH = Path("/etc/udev/rules.d/99-purdue-rov-cv-cameras.rules")


@dataclass(frozen=True, slots=True)
class Candidate:
    node: str
    serial: str
    id_path: str
    vendor_id: str
    product_id: str
    model: str
    video_index: str = "0"
    by_id: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class Assignment:
    camera_id: str
    candidate: str
    resolution_tier: str
    stable_property: str
    symlink: str
    video_index: str
    physical_port_label: str | None
    rule: str | None


def _run(command: Sequence[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, check=False, capture_output=True, text=True, timeout=8)


def _properties(output: str) -> dict[str, str]:
    return {key: value for line in output.splitlines() if "=" in line for key, value in (line.split("=", 1),)}


def enumerate_candidates(
    *,
    dev_root: Path = Path("/dev"),
    sys_class_root: Path = Path("/sys/class/video4linux"),
    runner: Callable[[Sequence[str]], subprocess.CompletedProcess[str]] = _run,
) -> tuple[Candidate, ...]:
    by_node: dict[str, list[str]] = {}
    by_id_dir = dev_root / "v4l" / "by-id"
    if by_id_dir.is_dir():
        for link in sorted(by_id_dir.iterdir()):
            try:
                target_node = str(link.resolve(strict=True))
            except (OSError, RuntimeError):
                continue
            by_node.setdefault(target_node, []).append(str(link))
    candidates: list[Candidate] = []
    for node in sorted(dev_root.glob("video[0-9]*")):
        result = runner(("udevadm", "info", "--query=property", "--name", str(node)))
        if result.returncode != 0:
            continue
        props = _properties(result.stdout)
        capabilities = props.get("ID_V4L_CAPABILITIES", "")
        if ":capture:" not in capabilities:
            continue
        try:
            video_index = (sys_class_root / node.name / "index").read_text(encoding="ascii").strip()
        except OSError:
            video_index = props.get("ID_V4L_INDEX", "")
        if not video_index.isdecimal():
            continue
        candidates.append(
            Candidate(
                str(node),
                props.get("ID_SERIAL_SHORT", ""),
                props.get("ID_PATH", ""),
                props.get("ID_VENDOR_ID", ""),
                props.get("ID_MODEL_ID", ""),
                props.get("ID_MODEL", ""),
                video_index,
                tuple(by_node.get(str(node.resolve()), ())),
            )
        )
    return tuple(candidates)


def _quoted(value: str) -> str:
    if any(character in value for character in {'"', "\n", "\r"}):
        raise ValueError("udev match values must not contain quotes or newlines")
    return value


def build_assignments(config: AppConfig, candidates: Sequence[Candidate]) -> tuple[Assignment, ...]:
    cameras = config.cameras
    assignments: list[Assignment] = []
    claimed_nodes: dict[str, str] = {}
    claimed_physical_devices: dict[str, str] = {}
    for camera_id, camera in sorted(cameras.items()):
        if camera.adapter is not CameraAdapter.V4L2:
            continue
        assert camera.device_path is not None
        assert camera.device_path_kind is not None
        assert camera.resolution_tier is not None
        if camera.device_path_kind is CameraPathKind.BY_ID:
            matches = [candidate for candidate in candidates if str(camera.device_path) in candidate.by_id]
            if len(matches) != 1:
                raise ValueError(f"{camera_id}: configured by-id path matched {len(matches)} candidates")
            candidate = matches[0]
            assignment = Assignment(
                camera_id,
                candidate.node,
                CameraResolutionTier.BY_ID.value,
                candidate.serial or Path(str(camera.device_path)).name,
                str(camera.device_path),
                candidate.video_index,
                None,
                None,
            )
        else:
            identity = camera.stable_identity or ""
            matches = [candidate for candidate in candidates if candidate.id_path == identity]
            if len(matches) != 1:
                raise ValueError(
                    f"{camera_id}: stable_identity matched {len(matches)} capture devices; "
                    "exactly one capture-capable interface is required"
                )
            candidate = matches[0]
            if candidate.serial or candidate.by_id:
                strongest = candidate.by_id[0] if candidate.by_id else f"USB serial {candidate.serial}"
                raise ValueError(
                    f"{camera_id}: fallback is forbidden because stronger by-id identity is available: {strongest}"
                )
            match_parts = [
                'SUBSYSTEM=="video4linux"',
                f'ENV{{ID_PATH}}=="{_quoted(identity)}"',
                f'ATTR{{index}}=="{_quoted(candidate.video_index)}"',
            ]
            if candidate.vendor_id:
                match_parts.append(f'ENV{{ID_VENDOR_ID}}=="{_quoted(candidate.vendor_id)}"')
            if candidate.product_id:
                match_parts.append(f'ENV{{ID_MODEL_ID}}=="{_quoted(candidate.product_id)}"')
            actions = [
                f'ENV{{PURDUE_ROV_CV_CAMERA_ID}}="{camera_id}"',
                f'ENV{{PURDUE_ROV_CV_TIER}}="{camera.resolution_tier.value}"',
                f'ENV{{PURDUE_ROV_CV_IDENTITY}}="{_quoted(identity)}"',
                f'SYMLINK+="purdue-rov-cv/{camera_id}"',
            ]
            if camera.resolution_tier is CameraResolutionTier.PHYSICAL_PORT:
                assert camera.physical_port_label is not None
                actions.insert(
                    -1,
                    f'ENV{{PURDUE_ROV_CV_PORT_LABEL}}="{_quoted(camera.physical_port_label)}"',
                )
            assignment = Assignment(
                camera_id,
                candidate.node,
                camera.resolution_tier.value,
                identity,
                str(camera.device_path),
                candidate.video_index,
                camera.physical_port_label,
                ", ".join((*match_parts, *actions)),
            )
        previous = claimed_nodes.get(assignment.candidate)
        if previous is not None:
            raise ValueError(
                f"physical capture device {assignment.candidate} is assigned to both {previous} and {camera_id}"
            )
        claimed_nodes[assignment.candidate] = camera_id
        physical_key = candidate.serial or candidate.id_path or next(iter(candidate.by_id), candidate.node)
        previous_physical = claimed_physical_devices.get(physical_key)
        if previous_physical is not None:
            raise ValueError(
                f"physical camera identity {physical_key!r} is assigned to both {previous_physical} and {camera_id}"
            )
        claimed_physical_devices[physical_key] = camera_id
        assignments.append(assignment)
    return tuple(assignments)


def render_rules(assignments: Sequence[Assignment]) -> str:
    rules = ["# Generated by scripts/configure_udev.py; do not edit by hand."]
    rules.extend(assignment.rule for assignment in assignments if assignment.rule is not None)
    return "\n".join(rules) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("config", type=Path)
    parser.add_argument("--install", action="store_true", help=f"write {RULE_PATH} and reload udev")
    parser.add_argument("--json", action="store_true", help="print machine-readable inspection output")
    args = parser.parse_args(argv)
    try:
        assignments = build_assignments(load_config(args.config, environ={}), enumerate_candidates())
        rules = render_rules(assignments)
        if args.install:
            RULE_PATH.write_text(rules, encoding="utf-8")
            for command in (
                ("udevadm", "control", "--reload-rules"),
                ("udevadm", "trigger", "--subsystem-match=video4linux"),
            ):
                result = _run(command)
                if result.returncode != 0:
                    raise OSError(result.stderr.strip() or f"{' '.join(command)} exited {result.returncode}")
        if args.json:
            print(json.dumps([asdict(item) for item in assignments], indent=2, sort_keys=True))
        else:
            for item in assignments:
                print(
                    f"{item.camera_id}: candidate={item.candidate} tier={item.resolution_tier} "
                    f"identity={item.stable_property} symlink={item.symlink}"
                )
            if not args.install:
                print("\nDry run; generated rules:\n" + rules)
        return 0
    except (ConfigurationError, OSError, ValueError) as error:
        print(f"configure_udev: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["Assignment", "Candidate", "build_assignments", "enumerate_candidates", "main", "render_rules"]
