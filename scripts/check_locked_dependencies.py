"""Validate only the distributions owned by requirements.lock.

Deployment environments expose Ubuntu's GStreamer Python bindings through
``--system-site-packages``. An environment-wide ``pip check`` also validates
unrelated apt packages and can therefore reject an otherwise valid deployment.
"""

from __future__ import annotations

import argparse
import sys
from importlib.metadata import PackageNotFoundError, distribution
from pathlib import Path

from packaging.requirements import Requirement
from packaging.utils import canonicalize_name
from packaging.version import Version


def parse_lock(path: Path) -> dict[str, Requirement]:
    locked: dict[str, Requirement] = {}
    for line_number, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        requirement = Requirement(line)
        if requirement.url is not None or len(requirement.specifier) != 1:
            raise ValueError(f"{path}:{line_number}: lock entries must pin one exact version")
        specifier = next(iter(requirement.specifier))
        if specifier.operator != "==" or "*" in specifier.version:
            raise ValueError(f"{path}:{line_number}: lock entries must use exact == pins")
        name = canonicalize_name(requirement.name)
        if name in locked:
            if locked[name].specifier != requirement.specifier or locked[name].extras != requirement.extras:
                raise ValueError(f"{path}:{line_number}: conflicting duplicate lock entry for {name}")
            continue
        locked[name] = requirement
    if not locked:
        raise ValueError(f"{path}: no locked dependencies found")
    return locked


def check_locked_dependencies(path: Path) -> list[str]:
    """Return missing, version-mismatched, or unsatisfied locked dependencies."""
    locked = parse_lock(path)
    errors: list[str] = []
    installed = {}
    for name, requirement in locked.items():
        try:
            package = distribution(name)
        except PackageNotFoundError:
            errors.append(f"{name} is missing (required {requirement.specifier})")
            continue
        installed[name] = package
        actual = Version(package.version)
        if actual not in requirement.specifier:
            errors.append(f"{name}=={actual} is installed; lock requires {requirement.specifier}")

    for name, package in installed.items():
        for raw_requirement in package.requires or ():
            dependency = Requirement(raw_requirement)
            if dependency.marker is not None and not dependency.marker.evaluate():
                continue
            dep_name = canonicalize_name(dependency.name)
            try:
                actual = Version(distribution(dep_name).version)
            except PackageNotFoundError:
                errors.append(f"{name} requires {dependency}, but {dep_name} is missing")
                continue
            if actual not in dependency.specifier:
                errors.append(f"{name} requires {dependency}, but {dep_name}=={actual} is installed")
    return errors


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("lock", nargs="?", type=Path, default=Path("requirements.lock"))
    args = parser.parse_args(argv)
    try:
        errors = check_locked_dependencies(args.lock)
    except (OSError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    if errors:
        for error in errors:
            print(f"error: {error}", file=sys.stderr)
        return 1
    print(f"locked dependencies are satisfied: {args.lock}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
