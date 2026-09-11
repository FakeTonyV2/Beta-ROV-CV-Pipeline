"""Preflight orchestration independent of terminal and process boundaries."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from purdue_rov_cv.config.issues import ConfigurationError
from purdue_rov_cv.config.loader import config_hash, load_config

from .checks import CHECK_SPECS, CheckResult, CheckStatus, EvidenceKind, PreflightReport, evaluate_checks, make_report
from .probes import PreflightProbe


def run_preflight(
    config_path: Path,
    probe: PreflightProbe,
    *,
    camera_duration_seconds: float = 10.0,
    now: datetime | None = None,
) -> PreflightReport:
    if camera_duration_seconds < 10.0:
        raise ValueError("the required simultaneous-camera check cannot be shorter than ten seconds")
    try:
        config = load_config(config_path, environ={})
    except ConfigurationError as error:
        detail = "; ".join(f"{issue.code} {issue.path}: {issue.message}" for issue in error.issues)
        if any(issue.code in {"CONFIG_FILE_NOT_FOUND", "CONFIG_FILE_READ_ERROR"} for issue in error.issues):
            return make_report((), now=now, execution_error=detail)
        spec = CHECK_SPECS[0]
        check = CheckResult(
            spec.check_id,
            spec.name,
            spec.probe,
            spec.inputs,
            spec.calculation,
            CheckStatus.FAIL,
            True,
            detail,
            {"path": str(config_path)},
            {"strict_unknown_fields": True},
            EvidenceKind.OBSERVED,
        )
        return make_report((check,), now=now)
    configuration_sha256 = config_hash(config)
    try:
        snapshot = probe.collect(config, camera_duration_seconds=camera_duration_seconds)
        checks = evaluate_checks(config, snapshot)
    except Exception as error:
        return make_report(
            (),
            now=now,
            execution_error=f"{type(error).__name__}: {error}",
            configuration_sha256=configuration_sha256,
        )
    return make_report(checks, now=now, configuration_sha256=configuration_sha256)


__all__ = ["run_preflight"]
