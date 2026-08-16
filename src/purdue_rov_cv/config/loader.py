"""Authoritative, side-effect-free YAML loading and configuration resolution."""

from __future__ import annotations

import copy
import hashlib
import json
import os
from collections.abc import Hashable, Mapping
from pathlib import Path

import yaml
from pydantic import ValidationError
from yaml.constructor import ConstructorError
from yaml.resolver import BaseResolver

from .issues import (
    ConfigFileError,
    ConfigIssue,
    ConfigIssueKind,
    ConfigSchemaError,
    ConfigStaticValidationError,
    ConfigYamlError,
)
from .models import AppConfig
from .validation import validate_static_config

DEFAULT_CONFIG_PATH = Path("/etc/purdue-rov-cv/mission.yaml")
CONFIG_PATH_ENV = "PURDUE_ROV_CV_CONFIG"
LOG_LEVEL_ENV = "PURDUE_ROV_CV_LOG_LEVEL"
_ALLOWED_ENVIRONMENT = frozenset({CONFIG_PATH_ENV, LOG_LEVEL_ENV})


class _UniqueKeySafeLoader(yaml.SafeLoader):
    """Safe YAML loader that rejects ambiguous duplicate mapping keys."""


def _construct_unique_mapping(
    loader: _UniqueKeySafeLoader,
    node: yaml.MappingNode,
    deep: bool = False,
) -> dict[object, object]:
    loader.flatten_mapping(node)
    mapping: dict[object, object] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if not isinstance(key, Hashable):
            raise ConstructorError(
                "while constructing a mapping",
                node.start_mark,
                "mapping keys must be scalar values",
                key_node.start_mark,
            )
        if key in mapping:
            raise ConstructorError(
                "while constructing a mapping",
                node.start_mark,
                f"duplicate key {key!r}",
                key_node.start_mark,
            )
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


_UniqueKeySafeLoader.add_constructor(BaseResolver.DEFAULT_MAPPING_TAG, _construct_unique_mapping)


def resolve_config_path(path: str | Path | None = None, environ: Mapping[str, str] | None = None) -> Path:
    """Use an explicit path, then PURDUE_ROV_CV_CONFIG, then production default."""
    environment = os.environ if environ is None else environ
    if path is not None:
        return Path(path)
    return Path(environment.get(CONFIG_PATH_ENV, DEFAULT_CONFIG_PATH))


def _environment_issues(environ: Mapping[str, str]) -> list[ConfigIssue]:
    return [
        ConfigIssue(ConfigIssueKind.CONFIG_ENV_INVALID, name, "environment override is not supported")
        for name in sorted(environ)
        if name.startswith("PURDUE_ROV_CV_") and name not in _ALLOWED_ENVIRONMENT
    ]


def _pydantic_issues(error: ValidationError) -> list[ConfigIssue]:
    return [
        ConfigIssue(
            ConfigIssueKind.CONFIG_SCHEMA_INVALID,
            ".".join(str(part) for part in detail["loc"]),
            detail["msg"],
            {"type": detail["type"]},
        )
        for detail in error.errors()
    ]


def parse_config_data(data: object, *, log_level_override: str | None = None) -> AppConfig:
    """Parse mapping data independently from files and CLI state."""
    if data is None:
        raise ConfigYamlError([ConfigIssue(ConfigIssueKind.CONFIG_EMPTY, "<root>", "YAML document is empty")])
    if not isinstance(data, dict):
        raise ConfigYamlError(
            [ConfigIssue(ConfigIssueKind.CONFIG_ROOT_INVALID, "<root>", "YAML root must be a mapping")]
        )
    candidate = copy.deepcopy(data)
    if log_level_override is not None:
        diagnostics = candidate.get("diagnostics")
        if not isinstance(diagnostics, dict):
            # Let Pydantic report the normal field path when the section itself is malformed.
            diagnostics = {}
            candidate["diagnostics"] = diagnostics
        diagnostics["log_level"] = log_level_override
    try:
        config = AppConfig.model_validate(candidate)
    except ValidationError as error:
        raise ConfigSchemaError(_pydantic_issues(error)) from error
    static_issues = validate_static_config(config)
    if static_issues:
        raise ConfigStaticValidationError(static_issues)
    return config


def load_config(path: str | Path | None = None, *, environ: Mapping[str, str] | None = None) -> AppConfig:
    """Read, parse, strictly validate, and semantically validate mission YAML."""
    environment = os.environ if environ is None else environ
    environment_issues = _environment_issues(environment)
    if environment_issues:
        raise ConfigSchemaError(environment_issues)
    config_path = resolve_config_path(path, environment)
    try:
        text = config_path.read_text(encoding="utf-8")
    except FileNotFoundError as error:
        raise ConfigFileError(
            [ConfigIssue(ConfigIssueKind.CONFIG_FILE_NOT_FOUND, str(config_path), "configuration file does not exist")]
        ) from error
    except OSError as error:
        raise ConfigFileError(
            [ConfigIssue(ConfigIssueKind.CONFIG_FILE_READ_ERROR, str(config_path), str(error))]
        ) from error
    try:
        data = yaml.load(text, Loader=_UniqueKeySafeLoader)
    except yaml.YAMLError as error:
        raise ConfigYamlError(
            [ConfigIssue(ConfigIssueKind.CONFIG_YAML_INVALID, str(config_path), str(error))]
        ) from error
    return parse_config_data(data, log_level_override=environment.get(LOG_LEVEL_ENV))


def config_hash(config: AppConfig) -> str:
    """Return a stable SHA-256 hash of the validated, JSON-safe configuration."""
    canonical = json.dumps(config.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
