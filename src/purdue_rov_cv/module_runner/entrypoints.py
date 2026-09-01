"""Installed module-runner service boundary and canonical exit translation."""

from __future__ import annotations

import argparse
import importlib
import sys
from pathlib import Path
from typing import Never

import zmq

from purdue_rov_cv.config.issues import ConfigurationError
from purdue_rov_cv.config.loader import load_config
from purdue_rov_cv.modules.base import CVModule
from purdue_rov_cv.runtime.exit_codes import ExitCode
from purdue_rov_cv.runtime.json_logging import configure_json_logger
from purdue_rov_cv.runtime.metrics import RuntimeMetrics
from purdue_rov_cv.runtime.publisher import PublisherSequence
from purdue_rov_cv.wire.errors import ErrorCode

from .artifacts import ArtifactValidationError, ArtifactValidator
from .frame_source import SharedMemoryFrameSource
from .service import ModuleInitializationError, ModuleRunnerService


class _ModuleArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> Never:
        self.print_usage(sys.stderr)
        self.exit(ExitCode.INVALID_ARGUMENTS, f"{self.prog}: error: {message}\n")


def load_module(module_path: str) -> CVModule:
    module_name, separator, class_name = module_path.rpartition(".")
    if not separator:
        raise ValueError("module_class must be a fully qualified class name")
    imported = importlib.import_module(module_name)
    candidate = getattr(imported, class_name)
    if not isinstance(candidate, type) or not issubclass(candidate, CVModule):
        raise TypeError("configured module_class is not a CVModule subclass")
    return candidate()


def _parser() -> _ModuleArgumentParser:
    parser = _ModuleArgumentParser(prog="purdue-cv-module-runner")
    parser.add_argument("--task", required=True, help="configured task ID to host in this process")
    parser.add_argument(
        "--config",
        type=Path,
        help="mission YAML; defaults to PURDUE_ROV_CV_CONFIG or /etc/purdue-rov-cv/mission.yaml",
    )
    parser.add_argument(
        "--shared-memory-name",
        help="camera-owned POSIX shared-memory object name; defaults to purdue_rov_cv_<camera_id>",
    )
    return parser


def module_runner_main(argv: list[str] | None = None) -> ExitCode:
    args = _parser().parse_args(argv)
    config = load_config(args.config)
    if args.task not in config.tasks:
        raise ValueError(f"unknown configured task: {args.task}")
    task = config.tasks[args.task]
    module = load_module(task.module_class)
    publisher_sequence = PublisherSequence()
    logger = configure_json_logger(
        device_id=config.device.device_id,
        process_name="purdue-cv-module-runner",
        source_id=args.task,
        publisher_session_id=publisher_sequence.session_id,
    )
    if module.requires_artifact:
        try:
            ArtifactValidator().validate(config, args.task)
        except ArtifactValidationError as error:
            for issue in error.issues:
                logger.log(
                    "ERROR",
                    issue.code,
                    issue.message,
                    context={"path": issue.path, "state": "ERROR", "task_id": args.task},
                )
            raise
    metrics = RuntimeMetrics()
    source = SharedMemoryFrameSource(
        args.shared_memory_name or f"purdue_rov_cv_{task.input_camera}",
        camera_id=task.input_camera,
        expected_slot_capacity_bytes=config.cameras[task.input_camera].slot_capacity_bytes,
        metrics=metrics,
    )
    service = ModuleRunnerService(
        config,
        args.task,
        module,
        source,
        logger=logger,
        metrics=metrics,
        publisher_sequence=publisher_sequence,
        install_signals=True,
    )
    return service.run()


def module_runner_entrypoint(argv: list[str] | None = None) -> int:
    try:
        return int(module_runner_main(argv))
    except SystemExit:
        raise
    except ConfigurationError as error:
        for issue in error.issues:
            print(f"{error.error_code} {issue.code} {issue.path}: {issue.message}", file=sys.stderr)
        if any(issue.code == "CONFIG_FILE_READ_ERROR" for issue in error.issues):
            return int(ExitCode.IO_FAILURE)
        return int(ExitCode.INVALID_CONFIGURATION)
    except ArtifactValidationError as error:
        for issue in error.issues:
            print(f"{ErrorCode.CONFIG_INVALID} {issue.code} {issue.path}: {issue.message}", file=sys.stderr)
        return int(ExitCode.INVALID_CONFIGURATION)
    except ModuleInitializationError as error:
        code = ErrorCode.MODEL_LOAD_FAILED if error.artifact_related else ErrorCode.INTERNAL_ERROR
        print(f"{code} <module-runner>: {error}", file=sys.stderr)
        return int(ExitCode.INVALID_CONFIGURATION if error.artifact_related else ExitCode.INTERNAL_SOFTWARE_FAILURE)
    except (ImportError, AttributeError, TypeError, ValueError) as error:
        print(f"{ErrorCode.CONFIG_INVALID} <module-runner>: {type(error).__name__}: {error}", file=sys.stderr)
        return int(ExitCode.INVALID_CONFIGURATION)
    except zmq.ZMQError as error:
        print(f"{ErrorCode.INTERNAL_ERROR} <module-runner>: ZMQError: {error}", file=sys.stderr)
        return int(ExitCode.TEMPORARY_FAILURE)
    except OSError as error:
        print(f"{ErrorCode.INTERNAL_ERROR} <module-runner>: {type(error).__name__}: {error}", file=sys.stderr)
        return int(ExitCode.IO_FAILURE)
    except Exception as error:
        print(f"{ErrorCode.INTERNAL_ERROR} <module-runner>: {type(error).__name__}: {error}", file=sys.stderr)
        return int(ExitCode.INTERNAL_SOFTWARE_FAILURE)


if __name__ == "__main__":
    raise SystemExit(module_runner_entrypoint())


__all__ = ["load_module", "module_runner_entrypoint", "module_runner_main"]
