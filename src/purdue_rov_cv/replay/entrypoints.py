"""Installed replay publisher and dedicated replay broker entry points."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Never

from purdue_rov_cv.config.loader import load_config
from purdue_rov_cv.messaging.broker import DataBrokerService
from purdue_rov_cv.runtime.exit_codes import ExitCode
from purdue_rov_cv.runtime.shutdown import ShutdownCoordinator, install_signal_handlers

from .structured import (
    DEFAULT_REPLAY_PUBLISHER_ENDPOINT,
    DEFAULT_REPLAY_SUBSCRIBER_ENDPOINT,
    IndexedMcapSource,
    ReplayRate,
    StructuredReplayer,
)
from .video import MatroskaVideoReplay


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> Never:
        self.print_usage(sys.stderr)
        self.exit(ExitCode.INVALID_ARGUMENTS, f"{self.prog}: error: {message}\n")


def replay_main(argv: list[str] | None = None) -> ExitCode:
    parser = _Parser(prog="purdue-cv-replay")
    parser.add_argument("recording", type=Path)
    parser.add_argument("--endpoint", default=DEFAULT_REPLAY_PUBLISHER_ENDPOINT)
    parser.add_argument("--config", type=Path, help="mission config used only for live-endpoint guarding")
    parser.add_argument("--allow-live-broker", action="store_true")
    parser.add_argument("--rate", default="1", choices=("0.25", "0.5", "1", "2", "max"))
    parser.add_argument("--start-time-ns", type=int)
    parser.add_argument("--end-time-ns", type=int)
    args = parser.parse_args(argv)
    live_endpoints: frozenset[str] = frozenset()
    if args.config is not None:
        config = load_config(args.config)
        live_endpoints = frozenset(
            {
                config.messaging.broker.publisher_endpoint,
                config.messaging.broker.subscriber_endpoint,
            }
        )
    elif args.endpoint != DEFAULT_REPLAY_PUBLISHER_ENDPOINT and not args.allow_live_broker:
        raise ValueError("a non-default endpoint requires --config for safety or --allow-live-broker")
    replay = StructuredReplayer(
        IndexedMcapSource(args.recording),
        endpoint=args.endpoint,
        rate=ReplayRate.parse(args.rate),
        live_endpoints=live_endpoints,
        allow_live_broker=args.allow_live_broker,
    )
    coordinator = ShutdownCoordinator(token=replay.shutdown)
    install_signal_handlers(coordinator)
    replay.run(start_time=args.start_time_ns, end_time=args.end_time_ns)
    return ExitCode.CLEAN_SHUTDOWN


def replay_broker_main(argv: list[str] | None = None) -> ExitCode:
    parser = _Parser(prog="purdue-cv-replay-broker")
    parser.add_argument("--publisher-endpoint", default=DEFAULT_REPLAY_PUBLISHER_ENDPOINT)
    parser.add_argument("--subscriber-endpoint", default=DEFAULT_REPLAY_SUBSCRIBER_ENDPOINT)
    args = parser.parse_args(argv)
    DataBrokerService(
        args.publisher_endpoint,
        args.subscriber_endpoint,
        install_signals=True,
    ).run()
    return ExitCode.CLEAN_SHUTDOWN


def video_replay_main(argv: list[str] | None = None) -> ExitCode:
    parser = _Parser(prog="purdue-cv-video-replay")
    parser.add_argument("recording", type=Path)
    parser.add_argument("--rate", default="1", choices=("0.25", "0.5", "1", "2", "max"))
    args = parser.parse_args(argv)
    replay = MatroskaVideoReplay(args.recording, lambda _frame: None, rate=ReplayRate.parse(args.rate))
    coordinator = ShutdownCoordinator(token=replay.shutdown)
    install_signal_handlers(coordinator)
    replay.run()
    return ExitCode.CLEAN_SHUTDOWN


def replay_entrypoint(argv: list[str] | None = None) -> int:
    try:
        return int(replay_main(argv))
    except SystemExit:
        raise
    except (OSError, ValueError, RuntimeError) as error:
        print(f"replay failed: {error}", file=sys.stderr)
        return int(ExitCode.IO_FAILURE if isinstance(error, OSError) else ExitCode.INVALID_ARGUMENTS)


def replay_broker_entrypoint(argv: list[str] | None = None) -> int:
    try:
        return int(replay_broker_main(argv))
    except SystemExit:
        raise
    except Exception as error:
        print(f"replay broker failed: {error}", file=sys.stderr)
        return int(ExitCode.INTERNAL_SOFTWARE_FAILURE)


def video_replay_entrypoint(argv: list[str] | None = None) -> int:
    try:
        return int(video_replay_main(argv))
    except SystemExit:
        raise
    except (OSError, ValueError, RuntimeError) as error:
        print(f"video replay failed: {error}", file=sys.stderr)
        return int(ExitCode.IO_FAILURE if isinstance(error, OSError) else ExitCode.INVALID_ARGUMENTS)


__all__ = [
    "replay_broker_entrypoint",
    "replay_broker_main",
    "replay_entrypoint",
    "replay_main",
    "video_replay_entrypoint",
    "video_replay_main",
]
