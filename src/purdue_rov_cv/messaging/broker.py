"""Real payload-agnostic XPUB/XSUB data broker."""

from __future__ import annotations

from threading import get_ident

import zmq

from purdue_rov_cv.config.models import AppConfig
from purdue_rov_cv.runtime.json_logging import StructuredJsonLogger
from purdue_rov_cv.runtime.metrics import RuntimeMetrics
from purdue_rov_cv.runtime.rate_limit import WarningRateLimiter
from purdue_rov_cv.runtime.shutdown import ShutdownCoordinator, install_signal_handlers
from purdue_rov_cv.runtime.state import ComponentState, ComponentStateMachine

from .sockets import configure_xpub, configure_xsub


class DataBrokerService:
    """Own both broker sockets and their context in the calling thread."""

    def __init__(
        self,
        publisher_endpoint: str,
        subscriber_endpoint: str,
        *,
        metrics: RuntimeMetrics | None = None,
        logger: StructuredJsonLogger | None = None,
        warning_limiter: WarningRateLimiter | None = None,
        install_signals: bool = False,
    ) -> None:
        self.publisher_endpoint = publisher_endpoint
        self.subscriber_endpoint = subscriber_endpoint
        self.metrics = metrics or RuntimeMetrics()
        self.logger = logger
        self.warning_limiter = warning_limiter or WarningRateLimiter()
        self.state_machine = ComponentStateMachine()
        self.shutdown = ShutdownCoordinator(state_machine=self.state_machine)
        self._install_signals = install_signals
        self._owner_thread_id: int | None = None

    @classmethod
    def from_config(
        cls,
        config: AppConfig,
        *,
        metrics: RuntimeMetrics | None = None,
        logger: StructuredJsonLogger | None = None,
        warning_limiter: WarningRateLimiter | None = None,
        install_signals: bool = False,
    ) -> DataBrokerService:
        return cls(
            config.messaging.broker.publisher_endpoint,
            config.messaging.broker.subscriber_endpoint,
            metrics=metrics,
            logger=logger,
            warning_limiter=warning_limiter,
            install_signals=install_signals,
        )

    @property
    def owner_thread_id(self) -> int | None:
        return self._owner_thread_id

    def request_shutdown(self, reason: str = "requested") -> None:
        self.shutdown.request(reason)

    def _forward(self, source: zmq.Socket[bytes], destination: zmq.Socket[bytes]) -> None:
        frames = source.recv_multipart()
        self.metrics.increment("messages_received")
        try:
            destination.send_multipart(frames, flags=zmq.DONTWAIT)
        except zmq.Again:
            warning = self.warning_limiter.check("BROKER_FORWARD_DROPPED")
            if warning.emit and self.logger is not None:
                self.logger.log(
                    "WARNING",
                    "BROKER_FORWARD_DROPPED",
                    "broker destination HWM reached",
                    context={"previously_suppressed": warning.suppressed_count},
                )
            elif not warning.emit:
                self.metrics.increment("warnings_suppressed")
            return
        self.metrics.increment("messages_sent")

    def run(self) -> None:
        """Bind and route until shutdown; all ZeroMQ use remains on this thread."""
        self._owner_thread_id = get_ident()
        if self._install_signals:
            install_signal_handlers(self.shutdown)
        context = zmq.Context()
        xsub: zmq.Socket[bytes] | None = None
        xpub: zmq.Socket[bytes] | None = None
        try:
            xsub = context.socket(zmq.XSUB)
            xpub = context.socket(zmq.XPUB)
            configure_xsub(xsub)
            configure_xpub(xpub)
            xsub.bind(self.publisher_endpoint)
            xpub.bind(self.subscriber_endpoint)
            self.state_machine.transition_to(ComponentState.READY)
            self.state_machine.transition_to(ComponentState.RUNNING)
            poller = zmq.Poller()
            poller.register(xsub, zmq.POLLIN)
            poller.register(xpub, zmq.POLLIN)
            while not self.shutdown.token.is_requested:
                events = dict(poller.poll(100))
                if xsub in events:
                    self._forward(xsub, xpub)
                if xpub in events:
                    self._forward(xpub, xsub)
        finally:
            if self.state_machine.state in {
                ComponentState.STARTING,
                ComponentState.READY,
                ComponentState.RUNNING,
                ComponentState.DEGRADED,
            }:
                self.shutdown.request("broker loop stopped")
            if xpub is not None:
                xpub.close(linger=0)
            if xsub is not None:
                xsub.close(linger=0)
            context.term()
            if self.state_machine.state is ComponentState.STOPPING:
                self.shutdown.run(timeout_seconds=5.0)


__all__ = ["DataBrokerService"]
