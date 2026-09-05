"""Nonblocking bounded local surface-video fan-out."""

from __future__ import annotations

import queue
from threading import Lock
from typing import Generic, TypeVar

T = TypeVar("T")


class LocalVideoSubscription(Generic[T]):
    def __init__(self, owner: LocalVideoFanout[T], name: str) -> None:
        self._owner = owner
        self.name = name
        self._queue: queue.Queue[T] = queue.Queue(maxsize=1)
        self._closed = False

    def receive(self, timeout_seconds: float = 0.250) -> T | None:
        if not 0 <= timeout_seconds <= 0.250:
            raise ValueError("local video receive timeout must be between zero and 250 ms")
        if self._closed:
            return None
        try:
            return self._queue.get(timeout=timeout_seconds)
        except queue.Empty:
            return None

    def _offer_latest(self, item: T) -> None:
        if self._closed:
            return
        try:
            self._queue.put_nowait(item)
            return
        except queue.Full:
            pass
        try:
            self._queue.get_nowait()
        except queue.Empty:
            pass
        try:
            self._queue.put_nowait(item)
        except queue.Full:
            pass

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._owner._remove(self.name, self)


class LocalVideoFanout(Generic[T]):
    """Each consumer gets an independent capacity-one keep-latest queue."""

    def __init__(self) -> None:
        self._subscriptions: dict[str, LocalVideoSubscription[T]] = {}
        self._lock = Lock()
        self._closed = False

    def subscribe(self, name: str) -> LocalVideoSubscription[T]:
        if not name:
            raise ValueError("consumer name is required")
        with self._lock:
            if self._closed:
                raise RuntimeError("fan-out is closed")
            if name in self._subscriptions:
                raise ValueError(f"duplicate local video consumer: {name}")
            subscription = LocalVideoSubscription(self, name)
            self._subscriptions[name] = subscription
            return subscription

    def publish(self, item: T) -> None:
        with self._lock:
            subscriptions = tuple(self._subscriptions.values())
        for subscription in subscriptions:
            subscription._offer_latest(item)

    def _remove(self, name: str, expected: LocalVideoSubscription[T]) -> None:
        with self._lock:
            if self._subscriptions.get(name) is expected:
                del self._subscriptions[name]

    def close(self) -> None:
        with self._lock:
            self._closed = True
            subscriptions = tuple(self._subscriptions.values())
            self._subscriptions.clear()
        for subscription in subscriptions:
            subscription._closed = True


__all__ = ["LocalVideoFanout", "LocalVideoSubscription"]
