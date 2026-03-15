"""Polling wait utilities for E2E tests."""

from __future__ import annotations

import time
from collections.abc import Callable


class WaitTimeoutError(Exception):
    """Raised when a wait operation times out."""

    def __init__(self, description: str, timeout: float) -> None:
        super().__init__(f"Timed out waiting for {description} after {timeout}s")
        self.description = description
        self.timeout = timeout


def wait_until(
    predicate: Callable[[], bool],
    *,
    timeout: float = 30.0,
    interval: float = 1.0,
    description: str = "condition",
) -> None:
    """Poll until predicate returns True or timeout is reached.

    Args:
        predicate: Callable that returns True when the condition is met.
        timeout: Maximum seconds to wait.
        interval: Seconds between polls.
        description: Human-readable description for error messages.

    Raises:
        WaitTimeoutError: If the predicate does not return True within the timeout.
    """
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None

    while time.monotonic() < deadline:
        try:
            if predicate():
                return
        except Exception as e:
            last_error = e
        time.sleep(interval)

    msg = f"Timed out waiting for {description} after {timeout}s"
    if last_error:
        msg += f" (last error: {last_error})"
    raise WaitTimeoutError(description, timeout)
