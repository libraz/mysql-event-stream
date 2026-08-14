"""Mirror of ``core/contracts/bindings.json``, the cross-binding contract.

The contract fixes error retryability, the reconnect backoff schedule, the
shared option defaults and their accepted ranges. ``tests/test_contract.py``
compares every constant below against that file, and the Node binding mirrors
the same table in ``src/contract.ts``. Editing a value here without editing the
contract (or the other way round) fails the test.

Shared option defaults are not restated here: they are the keyword defaults of
:meth:`CdcStream.__init__` and :meth:`BinlogClient.__init__`, and the same test
compares those signatures against the contract.
"""

from __future__ import annotations

from typing import NamedTuple

from ._ffi import (
    MES_ERR_AUTH,
    MES_ERR_CHECKSUM,
    MES_ERR_DECODE,
    MES_ERR_DECODE_COLUMN,
    MES_ERR_DECODE_ROW,
    MES_ERR_GTID_PURGED,
    MES_ERR_GTID_TAGGED_UNSUPPORTED,
    MES_ERR_INVALID_ARG,
    MES_ERR_NULL_ARG,
    MES_ERR_PARSE,
    MES_ERR_QUEUE_FULL,
    MES_ERR_VALIDATION,
)

#: Error codes that indicate a permanent failure where reconnecting is futile.
NON_RETRYABLE_ERROR_CODES = frozenset(
    {
        MES_ERR_NULL_ARG,
        MES_ERR_INVALID_ARG,
        MES_ERR_PARSE,
        MES_ERR_CHECKSUM,
        MES_ERR_DECODE,
        MES_ERR_DECODE_COLUMN,
        MES_ERR_DECODE_ROW,
        MES_ERR_QUEUE_FULL,
        MES_ERR_AUTH,
        MES_ERR_VALIDATION,
        MES_ERR_GTID_PURGED,
        MES_ERR_GTID_TAGGED_UNSUPPORTED,
    }
)

# Linear backoff capped at RECONNECT_MAX_DELAY_MS, then scaled by the jitter window.
RECONNECT_BASE_DELAY_MS = 1000
RECONNECT_MAX_DELAY_MS = 10000
RECONNECT_JITTER_MIN = 0.5
RECONNECT_JITTER_MAX = 1.0


def backoff_delay_ms(attempt: int, jitter: float) -> float:
    """Return the backoff before reconnect attempt ``attempt`` (1-based).

    Args:
        attempt: Retry ordinal; the undelayed base grows linearly with it.
        jitter: Uniform sample in ``[0, 1)`` selecting a point in the jitter
            window.

    Returns:
        Delay in milliseconds.
    """
    undelayed = min(RECONNECT_BASE_DELAY_MS * attempt, RECONNECT_MAX_DELAY_MS)
    return undelayed * (
        RECONNECT_JITTER_MIN + jitter * (RECONNECT_JITTER_MAX - RECONNECT_JITTER_MIN)
    )


class OptionRange(NamedTuple):
    """Accepted range for an integer option; ``None`` upper bound is unbounded."""

    minimum: int
    maximum: int | None


#: Accepted ranges for every shared integer option.
OPTION_RANGES: dict[str, OptionRange] = {
    "port": OptionRange(1, 65535),
    "server_id": OptionRange(1, 4294967295),
    "connect_timeout_s": OptionRange(0, 4294967295),
    "read_timeout_s": OptionRange(0, 4294967295),
    "ssl_mode": OptionRange(0, 4),
    "max_queue_size": OptionRange(0, None),
    "max_queue_bytes": OptionRange(0, None),
    "max_event_size": OptionRange(0, 4294967295),
    "max_reconnect_attempts": OptionRange(0, None),
    "start_binlog_position": OptionRange(0, 4294967295),
}

# Accepted max_events window for a batched poll.
POLL_BATCH_DEFAULT_MAX_EVENTS = 64
POLL_BATCH_MIN_MAX_EVENTS = 1
POLL_BATCH_MAX_MAX_EVENTS = 1024

# Accepted mes_log_level_t window for the process-wide log callback.
LOG_LEVEL_MIN = 0
LOG_LEVEL_MAX = 3
LOG_LEVEL_DEFAULT = 1

#: What the binding does when the optional metadata connection fails and no
#: handler is configured: nothing. The library never writes diagnostics itself.
METADATA_ERROR_DEFAULT = "silent"
