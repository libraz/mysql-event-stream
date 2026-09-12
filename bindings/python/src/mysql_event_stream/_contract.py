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


class ConditionalMinimum(NamedTuple):
    """Floor that holds for an option only while its companion option is set."""

    minimum: int
    companion: str


#: Floors that hold only while a companion option is set. :data:`OPTION_RANGES`
#: keeps the unconditional window, which stays in force otherwise:
#: ``start_binlog_position`` is left at 0 when no file/offset start was
#: requested, so the tighter floor applies only once ``start_binlog_file`` names
#: a file. The first binlog event begins after the file's 4-byte magic number,
#: so an offset into a named file cannot be below 4.
CONDITIONAL_OPTION_MINIMUMS: dict[str, ConditionalMinimum] = {
    "start_binlog_position": ConditionalMinimum(minimum=4, companion="start_binlog_file"),
}

#: Options that are supplied together or not at all. An offset without the file
#: it points into names no position a server can start from, so it is refused
#: rather than accepted and dropped. This is a different statement from
#: :data:`CONDITIONAL_OPTION_MINIMUMS`, which says what a supplied value must be
#: once its companion is set rather than whether it may appear alone.
REQUIRED_TOGETHER_OPTIONS: tuple[tuple[str, str], ...] = (
    ("start_binlog_file", "start_binlog_position"),
)

#: Options that name competing start modes, at most one of which may be
#: supplied. A GTID and a file/offset anchor each say where replication begins,
#: so a configuration carrying both asks for two starts and a binding honouring
#: either would replicate from a position the caller never asked for.
#:
#: The file anchor stands for its whole start mode because
#: :data:`REQUIRED_TOGETHER_OPTIONS` is checked first and binds the offset to
#: the file: a configuration that reaches this check carrying an offset carries
#: the file with it. What counts as supplied is what the pair rule counts,
#: unset spellings included, so an empty ``start_gtid`` is supplied -- it is a
#: deliberate request for the empty GTID set.
MUTUALLY_EXCLUSIVE_OPTIONS: tuple[tuple[str, str], ...] = (("start_gtid", "start_binlog_file"),)

#: What "not supplied" looks like for an option this surface cannot spell as
#: absent. ``start_binlog_position`` is a plain integer here, so a
#: zero-initialized configuration passes 0 -- below the conditional floor, and
#: therefore never a position the offset could have been asked to start from.
#: A caller who passes it explicitly is indistinguishable from one who passed
#: nothing, which is why it counts as unset for the pair rule.
UNSET_OPTION_VALUES: dict[str, int] = {"start_binlog_position": 0}

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
