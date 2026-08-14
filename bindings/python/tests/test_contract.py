"""Cross-binding contract tests.

Mirrors ``bindings/node/tests/contract.test.ts``: both suites assert the same
properties against ``core/contracts/bindings.json``, so a constant changed on
one surface alone fails that surface's test run.
"""

from __future__ import annotations

import contextlib
import inspect
import io
import warnings
from typing import Any

import pytest

from mysql_event_stream._contract import (
    LOG_LEVEL_DEFAULT,
    LOG_LEVEL_MAX,
    LOG_LEVEL_MIN,
    METADATA_ERROR_DEFAULT,
    NON_RETRYABLE_ERROR_CODES,
    OPTION_RANGES,
    POLL_BATCH_DEFAULT_MAX_EVENTS,
    POLL_BATCH_MAX_MAX_EVENTS,
    POLL_BATCH_MIN_MAX_EVENTS,
    RECONNECT_BASE_DELAY_MS,
    RECONNECT_JITTER_MAX,
    RECONNECT_JITTER_MIN,
    RECONNECT_MAX_DELAY_MS,
    backoff_delay_ms,
)
from mysql_event_stream.client import BinlogClient, validate_poll_batch_size
from mysql_event_stream.logging import set_log_callback
from mysql_event_stream.stream import CdcStream, _validate_stream_option

from .contract_fixture import load_binding_contract

contract = load_binding_contract()


def _start_position_of(stream: CdcStream) -> dict[str, Any]:
    """Reduce a stream to the canonical start-position triple."""
    return {
        "startGtid": stream._start_gtid or None,
        "startBinlogFile": stream._start_binlog_file or None,
        "startBinlogPosition": stream._start_binlog_position or None,
    }


def _config_for(start: dict[str, Any]) -> dict[str, Any]:
    """Build the constructor kwargs a checkpoint-resume case starts from."""
    kwargs: dict[str, Any] = {"host": "127.0.0.1"}
    if start["startGtid"] is not None:
        kwargs["start_gtid"] = start["startGtid"]
    if start["startBinlogFile"] is not None:
        kwargs["start_binlog_file"] = start["startBinlogFile"]
    if start["startBinlogPosition"] is not None:
        kwargs["start_binlog_position"] = start["startBinlogPosition"]
    return kwargs


class TestBindingContract:
    def test_classifies_exactly_the_contract_non_retryable_codes(self) -> None:
        assert sorted(NON_RETRYABLE_ERROR_CODES) == sorted(
            entry["code"] for entry in contract["nonRetryableErrorCodes"]
        )

    def test_never_classifies_a_retryable_code_as_permanent(self) -> None:
        for entry in contract["retryableErrorCodes"]:
            assert entry["code"] not in NON_RETRYABLE_ERROR_CODES, entry["name"]

    def test_matches_the_contract_backoff_window(self) -> None:
        assert contract["reconnect"]["baseDelayMs"] == RECONNECT_BASE_DELAY_MS
        assert contract["reconnect"]["maxDelayMs"] == RECONNECT_MAX_DELAY_MS
        assert contract["reconnect"]["jitterMin"] == RECONNECT_JITTER_MIN
        assert contract["reconnect"]["jitterMax"] == RECONNECT_JITTER_MAX

    def test_matches_the_contract_backoff_schedule(self) -> None:
        for step in contract["reconnect"]["schedule"]:
            attempt, undelayed = step["attempt"], step["undelayedMs"]
            assert backoff_delay_ms(attempt, 0) == undelayed * contract["reconnect"]["jitterMin"]
            assert backoff_delay_ms(attempt, 1) == undelayed * contract["reconnect"]["jitterMax"]

    def test_follows_the_contract_checkpoint_resume_cases(self) -> None:
        for case in contract["checkpointResume"]["cases"]:
            stream = CdcStream(**_config_for(case["config"]))
            stream._adopt_resume_position(case["checkpoint"])
            assert _start_position_of(stream) == case["expect"], case["name"]

    def test_stays_silent_when_metadata_fails_and_no_handler_is_configured(self) -> None:
        assert contract["metadataError"]["defaultBehaviour"] == METADATA_ERROR_DEFAULT

        stream = CdcStream(host="127.0.0.1")
        captured = io.StringIO()
        with warnings.catch_warnings(record=True) as raised, contextlib.redirect_stderr(captured):
            warnings.simplefilter("always")
            stream._report_metadata_error(RuntimeError("metadata connection refused"))

        assert raised == []
        assert captured.getvalue() == ""

    def test_hands_a_metadata_failure_to_the_configured_handler(self) -> None:
        seen: list[BaseException] = []
        stream = CdcStream(host="127.0.0.1", on_metadata_error=seen.append)
        stream._report_metadata_error(RuntimeError("metadata connection refused"))
        assert [str(error) for error in seen] == ["metadata connection refused"]

    def test_exposes_the_contract_iteration_release_entry_point(self) -> None:
        assert contract["iteration"]["releaseContract"] == "aclose"
        stream = CdcStream(host="127.0.0.1")
        assert inspect.iscoroutinefunction(stream.aclose)
        assert inspect.iscoroutinefunction(stream.close)

    @pytest.mark.parametrize("surface", [CdcStream, BinlogClient])
    def test_materializes_exactly_the_contract_shared_option_defaults(self, surface: type) -> None:
        parameters = inspect.signature(surface.__init__).parameters
        for option in contract["options"]:
            if "default" not in option:
                continue
            name = option["python"]
            if name not in parameters:
                continue
            assert parameters[name].default == option["default"], name

    def test_enforces_the_contract_shared_option_ranges(self) -> None:
        expected = {
            option["python"]: (option["min"], option["max"])
            for option in contract["options"]
            if option["type"] == "integer"
        }
        assert {key: tuple(value) for key, value in OPTION_RANGES.items()} == expected

        for name, (minimum, maximum) in expected.items():
            with pytest.raises(ValueError):
                _validate_stream_option(name, minimum - 1)
            _validate_stream_option(name, minimum)
            if maximum is None:
                continue
            with pytest.raises(ValueError):
                _validate_stream_option(name, maximum + 1)

    def test_enforces_the_contract_poll_batch_window(self) -> None:
        assert contract["pollBatch"]["defaultMaxEvents"] == POLL_BATCH_DEFAULT_MAX_EVENTS
        assert contract["pollBatch"]["minMaxEvents"] == POLL_BATCH_MIN_MAX_EVENTS
        assert contract["pollBatch"]["maxMaxEvents"] == POLL_BATCH_MAX_MAX_EVENTS

        with pytest.raises(ValueError):
            validate_poll_batch_size(POLL_BATCH_MIN_MAX_EVENTS - 1)
        with pytest.raises(ValueError):
            validate_poll_batch_size(POLL_BATCH_MAX_MAX_EVENTS + 1)
        validate_poll_batch_size(POLL_BATCH_MIN_MAX_EVENTS)
        validate_poll_batch_size(POLL_BATCH_MAX_MAX_EVENTS)
        validate_poll_batch_size(POLL_BATCH_DEFAULT_MAX_EVENTS)

    def test_enforces_the_contract_log_level_window(self) -> None:
        assert contract["logLevel"]["min"] == LOG_LEVEL_MIN
        assert contract["logLevel"]["max"] == LOG_LEVEL_MAX
        assert contract["logLevel"]["default"] == LOG_LEVEL_DEFAULT

        try:
            for level in range(LOG_LEVEL_MIN, LOG_LEVEL_MAX + 1):
                set_log_callback(lambda _level, _message: None, level)  # type: ignore[arg-type]
            with pytest.raises(ValueError):
                set_log_callback(lambda _level, _message: None, LOG_LEVEL_MIN - 1)  # type: ignore[arg-type]
            with pytest.raises(ValueError):
                set_log_callback(lambda _level, _message: None, LOG_LEVEL_MAX + 1)  # type: ignore[arg-type]
            with pytest.raises(TypeError):
                set_log_callback(lambda _level, _message: None, 1.5)  # type: ignore[arg-type]
        finally:
            set_log_callback(None)
