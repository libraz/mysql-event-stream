"""Cross-binding contract tests.

Mirrors ``bindings/node/tests/contract.test.ts``: both suites assert the same
properties against ``core/contracts/bindings.json``, so a constant changed on
one surface alone fails that surface's test run.
"""

from __future__ import annotations

import contextlib
import inspect
import io
import re
import warnings
from collections.abc import Iterator
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from mysql_event_stream._contract import (
    CONDITIONAL_OPTION_MINIMUMS,
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
from mysql_event_stream._options import validate_option
from mysql_event_stream.client import BinlogClient, validate_poll_batch_size
from mysql_event_stream.logging import set_log_callback
from mysql_event_stream.stream import CdcStream

from .contract_fixture import load_binding_contract, load_header_field_doc

contract = load_binding_contract()

#: Start-position offset the contract declares a conditional floor for.
START_POSITION = next(
    (option for option in contract["options"] if option["canonical"] == "startBinlogPosition"),
    None,
)

#: How a supplied offset relates to the windows the contract states. Crossed
#: with the companion file option's two states and with both entry points that
#: accept the option, this enumerates the whole start-position surface instead
#: of sampling it.
POSITION_CLASSES = (
    "omitted",
    "range_minimum",
    "below_floor",
    "at_floor",
    "at_maximum",
    "above_maximum",
)

_HEADER_WINDOW = re.compile(r"(\d+) through UINT32_MAX")


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


def _position_for(option: dict[str, Any], position_class: str) -> int | None:
    """Return the offset a class stands for, derived from the contract's numbers."""
    values = {
        "omitted": None,
        "range_minimum": option["min"],
        "below_floor": option["minWhenFileSet"] - 1,
        "at_floor": option["minWhenFileSet"],
        "at_maximum": option["max"],
        "above_maximum": option["max"] + 1,
    }
    return values[position_class]


def _rejects(
    option: dict[str, Any], file_set: bool, position: int | None, entry_point: str
) -> bool:
    """Report whether an entry point must refuse one combination.

    ``options`` is the shared option table, which range-checks each key on its
    own and so never sees the companion; ``client`` is the connect path, the
    only place the conditional floor applies. An offset supplied without a file
    is currently accepted and silently ignored here while the Node surface
    refuses it outright: the two surfaces disagree, and this predicate pins
    what each one does today rather than stating what it should do.
    """
    if position is not None and (position < option["min"] or position > option["max"]):
        return True
    if entry_point == "options":
        return False
    if not file_set:
        return False
    # An omitted offset reaches the check as the constructor's default, which
    # is below the floor.
    default = inspect.signature(BinlogClient.__init__).parameters[option["python"]].default
    return (default if position is None else position) < option["minWhenFileSet"]


def _case_kwargs(option: dict[str, Any], file_set: bool, position: int | None) -> dict[str, Any]:
    """Build the option subset one case supplies, leaving everything else unset."""
    kwargs: dict[str, Any] = {}
    if file_set:
        kwargs[option["fileOption"]["python"]] = "binlog.000001"
    if position is not None:
        kwargs[option["python"]] = position
    return kwargs


@contextlib.contextmanager
def _mocked_client_library() -> Iterator[None]:
    """Stand in for libmes so a client can be built and connected without a server."""
    lib = MagicMock()
    lib.mes_client_create.return_value = 0xDEAD
    lib.mes_client_set_max_event_size.return_value = 0
    lib.mes_client_set_max_queue_bytes.return_value = 0
    lib.mes_client_connect.return_value = 0
    with (
        patch("mysql_event_stream.client.get_library", return_value=lib),
        patch("mysql_event_stream.client.load_client_library", return_value=True),
    ):
        yield


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
                validate_option(name, minimum - 1)
            validate_option(name, minimum)
            if maximum is None:
                continue
            with pytest.raises(ValueError):
                validate_option(name, maximum + 1)

    def test_mirrors_the_contract_conditional_start_position_floor(self) -> None:
        assert START_POSITION is not None, "startBinlogPosition declared in the contract options"
        option = START_POSITION
        assert isinstance(option["minWhenFileSet"], int), "conditional floor stated in the contract"
        assert option["fileOption"]["python"] == "start_binlog_file"

        # The floor is conditional, so the stated range keeps a lower minimum:
        # the value the constructor passes when no file/offset start was
        # requested has to stay acceptable.
        unset = inspect.signature(BinlogClient.__init__).parameters[option["python"]].default
        assert option["min"] <= unset < option["minWhenFileSet"]
        validate_option(option["python"], unset)

        assert CONDITIONAL_OPTION_MINIMUMS[option["python"]] == (
            option["minWhenFileSet"],
            option["fileOption"]["python"],
        )

    def test_documents_the_contract_start_position_floor_in_the_abi_header(self) -> None:
        assert START_POSITION is not None
        documented = load_header_field_doc("binlog_position")
        window = _HEADER_WINDOW.search(documented)
        assert window is not None, f"mes.h states an accepted offset window: {documented}"
        assert int(window.group(1)) == START_POSITION["minWhenFileSet"]
        # UINT32_MAX as the header spells the upper bound the contract states.
        assert START_POSITION["max"] == 2**32 - 1
        # The header states the same trigger the contract does: the floor holds
        # for a file/offset start, not for every offset the field can carry.
        assert "MES_START_AT_POSITION" in documented
        # A parse that stops matching has to fail rather than hand back nothing
        # for the assertions above to pass over.
        with pytest.raises(RuntimeError):
            load_header_field_doc("no_such_field")

    def test_applies_the_start_position_floor_only_when_the_companion_file_is_set(self) -> None:
        assert START_POSITION is not None
        option = START_POSITION
        floor = option["minWhenFileSet"]

        for file_set in (False, True):
            for position_class in POSITION_CLASSES:
                position = _position_for(option, position_class)
                kwargs = _case_kwargs(option, file_set, position)
                label = f"{position_class}, file {'set' if file_set else 'unset'}"

                if position is not None:
                    if _rejects(option, file_set, position, "options"):
                        with pytest.raises(ValueError):
                            validate_option(option["python"], position)
                    else:
                        validate_option(option["python"], position)

                with _mocked_client_library():
                    if _rejects(option, file_set, position, "client"):
                        with pytest.raises(ValueError) as rejection:
                            BinlogClient(**kwargs).connect()
                        # A refusal the floor itself decides names the option
                        # and the floor; the stated range is another refusal
                        # with its own wording.
                        if position is None or position <= option["max"]:
                            assert option["python"] in str(rejection.value), label
                            assert str(floor) in str(rejection.value), label
                    else:
                        client = BinlogClient(**kwargs)
                        client.connect()
                        client.close()

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
