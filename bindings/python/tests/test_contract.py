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
    MUTUALLY_EXCLUSIVE_OPTIONS,
    NON_RETRYABLE_ERROR_CODES,
    OPTION_RANGES,
    POLL_BATCH_DEFAULT_MAX_EVENTS,
    POLL_BATCH_MAX_MAX_EVENTS,
    POLL_BATCH_MIN_MAX_EVENTS,
    RECONNECT_BASE_DELAY_MS,
    RECONNECT_JITTER_MAX,
    RECONNECT_JITTER_MIN,
    RECONNECT_MAX_DELAY_MS,
    REQUIRED_TOGETHER_OPTIONS,
    UNSET_OPTION_VALUES,
    backoff_delay_ms,
)
from mysql_event_stream._options import validate_option, validate_options
from mysql_event_stream.client import BinlogClient, validate_poll_batch_size
from mysql_event_stream.logging import set_log_callback
from mysql_event_stream.stream import CdcStream

from .contract_fixture import load_binding_contract, load_header_field_doc

contract = load_binding_contract()

#: Start-position offset, whose range the contract states.
START_POSITION = next(
    (option for option in contract["options"] if option["canonical"] == "startBinlogPosition"),
    None,
)

#: The offset and the file it points into, which the contract pairs.
START_POSITION_PAIR = next(
    (
        pair
        for pair in contract["requiredTogether"]["pairs"]
        if pair["canonical"] == ["startBinlogFile", "startBinlogPosition"]
    ),
    None,
)

#: The GTID anchor and the file anchor, which the contract makes exclusive.
START_MODE_PAIR = next(
    (
        pair
        for pair in contract["mutuallyExclusive"]["pairs"]
        if pair["canonical"] == ["startGtid", "startBinlogFile"]
    ),
    None,
)

#: A GTID a start-mode case can name. Which mode an option selects is what the
#: exclusion is about, so any well-formed set serves.
START_MODE_GTID = "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-2"

#: How a supplied offset relates to the windows the contract states. Crossed
#: with the paired file option's two states and with both entry points that
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

#: Options the contract carries a default for that a surface deliberately does
#: not accept, keyed by surface and stating why. An absence not listed here is a
#: gap: the defaults test compares whole maps, so anything omitted from a
#: constructor fails unless it is named below.
DELIBERATELY_ABSENT_OPTIONS: dict[str, dict[str, str]] = {
    "BinlogClient": {
        # Reconnecting is the stream's own retry loop; the client is one
        # connection the caller drives and reopens itself, so it carries no
        # retry budget. The Node surfaces draw the same line -- the option sits
        # on its stream configuration and not on its client one.
        "max_reconnect_attempts": "reconnecting belongs to the stream, not to one connection",
    },
}

#: Contract options that state the value a binding materializes when the option
#: is omitted. Selectors without a default are range-checked only.
DEFAULTED_OPTIONS = tuple(option for option in contract["options"] if "default" in option)


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


def _supplied(position: int | None) -> bool:
    """Report whether ``position`` is an offset this surface can see as supplied.

    The offset is a plain integer here, so the value the contract names as this
    surface's unset spelling cannot be told apart from an omitted one and counts
    as unset. That is the one combination where the two surfaces' decisions
    differ: Node spells unset by leaving the key out, so it refuses the value
    this surface accepts. Neither names a position -- the value is below the
    floor a companion file brings into force -- so no configuration that asks
    to start somewhere is treated differently.
    """
    assert START_POSITION is not None
    unset = contract["requiredTogether"]["unsetValues"]["python"][START_POSITION["python"]]
    return position is not None and position != unset


def _reason(option: dict[str, Any], file_set: bool, position: int | None) -> str | None:
    """Name the rule that refuses a combination, or ``None`` if it is accepted.

    One predicate serves both entry points: the shared option table and the
    client's own construction agree on every combination.
    """
    if position is not None and (position < option["min"] or position > option["max"]):
        return "range"
    if _supplied(position) != file_set:
        return "pair"
    if file_set and position is not None and position < option["minWhenFileSet"]:
        return "floor"
    return None


def _case_kwargs(file_set: bool, position: int | None) -> dict[str, Any]:
    """Build the option subset one case supplies, leaving everything else unset."""
    assert START_POSITION_PAIR is not None
    file_option, offset_option = START_POSITION_PAIR["python"]
    kwargs: dict[str, Any] = {}
    if file_set:
        kwargs[file_option] = "binlog.000001"
    if position is not None:
        kwargs[offset_option] = position
    return kwargs


def _start_mode_kwargs(gtid_set: bool, anchor_set: bool) -> dict[str, Any]:
    """Build the start-position options one start-mode case supplies.

    The anchor is both of its options, because the pair rule refuses either one
    alone: naming only the file would be refused over a constraint this case is
    not about.
    """
    assert START_MODE_PAIR is not None
    assert START_POSITION is not None
    gtid_option, file_option = START_MODE_PAIR["python"]
    kwargs: dict[str, Any] = {}
    if gtid_set:
        kwargs[gtid_option] = START_MODE_GTID
    if anchor_set:
        kwargs[file_option] = "binlog.000001"
        kwargs[START_POSITION["python"]] = START_POSITION["minWhenFileSet"]
    return kwargs


@contextlib.contextmanager
def _mocked_client_library() -> Iterator[MagicMock]:
    """Stand in for libmes so a client can be built and connected without a server.

    Yields:
        The stand-in library, whose recorded calls say how far a case got.
    """
    lib = MagicMock()
    lib.mes_client_create.return_value = 0xDEAD
    lib.mes_client_set_max_event_size.return_value = 0
    lib.mes_client_set_max_queue_bytes.return_value = 0
    lib.mes_client_connect.return_value = 0
    with (
        patch("mysql_event_stream.client.get_library", return_value=lib),
        patch("mysql_event_stream.client.load_client_library", return_value=True),
    ):
        yield lib


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
        absent = DELIBERATELY_ABSENT_OPTIONS.get(surface.__name__, {})
        stated = {option["python"] for option in DEFAULTED_OPTIONS}
        assert absent.keys() <= stated, "every excused absence names a contract option"

        expected = {
            option["python"]: option["default"]
            for option in DEFAULTED_OPTIONS
            if option["python"] not in absent
        }
        # Whole maps rather than one assertion per option, so an option this
        # surface never accepted fails as a missing key instead of passing
        # unexamined, and one excused above fails once it reappears.
        materialized = {name: parameters[name].default for name in stated if name in parameters}
        assert materialized == expected

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

    def test_mirrors_the_contract_required_together_pairs(self) -> None:
        expected = tuple(tuple(pair["python"]) for pair in contract["requiredTogether"]["pairs"])
        assert expected == REQUIRED_TOGETHER_OPTIONS

        for pair in expected:
            for name in pair:
                # Recognized by this surface's option table, whether or not the
                # shared table states a range for it: the value each option
                # carries when unset is accepted, and a name the table does not
                # know is refused by the same validator.
                validate_option(name, UNSET_OPTION_VALUES.get(name))
                with pytest.raises(TypeError):
                    validate_option(f"not_{name}", None)

    def test_mirrors_the_contract_mutually_exclusive_pairs(self) -> None:
        expected = tuple(tuple(pair["python"]) for pair in contract["mutuallyExclusive"]["pairs"])
        assert expected == MUTUALLY_EXCLUSIVE_OPTIONS

        for pair in expected:
            for name in pair:
                # Recognized by this surface's option table, and a name the
                # table does not know is refused by the same validator.
                validate_option(name, None)
                with pytest.raises(TypeError):
                    validate_option(f"not_{name}", None)

        # The file anchor stands for its whole start mode only because the pair
        # rule binds the offset to it, so the two blocks have to name the same
        # file option.
        assert START_MODE_PAIR is not None
        assert START_POSITION_PAIR is not None
        assert START_MODE_PAIR["python"][1] == START_POSITION_PAIR["python"][0]

    def test_refuses_two_start_modes_at_construction_on_every_entry_point(self) -> None:
        assert START_MODE_PAIR is not None
        gtid_option, file_option = START_MODE_PAIR["python"]

        for gtid_set in (False, True):
            for anchor_set in (False, True):
                kwargs = _start_mode_kwargs(gtid_set, anchor_set)
                label = (
                    f"gtid {'set' if gtid_set else 'unset'}, "
                    f"anchor {'set' if anchor_set else 'unset'}"
                )

                if not (gtid_set and anchor_set):
                    validate_options(kwargs)
                    CdcStream(**kwargs)
                    with _mocked_client_library():
                        client = BinlogClient(**kwargs)
                        client.connect()
                        client.close()
                    continue

                with pytest.raises(ValueError) as rejection:
                    validate_options(kwargs)
                stated = str(rejection.value)
                # A refusal the exclusion decides names both start modes.
                assert gtid_option in stated, label
                assert file_option in stated, label

                with pytest.raises(ValueError):
                    CdcStream(**kwargs)
                with _mocked_client_library() as lib:
                    with pytest.raises(ValueError):
                        BinlogClient(**kwargs)
                    # Construction is where it is refused, so nothing the
                    # connection would have done was reached.
                    lib.mes_client_create.assert_not_called()
                    lib.mes_client_connect.assert_not_called()

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

    def test_mirrors_the_contract_unset_option_values(self) -> None:
        assert START_POSITION is not None
        option = START_POSITION
        stated = contract["requiredTogether"]["unsetValues"]["python"]
        assert stated == UNSET_OPTION_VALUES

        # What the contract states this surface passes when nothing was
        # requested is what its signatures actually default to, on both entry
        # points, and it is below the floor so it can never be a real offset.
        unset = stated[option["python"]]
        for surface in (CdcStream, BinlogClient):
            parameters = inspect.signature(surface.__init__).parameters
            assert parameters[option["python"]].default == unset, surface.__name__
        assert unset < option["minWhenFileSet"]

        # The stated range has to admit it. This surface passes the value on
        # every construction that requested no file/offset start, so a range
        # tightened to the conditional floor would refuse each of them -- which
        # is the whole reason the range minimum sits below that floor.
        assert option["min"] <= unset <= option["max"]

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

    def test_requires_the_offset_and_its_file_together_at_every_entry_point(self) -> None:
        assert START_POSITION is not None
        assert START_POSITION_PAIR is not None
        option = START_POSITION
        file_option, offset_option = START_POSITION_PAIR["python"]

        for file_set in (False, True):
            for position_class in POSITION_CLASSES:
                position = _position_for(option, position_class)
                kwargs = _case_kwargs(file_set, position)
                label = f"{position_class}, file {'set' if file_set else 'unset'}"
                reason = _reason(option, file_set, position)

                if reason is None:
                    validate_options(kwargs)
                    with _mocked_client_library():
                        client = BinlogClient(**kwargs)
                        client.connect()
                        client.close()
                    continue

                with pytest.raises(ValueError) as rejection:
                    validate_options(kwargs)
                stated = str(rejection.value)
                if reason == "pair":
                    # A refusal the pair decides names both options.
                    assert file_option in stated, label
                    assert offset_option in stated, label
                else:
                    # A refusal a window decides names the option and the bound
                    # it crossed.
                    assert offset_option in stated, label
                    bound = option["minWhenFileSet"] if reason == "floor" else option["max"]
                    assert str(bound) in stated, label
                with _mocked_client_library(), pytest.raises(ValueError):
                    BinlogClient(**kwargs).connect()

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
