"""Test configuration for mysql-event-stream Python binding tests."""

import pytest

from mysql_event_stream._ffi import _find_library


def _find_lib_path() -> str:
    """Resolve libmes exactly the way the package's own default resolver does.

    The suite must exercise the image an ordinary caller reaches, so it
    delegates instead of repeating the search: a second search order here is
    how one process ends up with two libmes images loaded, whose module-level
    state -- the log callback above all -- is not shared.

    A library that cannot be resolved leaves nothing for the native tests to
    verify, so it aborts them instead of skipping them.
    """
    try:
        return _find_library()
    except OSError as exc:
        pytest.fail(str(exc), pytrace=False)


@pytest.fixture()
def lib_path() -> str:
    """Provide the path to the libmes shared library."""
    return _find_lib_path()
