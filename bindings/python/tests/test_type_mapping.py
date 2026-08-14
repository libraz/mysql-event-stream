"""The column-type table must say the same thing on every surface.

The contract lives in three documentation comments: the canonical table in
``core/include/mes.h`` and the table each binding restates for its own users.
Nothing but a test stops them from drifting apart -- which is how TIMESTAMP
came to be documented as an ``int`` while the core produced a string.

Every file writes its rows as ``<runtime type> => <MYSQL TYPE> <MYSQL TYPE>...``
so all three can be compared mechanically. The Node binding runs the same
comparison in ``bindings/node/tests/type-mapping.test.ts``, so either test
runner catches a one-sided edit.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]

# Runtime type each MES_COL_* value produces in each binding.
RUNTIME_TYPES = {
    "MES_COL_INT": {"node": "number | bigint", "python": "int"},
    "MES_COL_DOUBLE": {"node": "number", "python": "float"},
    "MES_COL_STRING": {"node": "string", "python": "str"},
    "MES_COL_BYTES": {"node": "Uint8Array", "python": "bytes"},
}

ROW_PATTERN = re.compile(
    r"^[\s*]*([A-Za-z_][A-Za-z0-9_ |]*?)\s*=>\s*([A-Z0-9]+(?: [A-Z0-9]+)*)\s*$"
)

EXPECTED_COLUMN_TYPES = {
    "BIGINT",
    "BINARY",
    "BIT",
    "BLOB",
    "CHAR",
    "DATE",
    "DATETIME",
    "DECIMAL",
    "DOUBLE",
    "ENUM",
    "FLOAT",
    "GEOMETRY",
    "INT",
    "JSON",
    "MEDIUMINT",
    "SET",
    "SMALLINT",
    "TEXT",
    "TIME",
    "TIMESTAMP",
    "TINYINT",
    "VARBINARY",
    "VARCHAR",
    "VECTOR",
    "YEAR",
}


def parse_table(file: str, targets: list[str]) -> dict[str, str]:
    """Parse the ``<target> => <TYPES>`` rows a documentation table declares."""
    mapping: dict[str, str] = {}
    rows = 0
    for line in (REPO_ROOT / file).read_text(encoding="utf-8").splitlines():
        match = ROW_PATTERN.match(line)
        if match is None or match.group(1) not in targets:
            continue
        rows += 1
        for mysql_type in match.group(2).split(" "):
            assert mysql_type not in mapping, f"{file} lists {mysql_type} more than once"
            mapping[mysql_type] = match.group(1)
    assert rows == len(targets), f"{file} must declare one row per column type category"
    return mapping


def canonical_table() -> dict[str, str]:
    return parse_table("core/include/mes.h", list(RUNTIME_TYPES))


def test_canonical_table_covers_every_representable_column_type() -> None:
    assert set(canonical_table()) == EXPECTED_COLUMN_TYPES


@pytest.mark.parametrize(
    ("binding", "file"),
    [
        ("python", "bindings/python/src/mysql_event_stream/types.py"),
        ("node", "bindings/node/src/types.ts"),
    ],
)
def test_binding_doc_matches_the_canonical_table(binding: str, file: str) -> None:
    canonical = canonical_table()
    expected = {
        mysql_type: RUNTIME_TYPES[target][binding] for mysql_type, target in canonical.items()
    }
    targets = [types[binding] for types in RUNTIME_TYPES.values()]
    assert parse_table(file, targets) == expected


@pytest.mark.parametrize(
    ("mysql_type", "expected"),
    [
        ("TIMESTAMP", "MES_COL_STRING"),
        ("ENUM", "MES_COL_INT"),
        ("SET", "MES_COL_INT"),
        ("VECTOR", "MES_COL_BYTES"),
    ],
)
def test_categories_that_have_been_documented_wrongly_before(
    mysql_type: str, expected: str
) -> None:
    """A table can agree with itself on every surface and still be wrong.

    TIMESTAMP is text from the decoder. ENUM and SET are ordinals kept out of
    the charset index space, so they can never carry the text of their labels.
    VECTOR is inside that index space with the binary collation, so it is
    always bytes.
    """
    assert canonical_table()[mysql_type] == expected
