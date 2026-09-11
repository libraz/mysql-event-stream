"""Loader for the numeric constants the C ABI header declares."""

from __future__ import annotations

import re
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent
_HEADER_PATH = _REPO_ROOT / "core" / "include" / "mes.h"
_LAYOUT_PATH = _REPO_ROOT / "core" / "src" / "capi.cpp"

# Only the public structs are of interest; the same assertions also compare
# sizeof(size_t) and similar, which are not part of any mirror.
_STRUCT_SIZE = re.compile(r"sizeof\((mes_\w+_t)\)\s*==\s*(\d+)")
_STRUCT_OFFSET = re.compile(r"offsetof\((mes_\w+_t),\s*(\w+)\)\s*==\s*(\d+)")

_ENUM_OPEN = re.compile(r"^typedef\s+enum")
_ENUM_CLOSE = re.compile(r"^}\s*(\w+)\s*;")
_ENUM_MEMBER = re.compile(r"^(MES_\w+)\s*=\s*(-?\d+)\s*[,}]?")
_INT_DEFINE = re.compile(r"^#define\s+(MES_\w+)\s+(-?\d+)[uU]?\s*(?:/[/*].*)?$")


def load_abi_enums() -> dict[str, dict[str, int]]:
    """Parse every ``typedef enum { ... } tag;`` block in ``core/include/mes.h``.

    Enumerators are returned under their C names, keyed by enum tag, so a test can
    compare a binding's mirror table against the header itself. A hand-copied
    expectation would just be another copy able to drift.

    Only explicitly valued enumerators are collected; an implicit one has no
    stable value for a binding to pin.
    """
    enums: dict[str, dict[str, int]] = {}
    current: dict[str, int] | None = None
    for raw in _HEADER_PATH.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if _ENUM_OPEN.match(line):
            current = {}
            continue
        if current is None:
            continue
        closing = _ENUM_CLOSE.match(line)
        if closing is not None:
            enums[closing.group(1)] = current
            current = None
            continue
        member = _ENUM_MEMBER.match(line)
        if member is not None:
            current[member.group(1)] = int(member.group(2))
    return enums


def load_abi_int_macros() -> dict[str, int]:
    """Parse the header's ``#define MES_* <integer>`` macros.

    Macros whose replacement list is an expression rather than a single integer
    literal are skipped: they have no single value to compare against.
    """
    macros: dict[str, int] = {}
    for raw in _HEADER_PATH.read_text(encoding="utf-8").splitlines():
        found = _INT_DEFINE.match(raw.strip())
        if found is not None:
            macros[found.group(1)] = int(found.group(2))
    return macros


def load_abi_struct_layout() -> tuple[dict[str, int], dict[str, dict[str, int]]]:
    """Parse the published layout the core records for the structs a caller allocates.

    Those structs have no runtime ``sizeof`` helper, because a mirror that got
    the layout wrong would already have corrupted memory by the time it could
    call one. The core pins their size and every field offset with compile-time
    assertions instead, and a mirror has to reproduce exactly those positions.
    Reading the numbers from the core rather than restating them is what keeps
    this side from drifting into agreement with itself.

    Returns:
        The recorded size of each struct, and each struct's field offsets.
    """
    source = _LAYOUT_PATH.read_text(encoding="utf-8")
    sizes = {m.group(1): int(m.group(2)) for m in _STRUCT_SIZE.finditer(source)}
    offsets: dict[str, dict[str, int]] = {}
    for match in _STRUCT_OFFSET.finditer(source):
        offsets.setdefault(match.group(1), {})[match.group(2)] = int(match.group(3))
    return sizes, offsets
