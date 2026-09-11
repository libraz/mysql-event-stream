"""Pin every numeric constant this binding re-declares against the C ABI header.

``core/include/mes.h`` is the only authority here; nothing below restates a value,
so a drift on either side fails instead of agreeing with a third copy of the same
number. Mirrors ``bindings/node/tests/abi-constants.test.ts``.

This module covers the tables the binding declares. An ABI value spelled as a bare
literal at a call site is pinned against the header where that call's behaviour is
asserted, such as the start-position mode in ``test_client.py``.
"""

from __future__ import annotations

from enum import Enum
from typing import NamedTuple

import pytest

from mysql_event_stream import _ffi
from mysql_event_stream.logging import LogLevel
from mysql_event_stream.types import EventType, MesErrorCode, ServerFlavor, SslMode

from .abi_fixture import load_abi_enums, load_abi_int_macros

abi_enums = load_abi_enums()
abi_macros = load_abi_int_macros()


class MirroredEnum(NamedTuple):
    """One Python enum and the C enum whose values it mirrors."""

    label: str
    """How the enum is spelled in this binding's public API."""

    tag: str
    """Tag of the ``typedef enum`` in ``core/include/mes.h``."""

    prefixes: tuple[str, ...]
    """Enumerator prefixes to strip, tried in order. ``mes_error_t`` needs two,
    because it spells success as ``MES_OK`` and failures as ``MES_ERR_*``."""

    enum: type[Enum]


MIRRORED_ENUMS = (
    MirroredEnum("MesErrorCode", "mes_error_t", ("MES_ERR_", "MES_"), MesErrorCode),
    MirroredEnum("SslMode", "mes_ssl_mode_t", ("MES_SSL_",), SslMode),
    MirroredEnum("ServerFlavor", "mes_server_flavor_t", ("MES_SERVER_FLAVOR_",), ServerFlavor),
    MirroredEnum("EventType", "mes_event_type_t", ("MES_EVENT_",), EventType),
    MirroredEnum("LogLevel", "mes_log_level_t", ("MES_LOG_",), LogLevel),
)

# The enums and macros the private FFI layer re-declares under their exact C
# names, so they are compared name for name rather than through a pairing key.
FFI_MIRRORED_TAGS = ("mes_error_t", "mes_log_level_t", "mes_col_type_t")
FFI_MIRRORED_MACROS = ("MES_ABI_VERSION",)


def _pairing_key(name: str, prefixes: tuple[str, ...]) -> str:
    """Reduce a name to the form that pairs a C enumerator with its Python member.

    ``MES_SERVER_FLAVOR_MARIADB`` and ``MARIADB`` meet without either side's
    naming convention being written down a second time.
    """
    bare = name
    for prefix in prefixes:
        if bare.startswith(prefix):
            bare = bare[len(prefix) :]
            break
    return bare.replace("_", "").lower()


def _header_constants() -> dict[str, int]:
    """Every integer constant the header declares, enumerators and macros alike."""
    declared = dict(abi_macros)
    for members in abi_enums.values():
        declared.update(members)
    return declared


@pytest.mark.parametrize("mirror", MIRRORED_ENUMS, ids=lambda m: m.label)
def test_enum_mirrors_the_header_declaration(mirror: MirroredEnum) -> None:
    """A mirrored enum declares exactly the header's enumerators, with its values."""
    declared = abi_enums.get(mirror.tag)
    assert declared, f"{mirror.tag} not declared in core/include/mes.h"

    remaining = {
        _pairing_key(name, ()): (name, member.value)
        for name, member in mirror.enum.__members__.items()
    }
    # Keyed by both names at once, so a mismatch reports which C constant and
    # which Python member disagree.
    expected: dict[str, int] = {}
    actual: dict[str, int] = {}
    for c_name, c_value in declared.items():
        key = _pairing_key(c_name, mirror.prefixes)
        member = remaining.pop(key, None)
        label = f"{c_name} ({mirror.label}.{member[0] if member else '<not declared>'})"
        expected[label] = c_value
        if member is not None:
            actual[label] = member[1]

    # Both directions at once: a wrong value and an enumerator the enum never
    # mirrored are the same failure here.
    assert actual == expected
    assert not remaining, (
        f"{mirror.label} members {mirror.tag} does not declare: "
        f"{sorted(name for name, _ in remaining.values())}"
    )


def test_ffi_declares_every_constant_it_mirrors() -> None:
    """The FFI layer carries each mirrored enumerator and macro at the header's value."""
    expected: dict[str, int] = {name: abi_macros[name] for name in FFI_MIRRORED_MACROS}
    for tag in FFI_MIRRORED_TAGS:
        declared = abi_enums.get(tag)
        assert declared, f"{tag} not declared in core/include/mes.h"
        expected.update(declared)

    actual = {name: getattr(_ffi, name) for name in expected if hasattr(_ffi, name)}
    assert actual == expected


def test_ffi_declares_no_constant_the_header_does_not() -> None:
    """A constant the header dropped or never had must not survive in the FFI layer."""
    declared = _header_constants()
    mirrored = {
        name: value
        for name, value in vars(_ffi).items()
        if name.startswith("MES_") and isinstance(value, int)
    }
    assert mirrored, "no MES_* integer constants found in the FFI layer"
    assert sorted(name for name in mirrored if name not in declared) == []
    assert mirrored == {name: declared[name] for name in mirrored}
