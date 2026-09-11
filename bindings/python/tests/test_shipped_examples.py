"""Pin the start position the shipped examples configure.

The examples are copied into services, so a start position one of them
configures is a recommendation. An empty GTID set is the single value that turns
a live-tail example into a replay of every binlog the server still retains, so
what each of them has to hand the stream is an omitted ``start_gtid`` -- ``None``
snapshots the current server position. Nothing else in the suite reads the
example sources, and they parse rather than a transcription of them, so the
assertions cannot drift away from what a reader copies.
"""

from __future__ import annotations

import ast
from pathlib import Path

_EXAMPLES_DIR = Path(__file__).resolve().parents[1] / "examples"


def _examples() -> list[tuple[str, ast.Module]]:
    """Parse every shipped example.

    Raises:
        AssertionError: If the examples directory reads as empty, which would
            let every assertion below pass by inspecting nothing.
    """
    paths = sorted(_EXAMPLES_DIR.glob("*.py"))
    assert paths, f"no examples found to check under {_EXAMPLES_DIR}"
    return [(path.name, ast.parse(path.read_text(encoding="utf-8"))) for path in paths]


def _is_empty_string(node: ast.expr | None) -> bool:
    """Whether an expression is the empty string literal."""
    return isinstance(node, ast.Constant) and node.value == ""


def _keyword(call: ast.Call, name: str) -> ast.expr | None:
    """The value supplied for a keyword argument, or None if it was not."""
    for keyword in call.keywords:
        if keyword.arg == name:
            return keyword.value
    return None


def _calls(tree: ast.Module) -> list[ast.Call]:
    """Every call expression in a parsed example."""
    return [node for node in ast.walk(tree) if isinstance(node, ast.Call)]


def test_no_example_passes_the_empty_gtid_set_as_a_start_position() -> None:
    offenders = sorted(
        name
        for name, tree in _examples()
        for call in _calls(tree)
        if _is_empty_string(_keyword(call, "start_gtid"))
    )
    assert offenders == [], (
        "an empty start_gtid asks the server for every retained binlog; "
        f"omit it instead: {offenders}"
    )


def test_no_example_defaults_a_gtid_command_line_option_to_the_empty_string() -> None:
    """An option defaulting to '' forwards the empty set when the flag is absent.

    Caught separately from the literal above because the value reaches the
    stream through a parsed argument rather than the call site, which is the
    shape a reader is least likely to notice.
    """
    offenders = sorted(
        name
        for name, tree in _examples()
        for call in _calls(tree)
        if isinstance(call.func, ast.Attribute)
        and call.func.attr == "add_argument"
        and any(
            isinstance(arg, ast.Constant) and isinstance(arg.value, str) and "gtid" in arg.value
            for arg in call.args
        )
        and _is_empty_string(_keyword(call, "default"))
    )
    assert offenders == [], (
        "a gtid option defaulting to '' requests every retained binlog when the "
        f"flag is omitted; default to None instead: {offenders}"
    )
