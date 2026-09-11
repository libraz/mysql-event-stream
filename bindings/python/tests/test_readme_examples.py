"""Pin the user-facing documentation against the shipped Python API.

Nothing else in the suite reads the README files, so a quickstart that leaks the
native client or names a method the class does not have is a defect a user hits
on their first copy-paste and the test run stays green. These tests parse the
documentation itself rather than a transcription of it, so the assertions cannot
drift away from what a reader actually sees.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from mysql_event_stream import BinlogClient, CdcEngine, CdcStream

_REPO_ROOT = Path(__file__).resolve().parents[3]

# Every README that documents the Python surface: the root pair carries the
# cross-language overview, the binding README the quickstart.
_README_PATHS = (
    _REPO_ROOT / "README.md",
    _REPO_ROOT / "README_ja.md",
    _REPO_ROOT / "bindings" / "python" / "README.md",
)

_PUBLIC_CLASSES = {
    "BinlogClient": BinlogClient,
    "CdcEngine": CdcEngine,
    "CdcStream": CdcStream,
}

_PYTHON_BLOCK = re.compile(r"```python\n(.*?)^```", re.DOTALL | re.MULTILINE)
_QUALIFIED_CALL = re.compile(r"`([A-Z][A-Za-z]*)\.([a-z_][a-z0-9_]*)\(\)`")

_BINDING_README = "bindings/python/README.md"
_PACKAGE_INIT = _REPO_ROOT / "bindings" / "python" / "src" / "mysql_event_stream" / "__init__.py"
_BUILD_SCRIPT = _REPO_ROOT / "bindings" / "python" / "build_native.sh"


def _section(text: str, heading: str) -> str | None:
    """The body a ``## heading`` opens, up to the next heading of the same level.

    ``None`` means the document has no such heading, which distinguishes a
    document that never makes the claim from one that makes it wrongly.
    """
    marker = f"\n## {heading}\n"
    start = text.find(marker)
    if start == -1:
        return None
    body = text[start + 1 :]
    end = body.find("\n## ", 1)
    return body if end == -1 else body[:end]


def _documents() -> list[tuple[str, str]]:
    """Read every documented surface, failing rather than skipping if one moved.

    Paths are reported relative to the repository root: three of these files
    share the base name README.md, so a bare name cannot say which one failed.
    """
    return [
        (str(path.relative_to(_REPO_ROOT)), path.read_text(encoding="utf-8"))
        for path in _README_PATHS
    ]


def _python_blocks() -> list[tuple[str, str]]:
    """Every fenced Python example across the documented surfaces."""
    return [(path, block) for path, text in _documents() for block in _PYTHON_BLOCK.findall(text)]


class TestDocumentedExamplesParse:
    """A published example that is not valid Python cannot be copy-pasted."""

    def test_every_python_block_parses(self) -> None:
        blocks = _python_blocks()
        # Without this the regex could silently stop matching and every
        # assertion below would pass over an empty list.
        assert blocks, "no fenced Python examples found in the documented READMEs"
        for path, block in blocks:
            try:
                ast.parse(block)
            except SyntaxError as exc:  # pragma: no cover - failure path
                pytest.fail(f"{path}: example does not parse: {exc}")


class TestStreamExamplesScopeTheStream:
    """``async for`` never finalizes the iterator it borrows.

    An example that iterates a stream it never scoped leaves the native client,
    its reader thread, and the socket alive on any early exit, and puts the
    post-iteration checkpoint out of reach because nothing holds a reference.
    """

    @staticmethod
    def _stream_constructions(tree: ast.AST) -> list[ast.Call]:
        return [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "CdcStream"
        ]

    def test_no_example_iterates_an_unscoped_stream(self) -> None:
        checked = 0
        for path, block in _python_blocks():
            tree = ast.parse(block)
            constructions = self._stream_constructions(tree)
            iterations = [node for node in ast.walk(tree) if isinstance(node, ast.AsyncFor)]
            if not constructions or not iterations:
                continue
            checked += 1

            for loop in iterations:
                assert not (
                    isinstance(loop.iter, ast.Call)
                    and isinstance(loop.iter.func, ast.Name)
                    and loop.iter.func.id == "CdcStream"
                ), f"{path}: iterates a CdcStream built in the loop header, so it is never closed"

            scoped = {
                id(item.context_expr)
                for node in ast.walk(tree)
                if isinstance(node, ast.AsyncWith)
                for item in node.items
            }
            for construction in constructions:
                assert id(construction) in scoped, (
                    f"{path}: constructs a CdcStream outside `async with`, "
                    "so nothing releases the native client"
                )

        assert checked, "no documented example both builds and iterates a CdcStream"


class TestDocumentedMethodsExist:
    """A method named in the documentation must exist on the class named with it."""

    def test_every_qualified_mention_resolves(self) -> None:
        mentions: set[tuple[str, str]] = set()
        for path, text in _documents():
            for class_name, method in _QUALIFIED_CALL.findall(text):
                if class_name not in _PUBLIC_CLASSES:
                    continue
                mentions.add((class_name, method))
                assert hasattr(_PUBLIC_CLASSES[class_name], method), (
                    f"{path} documents {class_name}.{method}(), which {class_name} does not expose"
                )

        # The cancellation entry point is the one the documentation has
        # previously attributed to the wrong class, so its presence is part of
        # the assertion rather than incidental.
        assert ("BinlogClient", "stop") in mentions, (
            "the documentation no longer attributes stop() to BinlogClient; "
            "cancellation guidance must name the class that owns the method"
        )


class TestEngineExamplesScopeTheEngine:
    """``CdcEngine`` holds native state that ``__del__`` releases whenever it runs.

    The class docstring prescribes the with-statement, so a binding-README
    example that constructs an engine and walks away teaches the one pattern the
    class itself tells callers not to use.
    """

    def test_every_engine_example_uses_the_with_statement(self) -> None:
        checked = 0
        for path, block in _python_blocks():
            if path != _BINDING_README:
                continue
            tree = ast.parse(block)
            constructions = [
                node
                for node in ast.walk(tree)
                if isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "CdcEngine"
            ]
            if not constructions:
                continue
            checked += 1

            scoped = {
                id(item.context_expr)
                for node in ast.walk(tree)
                if isinstance(node, ast.With)
                for item in node.items
            }
            for construction in constructions:
                assert id(construction) in scoped, (
                    f"{path}: constructs a CdcEngine outside `with`, so the native "
                    "engine lives until the garbage collector gets to it"
                )

        assert checked, f"no documented example in {_BINDING_README} constructs a CdcEngine"


class TestExportsAreDocumented:
    """A symbol a user can import needs a row in the list they read first."""

    @staticmethod
    def _exported_names() -> set[str]:
        """Every name ``__init__.py`` re-exports, read from the file itself.

        Derived rather than transcribed: ``__all__`` omits the deprecated
        helpers, which are still importable, and a hardcoded list here would be
        a second place to drift.
        """
        tree = ast.parse(_PACKAGE_INIT.read_text(encoding="utf-8"))
        return {
            alias.asname or alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
            for alias in node.names
        }

    def test_the_exports_table_covers_every_re_export(self) -> None:
        names = self._exported_names()
        assert names, "no re-exported names found in __init__.py"

        text = (_REPO_ROOT / _BINDING_README).read_text(encoding="utf-8")
        table = _section(text, "Exports")
        assert table is not None, f"{_BINDING_README} must keep its Exports section"
        for name in sorted(names):
            assert f"`{name}`" in table, (
                f"{_BINDING_README} exports {name} but the Exports table does not list it"
            )

    def test_deprecated_exports_are_flagged_as_such(self) -> None:
        from mysql_event_stream import types

        deprecated = {
            name
            for name in self._exported_names()
            if (doc := getattr(getattr(types, name, None), "__doc__", None))
            and ".. deprecated::" in doc
        }
        assert deprecated, "no exported symbol carries a deprecation notice"

        table = _section((_REPO_ROOT / _BINDING_README).read_text(encoding="utf-8"), "Exports")
        assert table is not None
        for name in sorted(deprecated):
            row = next((line for line in table.splitlines() if f"`{name}`" in line), None)
            assert row is not None
            assert "eprecated" in row, (
                f"{name} is deprecated in types.py but its Exports row does not say so"
            )


class TestDependencyClaimMatchesTheBuild:
    """The wheel's stated dependency set is a link-configuration fact."""

    def test_every_statically_linked_library_is_named(self) -> None:
        script = _BUILD_SCRIPT.read_text(encoding="utf-8")
        libraries = set(re.findall(r"-DMES_([A-Z0-9]+)_STATIC=ON", script))
        assert libraries, "build_native.sh statically links nothing"

        text = (_REPO_ROOT / _BINDING_README).read_text(encoding="utf-8")
        claims = [
            line
            for line in text.splitlines()
            if line.startswith("- **") and "libmysqlclient" in line
        ]
        assert claims, f"{_BINDING_README} no longer states its native dependencies"
        for claim in claims:
            for library in sorted(libraries):
                assert library.lower() in claim.lower(), (
                    f"{_BINDING_README} states its dependencies without naming {library}, "
                    "which build_native.sh links statically into the wheel"
                )


class TestColumnNamesFeatureIsReachable:
    """``CdcStream`` opens the metadata connection; ``CdcEngine`` does not."""

    def test_the_enabling_call_is_named_where_the_feature_is_advertised(self) -> None:
        assert hasattr(CdcEngine, "enable_metadata")

        advertising = [
            (path, text)
            for path, text in _documents()
            if "metadata connection" in text or "メタデータ接続" in text
        ]
        assert advertising, "no documented surface advertises the column-names feature"
        for path, text in advertising:
            assert "enable_metadata" in text, (
                f"{path} advertises resolved column names without naming enable_metadata, "
                "which a CdcEngine consumer has to call"
            )


class TestQuickstartLifecycle:
    """The shape the quickstart prescribes must release and checkpoint correctly."""

    @pytest.mark.asyncio
    async def test_scope_exit_releases_handles_and_keeps_the_checkpoint(self) -> None:
        stream = CdcStream(host="127.0.0.1", user="replicator", password="secret")
        client = MagicMock()
        client.current_gtid = "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5"
        stream._client = client
        stream._engine = MagicMock()

        async with stream:
            pass

        assert stream._client is None, "leaving the scope must release the native client"
        assert stream._engine is None, "leaving the scope must release the native engine"
        client.close.assert_called_once()
        assert stream.current_gtid == "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5", (
            "the checkpoint must survive the scope; callers persist it afterwards"
        )
