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
