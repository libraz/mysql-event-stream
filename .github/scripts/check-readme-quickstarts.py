#!/usr/bin/env python3
"""Check the root README examples against the API the bindings ship."""

from __future__ import annotations

import argparse
import ast
import inspect
import re
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
READMES = (ROOT / "README.md", ROOT / "README_ja.md")

# The examples call application helpers the README leaves to the reader.
NODE_HELPERS = """\
declare function loadCheckpoint(): Promise<string | undefined>;
declare function handle(event: unknown): Promise<void>;
declare function saveCheckpoint(gtid: string): Promise<void>;
"""
PYTHON_HELPERS = """\
from mysql_event_stream import CdcStream
async def load_checkpoint(): ...
async def handle(event): ...
async def save_checkpoint(gtid): ...
"""


def extract_first_fence(path: Path, language: str) -> str:
    text = path.read_text(encoding="utf-8")
    match = re.search(rf"```{re.escape(language)}\s*\n(.*?)\n```", text, re.DOTALL)
    if match is None:
        raise RuntimeError(f"{path.name}: no {language} fenced block found")
    source = match.group(1)
    if "CdcStream" not in source:
        raise RuntimeError(f"{path.name}: the {language} example must use CdcStream")
    return source


def check_node() -> None:
    node_dir = ROOT / "bindings" / "node"
    for readme in READMES:
        source = extract_first_fence(readme, "typescript")
        if 'from "@libraz/mysql-event-stream"' not in source:
            raise RuntimeError(
                f"{readme.name}: the example must use the published Node package import"
            )
        compilable_source = source.replace(
            'from "@libraz/mysql-event-stream"', 'from "./dist/index.js"'
        )
        temporary_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                suffix=".ts",
                prefix=".readme-",
                dir=node_dir,
                delete=False,
            ) as temporary:
                temporary_path = Path(temporary.name)
                temporary.write(compilable_source)
                temporary.write("\n")
                temporary.write(NODE_HELPERS)
            subprocess.run(
                [
                    "yarn",
                    "tsc",
                    "--ignoreConfig",
                    "--noEmit",
                    "--target",
                    "ES2022",
                    "--lib",
                    "ES2022,esnext.disposable",
                    "--module",
                    "Node16",
                    "--moduleResolution",
                    "Node16",
                    "--types",
                    "node",
                    "--strict",
                    "--skipLibCheck",
                    str(temporary_path),
                ],
                cwd=node_dir,
                check=True,
            )
        finally:
            if temporary_path is not None:
                temporary_path.unlink(missing_ok=True)

    subprocess.run(
        ["node", "--input-type=module", "--eval", 'await import("./dist/index.js");'],
        cwd=node_dir,
        check=True,
    )


def check_python() -> None:
    sys.path.insert(0, str(ROOT / "bindings" / "python" / "src"))
    from mysql_event_stream import CdcStream

    parameters = inspect.signature(CdcStream.__init__).parameters
    for readme in READMES:
        source = extract_first_fence(readme, "python")
        body = "\n".join(
            "    " + line if line else line for line in source.splitlines()
        )
        program = PYTHON_HELPERS + "async def example():\n" + body + "\n"
        tree = ast.parse(program, f"{readme.name}:python")
        compile(tree, f"{readme.name}:python", "exec")
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Call)
                and getattr(node.func, "id", None) == "CdcStream"
            ):
                unknown = [kw.arg for kw in node.keywords if kw.arg not in parameters]
                if unknown:
                    raise RuntimeError(
                        f"{readme.name}: CdcStream does not accept {unknown}"
                    )
            if (
                isinstance(node, ast.Attribute)
                and getattr(node.value, "id", None) == "stream"
                and not hasattr(CdcStream, node.attr)
            ):
                raise RuntimeError(
                    f"{readme.name}: CdcStream has no attribute {node.attr!r}"
                )


def main() -> None:
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--node", action="store_true")
    group.add_argument("--python", action="store_true")
    args = parser.parse_args()
    if args.node:
        check_node()
    else:
        check_python()


if __name__ == "__main__":
    main()
