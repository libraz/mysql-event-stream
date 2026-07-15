#!/usr/bin/env python3
"""Compile and import-smoke the root README quick-start snippets."""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
READMES = (ROOT / "README.md", ROOT / "README_ja.md")


def extract_first_fence(path: Path, language: str) -> str:
    text = path.read_text(encoding="utf-8")
    match = re.search(rf"```{re.escape(language)}\s*\n(.*?)\n```", text, re.DOTALL)
    if match is None:
        raise RuntimeError(f"{path.name}: no {language} fenced block found")
    source = match.group(1)
    if "CdcEngine" not in source or "MesEngine" in source:
        raise RuntimeError(f"{path.name}: quick start must import CdcEngine")
    if language == "typescript" and 'from "@libraz/mysql-event-stream"' not in source:
        raise RuntimeError(f"{path.name}: quick start must use the published Node package import")
    if language == "python" and "from mysql_event_stream import CdcEngine" not in source:
        raise RuntimeError(f"{path.name}: quick start must use the published Python package import")
    return source


def check_node() -> None:
    node_dir = ROOT / "bindings" / "node"
    for readme in READMES:
        source = extract_first_fence(readme, "typescript")
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
                temporary.write("declare const binlogChunk: Uint8Array;\n")
                temporary.write(compilable_source)
                temporary.write("\n")
            subprocess.run(
                [
                    "yarn",
                    "tsc",
                    "--ignoreConfig",
                    "--noEmit",
                    "--target",
                    "ES2022",
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
        [
            "node",
            "--input-type=module",
            "--eval",
            (
                'const { CdcEngine } = await import("./dist/index.js"); '
                "const engine = new CdcEngine(); "
                "engine.feed(new Uint8Array()); "
                "engine.destroy();"
            ),
        ],
        cwd=node_dir,
        check=True,
    )


def check_python() -> None:
    python_dir = ROOT / "bindings" / "python"
    env = os.environ.copy()
    source_path = str(python_dir / "src")
    env["PYTHONPATH"] = os.pathsep.join(filter(None, (source_path, env.get("PYTHONPATH", ""))))

    for readme in READMES:
        source = extract_first_fence(readme, "python")
        compile(source, f"{readme.name}:python", "exec")
        subprocess.run(
            [sys.executable, "-c", "binlog_chunk = b''\n" + source],
            cwd=python_dir,
            env=env,
            check=True,
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
