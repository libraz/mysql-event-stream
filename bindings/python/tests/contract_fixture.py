"""Loader for the cross-binding contract table shared with the Node binding."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent

_CONTRACT_PATH = _REPO_ROOT / "core" / "contracts" / "bindings.json"

_HEADER_PATH = _REPO_ROOT / "core" / "include" / "mes.h"


def load_binding_contract() -> dict[str, Any]:
    """Read ``core/contracts/bindings.json``, the source of truth both bindings mirror."""
    return json.loads(_CONTRACT_PATH.read_text(encoding="utf-8"))


def load_header_field_doc(field: str) -> str:
    """Read the doc comment ``core/include/mes.h`` attaches to a config field.

    The header is the published claim a C caller reads, so a range the contract
    states has to match the words shipped with the ABI. Parsed rather than
    restated: a hand-copied expectation would be one more copy free to drift.
    Every step that could stop matching raises instead of returning an empty
    result a test would pass over.

    Args:
        field: Name of the struct field, as the header declares it.

    Returns:
        The comment block immediately above the declaration, as one line.

    Raises:
        RuntimeError: If the header cannot be read, does not declare the field,
            or carries no doc comment for it.
    """
    lines = _HEADER_PATH.read_text(encoding="utf-8").splitlines()
    if len(lines) <= 1:
        raise RuntimeError("core/include/mes.h is not readable")

    declaration = next(
        (index for index, line in enumerate(lines) if line.strip().endswith(f" {field};")),
        None,
    )
    if declaration is None:
        raise RuntimeError(f"mes.h does not declare {field}")

    doc: list[str] = []
    for line in (lines[index].strip() for index in range(declaration - 1, -1, -1)):
        if not line.startswith("/**") and not line.startswith("*"):
            break
        doc.insert(0, line)
        if line.startswith("/**"):
            break
    text = " ".join(doc)
    if not text:
        raise RuntimeError(f"mes.h does not document {field}")
    return text
