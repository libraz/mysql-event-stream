"""Loader for the cross-binding contract table shared with the Node binding."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

_CONTRACT_PATH = (
    Path(__file__).resolve().parent.parent.parent.parent / "core" / "contracts" / "bindings.json"
)


def load_binding_contract() -> dict[str, Any]:
    """Read ``core/contracts/bindings.json``, the source of truth both bindings mirror."""
    return json.loads(_CONTRACT_PATH.read_text(encoding="utf-8"))
