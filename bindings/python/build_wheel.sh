#!/usr/bin/env bash
# Build a validated platform wheel through cibuildwheel.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

cd "$PROJECT_ROOT"
export CIBW_ARCHS="${CIBW_ARCHS:-native}"
python3 -m cibuildwheel \
  --config-file "$SCRIPT_DIR/pyproject.toml" \
  --output-dir "$SCRIPT_DIR/wheelhouse" \
  "$SCRIPT_DIR"
