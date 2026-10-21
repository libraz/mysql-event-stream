#!/usr/bin/env bash
# Fail when the built addon depends at load time on a library a release bundles.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ADDON="${1:-$SCRIPT_DIR/../build/Release/mes-node.node}"
# Highest glibc symbol version the published linux-x64 addon may require.
readonly GLIBC_FLOOR="2.39"

if [[ ! -f "$ADDON" ]]; then
  echo "addon not found: $ADDON" >&2
  exit 1
fi

if [[ "$(uname -s)" == "Darwin" ]]; then
  if otool -L "$ADDON" | grep -E '/(opt|usr/local)/|lib(ssl|crypto|z)\.'; then
    echo "addon has an unexpected non-system dynamic dependency" >&2
    exit 1
  fi
elif [[ "$(uname -s)" == "Linux" ]]; then
  if ldd "$ADDON" | grep -E 'lib(ssl|crypto|z|stdc\+\+|gcc_s)\.so'; then
    echo "addon has an unexpected bundled-runtime dependency" >&2
    exit 1
  fi
  required="$(objdump -T "$ADDON" | grep -oE 'GLIBC_[0-9]+(\.[0-9]+)+' | sed 's/^GLIBC_//' | sort -uV | tail -n 1)"
  echo "addon requires glibc $required"
  if [[ "$(printf '%s\n%s\n' "$required" "$GLIBC_FLOOR" | sort -V | tail -n 1)" != "$GLIBC_FLOOR" ]]; then
    echo "addon requires glibc $required, newer than the documented $GLIBC_FLOOR" >&2
    exit 1
  fi
else
  echo "Unsupported addon platform: $(uname -s)" >&2
  exit 1
fi
