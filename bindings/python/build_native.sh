#!/usr/bin/env bash
# Build the native ctypes library before hatchling creates a wheel.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
PYTHON_PKG="$SCRIPT_DIR/src/mysql_event_stream"

platform="$(uname -s | tr '[:upper:]' '[:lower:]')"
arch="$(uname -m)"
target="${MACOSX_DEPLOYMENT_TARGET:-native}"
BUILD_DIR="$PROJECT_ROOT/build-wheel-$platform-$arch-$target"

cmake_args=(
  -S "$PROJECT_ROOT"
  -B "$BUILD_DIR"
  -DCMAKE_BUILD_TYPE=Release
  -DBUILD_TESTING=OFF
  -DMES_OPENSSL_STATIC=ON
  -DMES_ZLIB_STATIC=ON
  -DMES_PORTABLE_RUNTIME=ON
)

if [[ -n "${MES_DEPS_PREFIX:-}" ]]; then
  cmake_args+=(
    "-DCMAKE_PREFIX_PATH=$MES_DEPS_PREFIX"
    "-DOPENSSL_ROOT_DIR=$MES_DEPS_PREFIX"
    "-DZLIB_ROOT=$MES_DEPS_PREFIX"
  )
fi

if [[ "$(uname -s)" == "Darwin" ]]; then
  if [[ -z "${MACOSX_DEPLOYMENT_TARGET:-}" ]]; then
    echo "MACOSX_DEPLOYMENT_TARGET must be set for a distributable macOS wheel" >&2
    exit 1
  fi
  cmake_args+=("-DCMAKE_OSX_DEPLOYMENT_TARGET=$MACOSX_DEPLOYMENT_TARGET")
fi

cmake "${cmake_args[@]}"
cmake --build "$BUILD_DIR" --config Release --parallel

if [[ "$(uname -s)" == "Darwin" ]]; then
  cp "$BUILD_DIR/core/libmes.dylib" "$PYTHON_PKG/libmes.dylib"
  install_name_tool -id @loader_path/libmes.dylib "$PYTHON_PKG/libmes.dylib"
  if otool -L "$PYTHON_PKG/libmes.dylib" | grep -E '/(opt|usr/local)/|lib(ssl|crypto|z)\.'; then
    echo "wheel library has an unexpected non-system dynamic dependency" >&2
    exit 1
  fi
elif [[ "$(uname -s)" == "Linux" ]]; then
  cp "$BUILD_DIR/core/libmes.so" "$PYTHON_PKG/libmes.so"
  if ldd "$PYTHON_PKG/libmes.so" | grep -E 'lib(ssl|crypto|z|stdc\+\+|gcc_s)\.so'; then
    echo "wheel library has an unexpected bundled-runtime dependency" >&2
    exit 1
  fi
else
  echo "Unsupported wheel platform: $(uname -s)" >&2
  exit 1
fi
