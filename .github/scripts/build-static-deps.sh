#!/usr/bin/env bash
# Build pinned PIC static dependencies for portable Node/Python artifacts.
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 PREFIX" >&2
  exit 2
fi

readonly PREFIX="$1"
readonly OPENSSL_VERSION="3.5.7"
readonly OPENSSL_SHA256="a8c0d28a529ca480f9f36cf5792e2cd21984552a3c8e4aa11a24aa31aeac98e8"
readonly ZLIB_VERSION="1.3.2"
readonly ZLIB_SHA256="bb329a0a2cd0274d05519d61c667c062e06990d72e125ee2dfa8de64f0119d16"
readonly MARKER="$PREFIX/.mes-static-deps-$OPENSSL_VERSION-$ZLIB_VERSION"

if [[ -f "$MARKER" ]]; then
  echo "Pinned static dependencies already exist at $PREFIX"
  exit 0
fi

work_dir="$(mktemp -d)"
trap 'rm -rf "$work_dir"' EXIT
mkdir -p "$PREFIX"

verify_sha256() {
  local expected="$1"
  local path="$2"
  if [[ "$(uname -s)" == "Darwin" ]]; then
    echo "$expected  $path" | shasum -a 256 -c
  else
    echo "$expected  $path" | sha256sum -c
  fi
}

jobs=2
if command -v nproc >/dev/null 2>&1; then
  jobs="$(nproc)"
elif command -v sysctl >/dev/null 2>&1; then
  jobs="$(sysctl -n hw.ncpu)"
fi

cd "$work_dir"
curl --fail --location --retry 3 --output "zlib-$ZLIB_VERSION.tar.gz" \
  "https://zlib.net/zlib-$ZLIB_VERSION.tar.gz"
verify_sha256 "$ZLIB_SHA256" "zlib-$ZLIB_VERSION.tar.gz"
tar -xzf "zlib-$ZLIB_VERSION.tar.gz"
cd "zlib-$ZLIB_VERSION"
CFLAGS="${CFLAGS:-} -fPIC" ./configure --static --prefix="$PREFIX"
make -j"$jobs"
make install

cd "$work_dir"
curl --fail --location --retry 3 --output "openssl-$OPENSSL_VERSION.tar.gz" \
  "https://github.com/openssl/openssl/releases/download/openssl-$OPENSSL_VERSION/openssl-$OPENSSL_VERSION.tar.gz"
verify_sha256 "$OPENSSL_SHA256" "openssl-$OPENSSL_VERSION.tar.gz"
tar -xzf "openssl-$OPENSSL_VERSION.tar.gz"
cd "openssl-$OPENSSL_VERSION"
./config no-shared no-tests no-docs -fPIC --prefix="$PREFIX" --libdir=lib
make -j"$jobs"
make install_sw

test -f "$PREFIX/lib/libz.a"
test -f "$PREFIX/lib/libssl.a"
test -f "$PREFIX/lib/libcrypto.a"
touch "$MARKER"
