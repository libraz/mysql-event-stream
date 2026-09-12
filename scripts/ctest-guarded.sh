#!/usr/bin/env bash
#
# Run a CTest selection, refusing to report success for a run in which nothing
# executed.
#
# CTest prints "No tests were found!!!" and exits 0 when a selection matches no
# test, so a run that executed nothing is indistinguishable from a run that
# passed. CTest can be asked up front how many tests a selection resolves to,
# so the guard runs first and fails without starting a suite.
#
# Usage: ctest-guarded.sh <build-dir> [ctest arguments...]
#
# Every argument after the build directory is passed through to CTest
# unchanged, so callers keep their own selection and flags.

set -euo pipefail

if [ "$#" -lt 1 ]; then
    echo "usage: $0 <build-dir> [ctest arguments...]" >&2
    exit 2
fi

build_dir="$1"
shift

# The listing goes to a file rather than straight into grep: a -q that stops at
# the first match closes the pipe under the enumerating process, and the SIGPIPE
# that follows would be read as an empty selection.
listing="$(mktemp)"
trap 'rm -f "$listing"' EXIT

if ! ctest --test-dir "$build_dir" -N "$@" > "$listing" 2>&1; then
    cat "$listing" >&2
    echo "Could not enumerate the selection: ctest --test-dir $build_dir -N $*" >&2
    exit 1
fi

if ! grep -qE 'Total Tests: [1-9]' "$listing"; then
    echo "No test matched the selection; refusing to report success." >&2
    echo "  ctest --test-dir $build_dir $*" >&2
    exit 1
fi

rm -f "$listing"
trap - EXIT
exec ctest --test-dir "$build_dir" "$@"
