#!/usr/bin/env bash
#
# Run a test suite, refusing to report success for a run in which nothing
# executed.
#
# pytest exits 0 when every collected test is skipped (and 5 when it collects
# none); vitest exits 0 when every test is filtered out. Neither can be asked
# up front how many tests a selection resolves to, so the suite has to run and
# the guard checks the runner's own summary afterwards. The patterns below
# exclude a count of zero, so a summary reporting none cannot satisfy them.
#
# Usage: run-suite-guarded.sh --runner=<pytest|vitest> <command> [arguments...]

set -uo pipefail

runner=""
case "${1:-}" in
    --runner=*) runner="${1#--runner=}"; shift ;;
esac

case "$runner" in
    pytest) executed_re='^=+ .*[1-9][0-9]* passed' ;;
    vitest) executed_re='^[[:space:]]*Tests[[:space:]]+[1-9][0-9]* passed' ;;
    *)
        echo "usage: $0 --runner=<pytest|vitest> <command> [arguments...]" >&2
        exit 2
        ;;
esac

if [ "$#" -lt 1 ]; then
    echo "usage: $0 --runner=<pytest|vitest> <command> [arguments...]" >&2
    exit 2
fi

output="$(mktemp)"
trap 'rm -f "$output"' EXIT

# The suite's own output still reaches the caller; tee only keeps a copy for
# the summary check. pipefail carries the runner's status past tee.
"$@" 2>&1 | tee "$output"
status=$?

if [ "$status" -ne 0 ]; then
    echo "[$runner] the test runner exited $status" >&2
    exit "$status"
fi

if ! grep -qE "$executed_re" "$output"; then
    echo "[$runner] no test executed; the run recorded no passing test" >&2
    exit 1
fi
