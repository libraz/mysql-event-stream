#!/bin/bash
# Cross-platform E2E matrix test runner for mysql-event-stream.
# Runs C++ and Node.js E2E tests against multiple MySQL and MariaDB versions.
#
# Usage:
#   ./e2e/run-matrix.sh                           # All targets
#   ./e2e/run-matrix.sh --only mysql:8.4           # Single target
#   ./e2e/run-matrix.sh --only mysql:8.4,mariadb:11.4  # Multiple targets
#   ./e2e/run-matrix.sh --cpp-only                 # C++ E2E only
#   ./e2e/run-matrix.sh --node-only                # Node.js E2E only
#   ./e2e/run-matrix.sh -- -R "Binlog"             # Pass ctest args
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BUILD_DIR="$PROJECT_ROOT/build"
NODE_DIR="$PROJECT_ROOT/bindings/node"

cd "$SCRIPT_DIR"

# Ensure Volta shims are on PATH (project pins Node via volta.node in package.json)
if [[ -d "$HOME/.volta/bin" ]]; then
    export PATH="$HOME/.volta/bin:$PATH"
fi

# Parse options
CTEST_ARGS=()
TARGETS=()
RUN_CPP=true
RUN_NODE=true

while [[ $# -gt 0 ]]; do
    case "$1" in
        --only)
            IFS=',' read -ra TARGETS <<< "$2"
            shift 2
            ;;
        --cpp-only)
            RUN_NODE=false
            shift
            ;;
        --node-only)
            RUN_CPP=false
            shift
            ;;
        --)
            shift
            CTEST_ARGS+=("$@")
            break
            ;;
        *)
            CTEST_ARGS+=("$1")
            shift
            ;;
    esac
done

# Default: all targets
if [[ ${#TARGETS[@]} -eq 0 ]]; then
    TARGETS=("mysql:8.4" "mysql:9.1" "mariadb:10.11" "mariadb:11.4")
fi

# Pre-flight checks
if [[ "$RUN_CPP" == true && ! -d "$BUILD_DIR" ]]; then
    echo "ERROR: Build directory not found at $BUILD_DIR"
    echo "Run: cmake -B build && cmake --build build --parallel"
    exit 1
fi

if [[ "$RUN_NODE" == true && ! -f "$NODE_DIR/build/Release/mes-node.node" ]]; then
    echo "WARNING: Node.js native addon not found. Skipping Node.js E2E tests."
    echo "Build with: cd bindings/node && yarn install && yarn build"
    RUN_NODE=false
fi

echo "============================================="
echo " mysql-event-stream E2E Matrix Test"
echo " Targets: ${TARGETS[*]}"
echo " C++ E2E: $RUN_CPP  |  Node.js E2E: $RUN_NODE"
echo "============================================="
echo ""

overall_pass=0
overall_fail=0
declare -a summary_lines

# Stop all test containers
stop_all_containers() {
    # C++ E2E containers (port 13308)
    docker compose -f "docker/docker-compose.yml" down -v 2>/dev/null || true
    docker compose -f "docker/docker-compose.mariadb.yml" down -v 2>/dev/null || true
    # Node.js E2E containers (port 13307)
    docker compose -f "$NODE_DIR/e2e/docker/docker-compose.yml" down -v 2>/dev/null || true
    docker compose -f "$NODE_DIR/e2e/docker/docker-compose.mariadb.yml" down -v 2>/dev/null || true
}

for target in "${TARGETS[@]}"; do
    flavor="${target%%:*}"
    version="${target##*:}"

    echo "============================================="
    echo " $flavor $version"
    echo "============================================="

    # Select compose files and env vars
    if [[ "$flavor" == "mariadb" ]]; then
        cpp_compose="docker/docker-compose.mariadb.yml"
        node_compose="$NODE_DIR/e2e/docker/docker-compose.mariadb.yml"
        export MARIADB_VERSION="$version"
        export DB_FLAVOR="mariadb"
    else
        cpp_compose="docker/docker-compose.yml"
        node_compose="$NODE_DIR/e2e/docker/docker-compose.yml"
        export MYSQL_VERSION="$version"
        export DB_FLAVOR="mysql"
        unset MARIADB_VERSION 2>/dev/null || true
    fi

    stop_all_containers

    target_pass=true

    # ---- C++ E2E ----
    if [[ "$RUN_CPP" == true ]]; then
        echo ""
        echo "  [C++] Starting $flavor $version on port 13308..."
        docker compose -f "$cpp_compose" up -d --wait --wait-timeout 120 2>&1

        if [[ $? -ne 0 ]]; then
            echo "  [C++] FAIL: Could not start container"
            target_pass=false
        else
            echo "  [C++] Running E2E tests..."
            cd "$BUILD_DIR"
            DB_FLAVOR="$DB_FLAVOR" ctest -R "E2E" \
                --output-on-failure \
                --timeout 60 \
                "${CTEST_ARGS[@]+"${CTEST_ARGS[@]}"}" \
                2>&1
            if [[ $? -ne 0 ]]; then
                target_pass=false
            fi
            cd "$SCRIPT_DIR"
        fi

        docker compose -f "$cpp_compose" down -v 2>/dev/null || true
    fi

    # ---- Node.js E2E ----
    if [[ "$RUN_NODE" == true ]]; then
        echo ""
        echo "  [Node] Starting $flavor $version on port 13307..."
        docker compose -f "$node_compose" up -d --wait --wait-timeout 120 2>&1

        if [[ $? -ne 0 ]]; then
            echo "  [Node] FAIL: Could not start container"
            target_pass=false
        else
            echo "  [Node] Running E2E tests..."
            cd "$NODE_DIR"
            DB_FLAVOR="$DB_FLAVOR" yarn test:e2e 2>&1
            if [[ $? -ne 0 ]]; then
                target_pass=false
            fi
            cd "$SCRIPT_DIR"
        fi

        docker compose -f "$node_compose" down -v 2>/dev/null || true
    fi

    # Collect summary
    if [[ "$target_pass" == true ]]; then
        summary_lines+=("PASS  $flavor $version")
        ((overall_pass++))
    else
        summary_lines+=("FAIL  $flavor $version")
        ((overall_fail++))
    fi

    echo ""
done

# Final summary
echo "============================================="
echo " Matrix Summary"
echo "============================================="
for line in "${summary_lines[@]}"; do
    echo "  $line"
done
echo ""
echo "  Pass: $overall_pass / $((overall_pass + overall_fail))"

if [[ $overall_fail -gt 0 ]]; then
    echo ""
    echo "FAILED: $overall_fail target(s) had failures."
    exit 1
fi

echo ""
echo "All targets passed!"
exit 0
