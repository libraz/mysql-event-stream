# mysql-event-stream Makefile
# Convenience wrapper for CMake + Node.js + Python build systems

.PHONY: help build test test-tsan benchmark benchmark-socket benchmark-python clean rebuild \
        install uninstall format format-check \
        node-build node-test node-check node-fix \
        py-test py-lint py-format py-typecheck \
        e2e e2e-cpp build-wheel configure lint

# Build directory
BUILD_DIR  := build
BUILD_TYPE ?= Release

# clang-format command (can be overridden: make CLANG_FORMAT=clang-format-18 format)
CLANG_FORMAT ?= clang-format

# Default target
.DEFAULT_GOAL := build

help:
	@echo "mysql-event-stream Build System"
	@echo ""
	@echo "C++ core:"
	@echo "  make build          - Build C++ core (default)"
	@echo "  make test           - Run C++ unit tests"
	@echo "  make test-tsan      - Run non-E2E C++ tests under ThreadSanitizer"
	@echo "  make benchmark      - Measure decode throughput, latency, RSS and per-event bytes"
	@echo "  make benchmark-socket - Measure plaintext ReadExact staging over loopback"
	@echo "  make clean          - Clean build directory"
	@echo "  make rebuild        - Clean and rebuild"
	@echo "  make install        - Install library"
	@echo "  make uninstall      - Uninstall library"
	@echo "  make configure      - Configure CMake (for changing options)"
	@echo "  make format         - Format C++ (clang-format), Node.js (Biome) and Python (ruff)"
	@echo "  make format-check   - Check C++ code formatting (CI)"
	@echo ""
	@echo "Node.js binding:"
	@echo "  make node-build     - Build Node.js native addon"
	@echo "  make node-test      - Run Node.js tests"
	@echo "  make node-check     - Lint & format check with Biome"
	@echo "  make node-fix       - Auto-fix with Biome"
	@echo ""
	@echo "Python binding:"
	@echo "  make py-test        - Run Python tests"
	@echo "  make py-lint        - Lint with ruff"
	@echo "  make py-format      - Format with ruff"
	@echo "  make py-typecheck   - Type check with mypy"
	@echo "  make benchmark-python - Measure ctypes column marshalling cost"
	@echo "  make build-wheel    - Build Python wheel"
	@echo ""
	@echo "Cross-cutting:"
	@echo "  make lint           - Run all linters (C++ format-check + Biome + ruff + mypy)"
	@echo ""
	@echo "E2E tests:"
	@echo "  make e2e            - Run E2E tests (Python, requires Docker)"
	@echo "  make e2e-cpp        - Run C++ E2E tests (requires Docker)"

# ============================================================================
# C++ Core
# ============================================================================

configure:
	cmake -B $(BUILD_DIR) -DCMAKE_BUILD_TYPE=$(BUILD_TYPE) $(CMAKE_OPTIONS)

build: configure
	cmake --build $(BUILD_DIR) --parallel

# Every test runner used here reports success for a run in which nothing ran, so
# no target may take its result from the exit status alone. Both guards live in
# scripts/ so that the workflows and the bindings' own test entry points run the
# same check rather than each restating it. There are two of them rather than
# one because the runners differ in when the count is available: ctest can be
# asked up front how many tests a selection resolves to, so its guard fails fast
# without running anything, while pytest and vitest report an executed count
# only in the summary they print at the end, so theirs has to run the suite,
# keep the output and check it afterwards. Neither runner can be checked the
# other's way.
CTEST_GUARDED := scripts/ctest-guarded.sh
SUITE_GUARDED := $(CURDIR)/scripts/run-suite-guarded.sh

test: build
	$(CTEST_GUARDED) $(BUILD_DIR) --output-on-failure --parallel

test-tsan:
	cmake -B build-tsan -DCMAKE_BUILD_TYPE=Debug -DMES_ENABLE_TSAN=ON $(CMAKE_OPTIONS)
	cmake --build build-tsan --parallel
	$(CTEST_GUARDED) build-tsan --output-on-failure --parallel -E "E2E"

BENCH_DIR    := build-bench
BENCH_STREAMS ?= $(BENCH_DIR)/streams
BENCH_LIB_EXT := $(if $(filter Darwin,$(shell uname -s)),dylib,so)

benchmark:
	cmake -B $(BENCH_DIR) -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=OFF -DMES_BUILD_BENCHMARKS=ON $(CMAKE_OPTIONS)
	cmake --build $(BENCH_DIR) --target mes_benchmark_feed mes_benchmark_workloads mes_benchmark_workloads_mem --parallel
	$(BENCH_DIR)/core/mes_benchmark_feed
	$(BENCH_DIR)/core/mes_benchmark_workloads
	$(BENCH_DIR)/core/mes_benchmark_workloads_mem

# Separate target: the loopback run moves gigabytes and takes minutes.
benchmark-socket:
	cmake -B $(BENCH_DIR) -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=OFF -DMES_BUILD_BENCHMARKS=ON $(CMAKE_OPTIONS)
	cmake --build $(BENCH_DIR) --target mes_benchmark_socket_read --parallel
	$(BENCH_DIR)/core/mes_benchmark_socket_read

# Feeds the Python binding the same event streams the core benchmarks decode.
benchmark-python:
	cmake -B $(BENCH_DIR) -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=OFF -DMES_BUILD_BENCHMARKS=ON $(CMAKE_OPTIONS)
	cmake --build $(BENCH_DIR) --target mes_benchmark_workloads mes-shared --parallel
	mkdir -p $(BENCH_STREAMS)
	$(BENCH_DIR)/core/mes_benchmark_workloads --emit $(BENCH_STREAMS)
	python3 bindings/python/benchmarks/bench_convert_columns.py \
		--streams $(BENCH_STREAMS) --lib $(BENCH_DIR)/core/libmes.$(BENCH_LIB_EXT)

clean:
	rm -rf $(BUILD_DIR)

rebuild: clean build

install: build
	cmake --install $(BUILD_DIR)

uninstall:
	@if [ -f $(BUILD_DIR)/install_manifest.txt ]; then \
		xargs rm -f < $(BUILD_DIR)/install_manifest.txt; \
		echo "Uninstalled."; \
	else \
		echo "No install_manifest.txt found. Run 'make install' first."; \
		exit 1; \
	fi

# ============================================================================
# Formatting
# ============================================================================

format: node-fix py-format
	@echo "Formatting C++ code..."
	@find core/src core/include core/tests bindings/node/src/addon -type f \( -name "*.cpp" -o -name "*.h" \) | xargs $(CLANG_FORMAT) -i
	@echo "Format complete!"

format-check:
	@echo "Checking C++ code formatting..."
	@find core/src core/include core/tests bindings/node/src/addon -type f \( -name "*.cpp" -o -name "*.h" \) | xargs $(CLANG_FORMAT) --dry-run --Werror
	@echo "Format check passed!"

# ============================================================================
# Node.js Binding
# ============================================================================

node-build:
	cd bindings/node && yarn install && yarn build

node-test: node-build
	cd bindings/node && yarn test:ci

node-check:
	cd bindings/node && yarn check && yarn typecheck

node-fix:
	cd bindings/node && yarn check:fix

# ============================================================================
# Python Binding
# ============================================================================

py-test: build
	cd bindings/python && $(SUITE_GUARDED) --runner=pytest rye run pytest

py-lint:
	cd bindings/python && rye run ruff check .

py-format:
	cd bindings/python && rye run ruff format .

py-typecheck:
	cd bindings/python && rye run mypy src tests

build-wheel: build
	cd bindings/python && bash build_wheel.sh

# ============================================================================
# Cross-cutting
# ============================================================================

lint: format-check node-check py-lint py-typecheck

# ============================================================================
# E2E Tests
# ============================================================================

e2e:
	cd bindings/python && $(SUITE_GUARDED) --runner=pytest rye run pytest e2e/tests -v --timeout=120

e2e-cpp: build
	$(CTEST_GUARDED) $(BUILD_DIR) -R "E2E" --output-on-failure --timeout 60
