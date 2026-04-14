# mysql-event-stream Makefile
# Convenience wrapper for CMake + Node.js + Python build systems

.PHONY: help build test clean rebuild install uninstall format format-check \
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
	@echo "  make clean          - Clean build directory"
	@echo "  make rebuild        - Clean and rebuild"
	@echo "  make install        - Install library"
	@echo "  make uninstall      - Uninstall library"
	@echo "  make configure      - Configure CMake (for changing options)"
	@echo "  make format         - Format C++ code with clang-format"
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

test: build
	ctest --test-dir $(BUILD_DIR) --output-on-failure --parallel

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

format:
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
	cd bindings/node && yarn test

node-check:
	cd bindings/node && yarn check

node-fix:
	cd bindings/node && yarn check:fix

# ============================================================================
# Python Binding
# ============================================================================

py-test:
	cd bindings/python && rye run pytest

py-lint:
	cd bindings/python && rye run ruff check .

py-format:
	cd bindings/python && rye run ruff format .

py-typecheck:
	cd bindings/python && rye run mypy src

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
	cd e2e && pytest

e2e-cpp: build
	ctest --test-dir $(BUILD_DIR) -R "E2E" --output-on-failure --timeout 60
