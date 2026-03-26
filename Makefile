.PHONY: build test install uninstall clean build-wheel build-node

BUILD_TYPE ?= Release
BUILD_DIR  := build

build:
	cmake -B $(BUILD_DIR) -DCMAKE_BUILD_TYPE=$(BUILD_TYPE)
	cmake --build $(BUILD_DIR) --parallel

test: build
	ctest --test-dir $(BUILD_DIR) --output-on-failure --parallel

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

clean:
	rm -rf $(BUILD_DIR)

build-wheel: build
	cd bindings/python && bash build_wheel.sh

build-node:
	cd bindings/node && yarn install && yarn build
