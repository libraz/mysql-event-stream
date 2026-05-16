<!-- markdownlint-disable MD024 -->
# Changelog

All notable changes to mysql-event-stream will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

**Note**: For detailed release information, see [docs/releases/](docs/releases/).

## [Unreleased]

## [1.3.2] - 2026-05-17

### Fixed

- **Auth (OpenSSL EVP migration)** — `core/src/protocol/mysql_auth.cpp` migrated from deprecated `SHA1()` / `SHA256()` one-shot APIs and `SHA_CTX` / `SHA256_CTX` low-level structs to `EVP_MD_CTX` via a new `ComputeDigest()` helper; `SecureCleanse` RAII now wipes all intermediate hash buffers including the previously unprotected scramble and `hash3` outputs
- **Row decoder column count validation** — `core/src/row_decoder.cpp` `ParseRowsContext()` now validates that the `ROWS_EVENT` column_count matches `TABLE_MAP` metadata and rejects mismatched events with `MES_ERR_DECODE_ROW` instead of silently decoding garbage
- **CdcEngine error propagation** — `IsError()` now returns true when `last_error_ != MES_OK` even if the parser state machine has not transitioned to `kError`; `Feed()` short-circuits to `0` on entry when in an error state and breaks mid-stream after a row-decode failure; `ErrorCode()` precedence cleaned up
- **Python binding input validation** — `set_max_queue_size()` raises `ValueError` for negative values before crossing the FFI boundary
- **Python annotation** — `_client_configured_libs` type annotation no longer string-quoted (`WeakValueDictionary[int, ctypes.CDLL]`) so it is valid under Python 3.11+ without `from __future__ import annotations`
- **Node addon warnings** — `(void)info;` casts added to `Stop()` / `Disconnect()` / `Destroy()` N-API callbacks to silence unused-parameter warnings under `-Wall -Werror`

### Added

- **`make test-tsan` target** — `Makefile` now exposes a Debug+ThreadSanitizer build that runs all non-E2E C++ tests under TSan
- **Thread safety documentation** — All five READMEs (`README.md`, `README_ja.md`, `bindings/node/README.md`, `bindings/node/README.npm.md`, `bindings/python/README.md`) document single-owner semantics for `CdcEngine` and the any-thread `stop()` cancellation path for `BinlogClient` / `CdcStream`

### Testing

- `TruncatedRowEventSetsDecodeError` (C++) and `FeedReturnsDecodeErrorForTruncatedRowEvent` (C API) cover the truncated-row error path and post-`Reset()` recovery
- `RejectsColumnCountMismatch` unit test for the new column-count validation in `row_decoder`
- `test_negative_max_queue_size_rejected` in the Python binding tests
- `test_e2e_main.cpp` with `E2eEnvironment::IsE2eServerAvailable()` probe emits `GTEST_SKIP` when the test database is not reachable, preventing false failures in offline CI environments

### Changed

- **CMake test boilerplate** — Introduced `mes_add_gtest()` and `mes_add_e2e_gtest()` helpers in `core/tests/CMakeLists.txt` and `core/tests/e2e/CMakeLists.txt`, replacing 17 copies of the same 4-line `add_executable`/`target_link_libraries`/`gtest_discover_tests` boilerplate (~118 → ~68 lines combined). E2E tests now link `GTest::gtest` and share `test_e2e_main.cpp`; all `gtest_discover_tests()` calls get `DISCOVERY_TIMEOUT 30`

### Notes

- No public API or wire-format changes versus v1.3.1
- All four matrix targets (MySQL 8.4, MySQL 9.1, MariaDB 10.11, MariaDB 11.4) pass C++ and Node.js E2E

**Detailed Release Notes**: [docs/releases/v1.3.2.md](docs/releases/v1.3.2.md)

## [1.3.1] - 2026-04-15

### Fixed

- `bindings/node/src/stream.ts`: Biome formatting violation (multi-line `if` condition collapsed to one line) that blocked npm publish on v1.3.0

### CI

- Added `yarn check` (Biome lint) step to `ci.yml` and `develop-ci.yml` so Node formatting/lint issues are caught before the publish workflow runs

### Notes

- No runtime behavior changes versus v1.3.0
- v1.3.0 was released to PyPI but not to npm due to the formatting check failure; v1.3.1 publishes the same content to both registries

## [1.3.0] - 2026-04-15

### Added

- **MariaDB 10.11+ support** — Full MariaDB binlog protocol: flavor auto-detection, GTID events (type 162, `domain-server-seq` format), ANNOTATE_ROWS, slave capability negotiation (`@mariadb_slave_capability = 4`), flavor-aware GTID encoding/decoding
- **Configurable max event size** — `maxEventSize` / `max_event_size` client option to cap peak memory per event
- **Zero-copy reader path** — Avoid buffer copies in the hot streaming path
- **Develop branch CI** — Fast-feedback CI workflow on `develop` branch
- E2E matrix runner (`e2e/run-matrix.sh`) covering `mysql:8.4`, `mysql:9.1`, `mariadb:10.11`, `mariadb:11.4`

### Changed

- Consolidated `ColumnValue` byte storage for reduced memory footprint
- Hardened error granularity, observability, and correctness across all layers
- Applied clang-format and reduced duplication across layers

### Fixed

- MariaDB E2E infrastructure: slave capability negotiation, flavor-aware GTID queries, GTID format assertions, Docker compose port alignment
- Node addon: link `mariadb_gtid.cpp` and `mariadb_event_parser.cpp` into `CORE_SOURCES` (previously unreferenced)
- Python E2E: configurable MySQL connection via `MES_MYSQL_*` env vars, flavor-aware `get_current_gtid()`, add `cryptography` dev dep for modern PyMySQL auth
- Multiple correctness bugs, undefined behavior, and safety hardening across all layers

### Testing

- Vitest pinned to exact `2.1.9`; refreshed Node lockfile

## [1.2.0] - 2026-04-13

### Added

- **CRC32 binlog checksum validation** — Verify event integrity when server advertises `CRC32` checksum

### Changed

- E2E Docker port updated (13307 → 13308) to avoid conflicts

## [1.1.0] - 2026-04-09

### Added

- **RSA public key auth** — `caching_sha2_password` full auth without TLS via RSA-OAEP encryption
- **VECTOR type support** — MySQL 9.0+ `MYSQL_TYPE_VECTOR` (0xF2) decoded as raw bytes
- **MySQL 9.x E2E compatibility** — Docker Compose accepts `MYSQL_VERSION` env var; version-gated VECTOR tests

### Changed

- Dropped `mysql_native_password` dependency; all test users use `caching_sha2_password`
- Removed deprecated `--binlog-format=ROW` and `--mysql-native-password=ON` from Docker config

### Documentation

- Added Version, npm, PyPI, MySQL 8.4+, Platform badges across all READMEs
- Renamed `README.ja.md` to `README_ja.md` for cross-project consistency
- Updated MySQL version references to 8.4+ throughout

**Detailed Release Notes**: [docs/releases/v1.1.0.md](docs/releases/v1.1.0.md)

## [1.0.1] - 2026-03-31

### Fixed

- Improved symbol visibility, thread safety, and API completeness
- Hardened bounds checking, resource cleanup, and type safety across stack
- Hardened SSL, binary decoding, and Node.js destroy safety

### Changed

- Propagated sanitizer/coverage link options as PUBLIC on mes-core

## [1.0.0] - 2026-03-30

Initial public release.

[Unreleased]: https://github.com/libraz/mysql-event-stream/compare/v1.3.2...HEAD
[1.3.2]: https://github.com/libraz/mysql-event-stream/compare/v1.3.1...v1.3.2
[1.3.1]: https://github.com/libraz/mysql-event-stream/compare/v1.3.0...v1.3.1
[1.3.0]: https://github.com/libraz/mysql-event-stream/compare/v1.2.0...v1.3.0
[1.2.0]: https://github.com/libraz/mysql-event-stream/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/libraz/mysql-event-stream/compare/v1.0.1...v1.1.0
[1.0.1]: https://github.com/libraz/mysql-event-stream/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/libraz/mysql-event-stream/releases/tag/v1.0.0
