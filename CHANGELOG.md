<!-- markdownlint-disable MD024 -->
# Changelog

All notable changes to mysql-event-stream will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

**Note**: For detailed release information, see [docs/releases/](docs/releases/).

## [Unreleased]

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

[Unreleased]: https://github.com/libraz/mysql-event-stream/compare/v1.3.1...HEAD
[1.3.1]: https://github.com/libraz/mysql-event-stream/compare/v1.3.0...v1.3.1
[1.3.0]: https://github.com/libraz/mysql-event-stream/compare/v1.2.0...v1.3.0
[1.2.0]: https://github.com/libraz/mysql-event-stream/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/libraz/mysql-event-stream/compare/v1.0.1...v1.1.0
[1.0.1]: https://github.com/libraz/mysql-event-stream/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/libraz/mysql-event-stream/releases/tag/v1.0.0
