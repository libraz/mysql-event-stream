<!-- markdownlint-disable MD024 -->
# Changelog

All notable changes to mysql-event-stream will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

**Note**: For detailed release information, see [docs/releases/](docs/releases/).

## [Unreleased]

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

[Unreleased]: https://github.com/libraz/mysql-event-stream/compare/v1.1.0...HEAD
[1.1.0]: https://github.com/libraz/mysql-event-stream/compare/v1.0.1...v1.1.0
[1.0.1]: https://github.com/libraz/mysql-event-stream/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/libraz/mysql-event-stream/releases/tag/v1.0.0
