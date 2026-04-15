# Release Notes

This directory contains detailed release notes for each version of mysql-event-stream.

## Available Versions

- v1.3.1 - Latest release - Fix npm publish formatting check blocked on v1.3.0 (no runtime changes)
- v1.3.0 - 2026-04-15 - MariaDB 10.11+ support, configurable max event size, zero-copy reader path (PyPI only; npm skipped)
- v1.2.0 - 2026-04-13 - CRC32 binlog checksum validation, E2E port update
- [v1.1.0](v1.1.0.md) - 2026-04-09 - MySQL 9.x compatibility, VECTOR type, RSA auth
- v1.0.1 - 2026-03-31 - Security hardening, thread safety, bounds checking
- v1.0.0 - 2026-03-30 - Initial public release

## Quick Links

- [CHANGELOG.md](../../CHANGELOG.md) - Concise version history (Keep a Changelog format)
- [GitHub Releases](https://github.com/libraz/mysql-event-stream/releases) - Download and release assets
- [Latest Release](https://github.com/libraz/mysql-event-stream/releases/latest) - Most recent version

## Format

Each release note file includes:

- **Overview**: Major features and improvements
- **New Features**: Detailed feature descriptions with files changed
- **Testing**: Test coverage additions
- **Documentation**: Documentation updates
- **Migration Guide**: Step-by-step upgrade instructions

## Versioning

mysql-event-stream follows [Semantic Versioning](https://semver.org/):

- **MAJOR** (X.0.0): Incompatible API changes
- **MINOR** (0.X.0): New features (backward compatible)
- **PATCH** (0.0.X): Bug fixes (backward compatible)
