<!-- markdownlint-disable MD024 -->
# Changelog

All notable changes to mysql-event-stream will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

**Note**: For detailed release information, see [docs/releases/](docs/releases/).

## [Unreleased]

## [1.5.0] - 2026-07-15

A client-side flow-control and packaging release. New C ABI surface is additive
only — no symbols removed and no struct layout changes. One behavioral change:
`mes_client_stop()` is now synchronous (see Changed).

### Added

- **`mes_client_set_max_queue_bytes()` / `mes_client_get_max_queue_bytes()` /
  `mes_client_queued_bytes()`** (C ABI) — byte-budgeted backpressure on the
  client event queue, complementing the existing event-count limit. `EventQueue`
  charges packet capacity plus checkpoint storage against the budget. Exposed as
  Node `maxQueueBytes` and Python `max_queue_bytes`
- **`mes_client_set_max_event_size()` / `mes_client_get_max_event_size()`**
  (C ABI) — clamp oversized binlog events at the client reader, mirroring the
  engine cap. Exposed as Node `maxEventSize` and Python `max_event_size`
- **`mes_client_is_streaming()`** (C ABI) — distinguishes a drainable terminal
  state from `mes_client_is_connected()`, letting consumers read a queued
  terminal error exactly once instead of busy-looping on `MES_ERR_DISCONNECTED`.
  Exposed as Node `isStreaming` and Python `is_streaming`
- **`mes_client_checksum_enabled()`** (C ABI) — reports CRC32 trailer detection
  so a raw engine consuming the client's poll output can match its checksum
  handling
- **`TransactionGtidTracker`** — `mes_client_current_gtid()` now advances only
  after a committed transaction is delivered, documented as a delivery
  acknowledgement rather than a durable checkpoint. Exposed as Node `currentGtid`
  and Python `current_gtid`
- **`allow_public_key_retrieval`** — opt-in config for plaintext
  `caching_sha2_password` public-key retrieval, disabled by default; surfaced in
  both bindings
- **Prebuilt distribution** — per-platform Node native packages
  (`@libraz/mysql-event-stream-<platform>-<arch>`) with a loader that falls back
  from a local build, and per-platform Python wheels, both built in CI against
  pinned static dependencies

### Changed

- **`mes_client_stop()` is now synchronous** — it joins the reader thread before
  returning and is therefore no longer async-signal-safe. Call it from a normal
  thread, not a signal handler
- `make format` now fans out to Biome (Node) and ruff (Python) alongside
  clang-format
- Node dev/runtime dependencies and Python dev-tool floors updated to current
  releases
- CI: documentation-only changes no longer trigger the build; sanitizer runs
  moved to the develop branch and an on-demand safety workflow; the protocol
  matrix and fuzz corpus gate publication

### Fixed

- Hardened protocol packet and socket I/O, state-machine buffering, TABLE_MAP
  metadata parsing, row decoding, and connection-validation edge cases against
  malformed or truncated input
- Prevented a `SIGPIPE`-induced process crash when a TLS connection is written
  to after the server closed it (e.g. a rejected stream). `SSL_write()` has no
  `MSG_NOSIGNAL` equivalent and `SO_NOSIGPIPE` is unavailable on Linux, so TLS
  writes are now guarded by a thread-scoped `SIGPIPE` suppressor

## [1.4.0] - 2026-06-26

A correctness-focused release resolving a commercial-quality audit: data-loss
and crash defects, two consolidation fixes at their shared root cause, and
cross-surface API completeness. Public API changes are additive only — no
breaking changes.

### Added

- **`mes_set_checksum_enabled()`** (C ABI) — explicitly enable/disable binlog
  checksum stripping on a `CdcEngine`. The parser also auto-detects the
  checksum algorithm from the `FORMAT_DESCRIPTION_EVENT`, so `binlog_checksum=NONE`
  streams (the MariaDB default) are no longer truncated by 4 bytes per event
- **`mes_sizeof_event()` / `mes_sizeof_column()`** (C ABI) — report the exact
  `mes_event_t` / `mes_column_t` struct sizes so bindings can assert an exact
  ABI-layout match instead of a loose byte-range heuristic
- **`names_resolved` field on `mes_event_t`** — surfaces whether column names
  were resolved, distinguishing "no names" from "metadata lookup failed";
  plumbed through both bindings
- **Structured log callback in both bindings** — Node `setLogCallback()` /
  `LogLevel` (thread-safe function) and Python `set_log_callback()` /
  `LogLevel`, exposing the previously C-ABI-only diagnostic callback
- **Typed error mapping** — Node errors now carry a numeric `code` and category
  name; the Python client maps error codes to the same typed exceptions as the
  engine
- **libFuzzer harness** (`MES_ENABLE_FUZZ`, Clang-only) driving `mes_feed` /
  `mes_next_event` with chunked malformed input, intended to run under ASan/UBSan
- **Python E2E in the matrix runner** — `e2e/run-matrix.sh` now drives the C++,
  Node.js, and Python suites against MySQL and MariaDB versions, with
  per-suite scoping flags; documented in `e2e/README.md`

### Fixed

- **Python use-after-free on shutdown** — `BinlogClient.close()` serializes
  against an in-flight `poll()` via a lock, and `CdcStream.close()` awaits the
  in-flight poll task before teardown, so a normal break/GC during streaming no
  longer crashes
- **Duplicate event replay on multi-UUID GTID resume** — each comma-separated
  SID is normalized independently (`uuid:N` → `uuid:1-N`) instead of skipping
  normalization when more than one UUID is present
- **UNSIGNED columns decoded as signed** — the TABLE_MAP optional metadata block
  (SIGNEDNESS, COLUMN_NAME) is now parsed; UNSIGNED columns decode correctly from
  raw binlog bytes with no metadata side-connection, and an UNSIGNED BIGINT that
  overflows int64 surfaces as a string instead of a wrong `0`
- **`binlog_checksum=NONE` corruption** — see `mes_set_checksum_enabled` above;
  events are no longer truncated by a phantom 4-byte checksum
- **Negative TIME2 / DATETIME2 decoding** — negative temporal values are
  reconstructed from the combined signed packed value (MySQL complement form)
  instead of decoding the fractional part independently; the lossy 10-bit hour
  mask is dropped
- **FLOAT / DOUBLE on big-endian builds** — decoded through a byte-order-independent
  little-endian read before reinterpreting the bit pattern
- **Stale column metadata after DDL** — QUERY_EVENT DDL
  (ALTER/RENAME/DROP/CREATE/TRUNCATE) invalidates the metadata cache so post-DDL
  column names and signedness are refreshed
- **MariaDB checksum false detection** — checksum state is reset before the
  detection query so a failed `@@global.binlog_checksum` lookup no longer leaves
  verification stuck enabled
- **TABLE_MAP registry retained across ROTATE** — the registry and derived filter
  cache are cleared on ROTATE, preventing decode against stale metadata and
  unbounded growth toward the registry cap across many rotations
- **Resume position advanced past a failed event** — the recorded position rolls
  back on a decode failure so a reconnect re-reads the offending event instead of
  skipping it
- **TLS peer verification** — verify-CA/identity modes assert the peer certificate
  and `SSL_get_verify_result() == X509_V_OK` after `SSL_connect`; `verify_identity`
  binds IP literals via `X509_VERIFY_PARAM_set1_ip_asc`, rejects empty hostnames
  (no silent CA-only downgrade), and omits SNI for IP literals (RFC 6066)
- **Truncated result-set / row data** — treated as a parse error instead of
  fabricating NULL columns, so corrupt or unexpectedly compressed payloads surface
- **DEPRECATE_EOF framing** — the negotiated capability is threaded into every
  `ExecuteQuery` call instead of unconditionally assuming DEPRECATE_EOF
- **PARTIAL_JSON delivery** — the connection validator rejects
  `binlog_row_value_options=PARTIAL_JSON`, which would otherwise deliver corrupt
  partial-JSON row payloads
- **Node feed data loss** — bytes the engine did not consume from `feed()` are
  retained and prepended to the next poll, so enabling a queue limit cannot
  silently drop binlog bytes under backpressure
- **Node `maxQueueSize` truncation** — read as a 64-bit value (the C field is
  `size_t`) and negatives rejected, instead of truncating through `Uint32Value`
- **Node stream retry on fatal errors** — auth/validation failures fail fast
  instead of being retried up to the reconnect limit
- Handshake parsing uses a length-checked fixed-int read so it cannot read past
  the packet buffer; the auth-switch trailing-NUL strip is gated to the native
  and caching_sha2 plugins; the metadata fetcher scrubs its stored password on
  teardown; the error-sentinel queue push is checked against a queue-close race

### Changed

- **`mes_set_max_event_size(0)` means "no limit"** (the absolute cap still
  applies) instead of rejecting every event, matching sibling APIs
- **MySQL 8.4 tagged GTIDs (`uuid:tag:N`)** are rejected with a clear log instead
  of an opaque interval-parse failure
- TABLE_MAP binlog signedness is treated as authoritative; the metadata fetcher
  no longer overwrites it with possibly stale `SHOW COLUMNS` data

### Documentation

- Documented the `mes_feed` error-recovery contract (reset-only after an error),
  the unbounded-queue default risk, how JSON/ENUM/SET/BIT columns are represented
  across bindings, the non-UTF-8 string caveat, and the `max_queue_size`
  zero-semantics difference between the engine setter and the client config

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

[Unreleased]: https://github.com/libraz/mysql-event-stream/compare/v1.5.0...HEAD
[1.5.0]: https://github.com/libraz/mysql-event-stream/compare/v1.4.0...v1.5.0
[1.4.0]: https://github.com/libraz/mysql-event-stream/compare/v1.3.2...v1.4.0
[1.3.2]: https://github.com/libraz/mysql-event-stream/compare/v1.3.1...v1.3.2
[1.3.1]: https://github.com/libraz/mysql-event-stream/compare/v1.3.0...v1.3.1
[1.3.0]: https://github.com/libraz/mysql-event-stream/compare/v1.2.0...v1.3.0
[1.2.0]: https://github.com/libraz/mysql-event-stream/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/libraz/mysql-event-stream/compare/v1.0.1...v1.1.0
[1.0.1]: https://github.com/libraz/mysql-event-stream/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/libraz/mysql-event-stream/releases/tag/v1.0.0
