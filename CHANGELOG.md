<!-- markdownlint-disable MD024 -->
# Changelog

All notable changes to mysql-event-stream will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

**Note**: For detailed release information, see [docs/releases/](docs/releases/).

## [Unreleased]

## [1.7.0] - 2026-09-13

The C ABI gains four functions and one struct field, so `mes_abi_version()` now
reports 3. The addition is laid out so that a binary built against the 1.6
header keeps linking and running: `mes_poll_result_t` is still 32 bytes and its
four original fields are still at offsets 0, 8, 16 and 24. The reverse does not
hold — code built against the 1.7 header reads `checksum_enabled` at offset 28,
which a 1.6 library never writes — and that is what the ABI version now refuses.

Independent `CdcEngine` instances no longer share an allocator lock. Decoding
the same workload on eight threads reaches 4.8-5.5x the single-thread rate,
where it previously peaked at two threads and fell from there.

### Added

- **`mes_set_max_queue_bytes()` / `mes_get_max_queue_bytes()`** (C ABI, both
  bindings) — the engine's queue byte budget is now reachable. `max_queue_size`
  cannot express a memory limit, because what one entry costs depends on the
  table it came from and any statement annotating it
- **`mes_set_trailer_pre_verified()` / `mes_get_trailer_pre_verified()`** (C ABI,
  both bindings) — declare that something upstream has already verified each
  event's CRC32, so the engine does not compute it a second time. Nothing
  verifies the promise: set it on an unvalidated stream and a corrupt event is
  accepted in silence
- **`checksum_enabled` on the poll result** (C ABI, both bindings) — the
  framing an event was read under now travels with the event. A
  `FORMAT_DESCRIPTION_EVENT` moves the client's framing while events read under
  the previous value are still queued, so an engine fed those bytes must be
  framed from the result rather than from the client's current view
- **Declared error types** — Python exports `MesError`, `MesConnectionError`,
  `ParseError`, `DecodeError` and `ChecksumError`, each carrying the native
  `code`; Node exports the `MesError` shape and an `isMesError()` guard

### Fixed

- **Checksum framing was established by position rather than by verification** —
  the trailer probe now decides from a CRC32 that matches, and framing is kept
  per event instead of sampled once per stream
- **Queue memory was bounded by entry count alone** — the engine and the client
  queue are both bounded by the bytes they actually hold, and the table-map
  registry by the bytes it retains
- **Decode correctness** — DECIMAL groups are validated and decoded unsigned, a
  BIT column declared wider than 64 bits is rejected, the packed-integer NULL
  marker is rejected in length fields, and text-versus-bytes for character
  columns is decided in one place
- **Connection and stream lifetime** — a transport whose dump was abandoned is
  retired, resumable dump errors and dead sockets are no longer hidden, a
  restarted stream resumes from the published checkpoint, and the TLS SIGPIPE
  guard is held for a whole read
- **Credentials** — the metadata connection password is wiped at the C ABI and
  the staged replication password in the Node addon
- **Option validation** — both bindings refuse the same configuration, two
  competing start modes are refused at construction, a start GTID naming no
  transaction is rejected, and Node refuses a non-integer where it declares an
  integer instead of truncating it
- **Python** — native dispatch in `CdcStream` is serialized, the log callback is
  detached at interpreter shutdown, client lifecycle calls no longer block, and
  the library loaded is the one belonging to the running environment

### Changed

- **Row column arrays no longer come from a pooled memory resource.** Every row
  in the process allocates from one stateless resource, which removes the lock
  independent engines were contending on. Queued memory per event fell with it,
  by up to 24% on the widest workload, because the pool rounded each column
  array up to its block size
- **An event's column names and annotating statement are shared rather than
  copied per row**, and a DECIMAL is rendered from the digits it produces rather
  than its declared width

### Documentation

- The measurement baseline records queued memory and thread scaling taken after
  the allocator change
- Both bindings document what the queue byte budget charges, the charset
  fallback for both column families, and the type each declares the error code on

## [1.6.1] - 2026-08-15

A correctness and hardening release. The C ABI is unchanged — no symbol, field,
or enum value was added, removed, or moved, so a 1.6.0 build links without
recompiling. Several failures that used to be silent are now loud, and both
bindings validate their options the same way, so a configuration that was
previously accepted out of range is now rejected at construction.

### Added

- **Shared binding contract** — `core/contracts/bindings.json` fixes error
  retryability, the reconnect backoff schedule, the checkpoint-resume rule, and
  the shared option defaults and ranges in one place. Both bindings mirror it as
  typed constants and verify them against the JSON, so a value changed on one
  side without the other fails that binding's suite
- **Benchmark suite** — synthetic workload, loopback socket-read, and
  allocation-tracking harnesses with a recorded baseline, driven by
  `make benchmark`, `make benchmark-socket`, and `make benchmark-python`
- **Security policy** — `SECURITY.md` documents private disclosure, supported
  versions, and what is in and out of scope for the binlog protocol surface

### Fixed

- **MariaDB `source_sql` was always empty.** The dump request now sets the
  ANNOTATE_ROWS flag, which the slave capability alone does not imply, so the
  statement text actually arrives. It is decoded once and shared by every row of
  the following row event instead of copied per row
- **MariaDB standalone GTID groups checkpointed too early.** A group with no
  COMMIT or XID was checkpointed at its GTID event, before its own row payload
  was delivered; it now waits for the group's terminating event. A GTID that
  cannot merge into the tracked set is logged as
  `gtid_checkpoint_merge_failed` and kept pending instead of being dropped
- **An empty reconnect checkpoint was forwarded as a resume position.** Both
  bindings sent `startGtid` / `start_gtid` of `""` when a connection dropped
  before the first commit, which the server reads as "send every retained
  binlog"; the configured start mode is now kept verbatim until a real
  checkpoint exists
- **ENUM, SET, and VECTOR were counted wrongly in the charset index space.**
  ENUM and SET travel as `MYSQL_TYPE_STRING` but carry their collations in
  separate metadata, so they no longer consume a `DEFAULT_CHARSET` slot, while
  VECTOR does. A table mixing them could otherwise assign a later column the
  wrong collation and surface it as bytes instead of text
- **`mes_client_stop()` could not interrupt `mes_client_start()`.** Startup no
  longer holds the lifecycle lock across its blocking round trips; an
  interrupted start returns `MES_ERR_DISCONNECTED`, and two concurrent starts
  are refused with `MES_ERR_STREAM` rather than serialized
- **A bare table filter entry could match on the database name.** An entry
  without a `.` is now compared against the bare table name only, so
  `log*` no longer matches database `logs`
- **A NULL element in a filter array silently cleared the filter.**
  `mes_set_include_databases()`, `mes_set_include_tables()`, and
  `mes_set_exclude_tables()` now reject the whole call with `MES_ERR_NULL_ARG`
  and leave the installed filter untouched
- **Artificial ROTATE detection could reframe the event being parsed.** Checksum
  presence is only ever turned on by that path, never re-derived mid-event
- **A dual-stack hostname could block for twice the connect timeout.** One
  deadline is now computed once and shared across every resolved address
- **An unsupported auth plugin with an empty password produced an empty auth
  response** instead of being rejected; the plugin name is validated first
- **Column names were not charged against the result-set byte budget**, which
  could be exceeded before any row value was read. Column-definition size and
  repeated empty row packets are now bounded as well
- **The metadata fetcher could be destroyed before the engine that used it**;
  ownership now guarantees the fetcher outlives it
- **An oversized column payload asserted at the C ABI boundary.** It is clamped
  to `UINT32_MAX` and reported as a `column_data_truncated` warning instead
- Every event type a supported server emits is now explicitly decoded, skipped
  as a control event, or refused with `MES_ERR_PARSE`, so a newly introduced
  event type is loud rather than lossy
- Row decoding charges a per-event budget and cross-checks the wire column count
  against TABLE_MAP before any bitmap arithmetic, so a compressed or malformed
  row event cannot drive heap growth far past the event size
- A tagged MySQL GTID naming a bare transaction number (`uuid:tag:5`) is widened
  to `uuid:tag:1-5` like the untagged form, instead of being sent as written
- Both bindings validate every option at construction the same way `configure()`
  does, and range-check batch size and log level against the shared contract
- Every error raised across the native boundary carries a numeric error code, so
  a permanent lifecycle violation is never retried as a transient failure

### Changed

- **Events that precede a terminal error are now delivered.** A batch poll
  reports the events it collected and latches the terminal error for the next
  call; the native checkpoint advances as if the batch was consumed, so
  discarding it lost events permanently
- **Transport and framing failures during a query report `MES_ERR_STREAM`**
  instead of `MES_ERR_PARSE`. Callers matching on the specific code need to
  expect the new one
- The client queue byte budget charges only the buffered wire payload;
  checkpoint text and error messages are no longer counted against it, and
  admission is checked against the minimum budget one maximum-size event needs
- Credential bytes are wiped from protocol packet buffers on clear and
  destruction, and the staging copy of the password is wiped after connect
- Python column marshalling copies payloads through a single window over the C
  buffer instead of one `ctypes.string_at` call per column
- The Node binding pins its toolchain with mise instead of volta and moves to
  Yarn 4.18

### Documentation

- The column-type mapping is stated once as a canonical table in `mes.h` and
  restated in each binding, with a test comparing the tables so the surfaces
  cannot drift. TIMESTAMP is documented as a decimal Unix-epoch string, which is
  what the engine has always produced
- The `feed()` quickstarts drain the queue on each iteration, which is required
  because `feed()` stops early when the queue fills mid-chunk
- The `caching_sha2_password` guidance explains why `preferred` and `required`
  do not satisfy full authentication and names both remedies
- `README_ja.md` gains the error-code and package-install sections that were
  English-only, and drops an incorrect `gtid_strict_mode` requirement

## [1.6.0] - 2026-07-29

A correctness and throughput release. The C ABI grows to version 2: no symbols
are removed, but `mes_client_config_t` and `mes_event_t` gain trailing fields,
so consumers must recompile against the new header. Several defaults and
failure behaviors changed — see Changed before upgrading.

### Added

- **`mes_version()` / `mes_abi_version()` / `MES_ABI_VERSION`** (C ABI) — both
  bindings verify the ABI version when they load the shared library and refuse
  a mismatched build instead of misreading structs
- **`mes_error_string()`** (C ABI) — stable text for every error code, replacing
  the message tables previously maintained inside each binding
- **`mes_client_poll_batch()`** (C ABI) — block for one result, then drain what
  is already queued in a single call. Exposed as Node `pollBatch()` and Python
  `poll_batch()`, and used by both stream implementations
- **Explicit start positions** — `mes_start_position_mode_t` plus `binlog_file` /
  `binlog_position` in `mes_client_config_t` separate "snapshot the current GTID
  set", "resume from this GTID set" (including an empty one), and "resume from
  this file and offset". Exposed as Node `startBinlogFile` / `startBinlogPosition`
  and Python `start_binlog_file` / `start_binlog_position`
- **`MES_ERR_GTID_PURGED` (405)** — the client compares the requested GTID set
  against `@@GLOBAL.gtid_purged` before dumping and maps server error 1236, so a
  purged checkpoint reports a diagnosable error instead of a dropped connection
- **`mes_event_t::source_sql`** — original statement text from MariaDB
  `ANNOTATE_ROWS` events. Exposed as Node `sourceSql` and Python `source_sql`
- **`mes_client_flavor()` / `mes_client_crc_errors()`** (C ABI) — detected server
  flavor and the count of CRC32-invalid events. Exposed as Node `flavor` /
  `crcErrors` and Python `flavor` / `crc_errors`
- **Tagged GTID support** — a shared `GtidSet` parses, merges, and encodes
  MySQL 8.4 tagged GTIDs (`uuid:tag:1-5`), including `GTID_TAGGED_LOG_EVENT` in
  the transaction tracker; tagged sets were previously rejected
- **Charset-aware column typing** — TABLE_MAP charset metadata decides text
  versus binary, so a non-binary BLOB-family column decodes as a string and a
  binary `VARCHAR`/`CHAR` decodes as bytes
- **Stream-level filtering in both bindings** — `includeDatabases` /
  `includeTables` / `excludeTables` (Node) and their Python equivalents are
  applied at start and re-applied after a reconnect; filter entries accept a
  trailing `*` as a prefix wildcard
- **`MesErrorCode` and `ServerFlavor`** enums in both bindings; every raised
  error carries the numeric code
- Python `on_metadata_error` callback, replacing silently suppressed metadata
  failures
- `make benchmark` feed/decode harness and a protocol-parser fuzz target with a
  seeded corpus

### Changed

- **ABI version 2 — recompile required.** `mes_client_config_t` gains
  `start_position_mode`, `binlog_file`, and `binlog_position`; `mes_event_t`
  gains `source_sql`. Existing field offsets are unchanged and no symbol was
  removed, but a caller compiled against the 1.5 header must be rebuilt
- **Unknown and unsupported binlog events now fail.** `PARTIAL_UPDATE_ROWS`,
  the MariaDB compressed event types, and any unrecognized type return
  `MES_ERR_PARSE` instead of being skipped, so a checkpoint can no longer
  advance past a change the consumer never saw
- **Partial row images are rejected.** A row event whose columns-present bitmap
  has cleared bits fails rather than reporting absent columns as NULL; sources
  must run `binlog_row_image=FULL`
- **Queue defaults lowered** — the client byte budget defaults to 48 MiB (was
  256 MiB) and the maximum event size to 32 MiB (was 64 MiB); charged bytes now
  use payload size instead of allocator capacity
- **`mes_set_max_queue_size(0)` restores the bounded default** (10000 events);
  the engine no longer defaults to an unbounded queue
- **`mes_reset()` keeps already-decoded events** so events that preceded a parse
  error can still be drained; it clears parser, table-map, position, and error
  state as before
- `names_resolved` now reports whether every column name is non-empty, counting
  names carried by TABLE_MAP, instead of whether the metadata side-connection
  returned a matching column count
- `caching_sha2_password` full authentication sends the cleartext password only
  when TLS verified the server certificate
- Both bindings default `sslMode` / `ssl_mode` to preferred instead of disabled,
  and reject `server_id` 0 at construction
- Python `start_gtid=None` snapshots the current position while `""` requests an
  explicit empty set; the engine raises `ParseError` for unrepresentable events
  instead of warning and returning nothing
- Both stream implementations treat parse, checksum, decode, queue-full, and
  GTID errors as permanent and stop reconnecting; the retry budget resets only
  after an event is decoded
- MariaDB: a failed `@mariadb_slave_capability` negotiation is now fatal,
  `log_bin_compress=ON` is rejected during validation, and "start at current"
  prefers `@@GLOBAL.gtid_binlog_pos`
- The Python package is classified as Beta
- The Python development environment is managed with rye alone:
  `requirements.lock` / `requirements-dev.lock` replace `uv.lock`, the toolchain
  pin is tracked, and CI installs from the lockfiles instead of resolving with
  pip
- Node and Python development dependencies updated to current releases

### Fixed

- Reader-thread and socket lifecycle races: the descriptor is atomic and its
  shutdown/close is serialized, a finished reader is joined before replacement,
  and `stop()` interrupts an in-flight handshake
- The TLS handshake no longer blocks past the read timeout, verify modes without
  an explicit CA load the system trust store, and a final TLS record arriving
  together with a hangup is no longer discarded
- Result-set reading: a first field of 16 MiB or more is no longer mistaken for
  the end-of-rows marker, server errors are classified as validation errors, and
  a connection left mid-result-set is poisoned instead of reused
- `FORMAT_DESCRIPTION` checksum detection verifies the CRC32 trailer instead of
  trusting the algorithm byte
- SIGNEDNESS metadata accounting (TYPED_ARRAY, DECIMAL, YEAR) and rejection of a
  truncated bitmap; fractional seconds are emitted at the column's declared
  precision; `SET` and `BIT` values above `INT64_MAX` are returned as exact
  decimal strings
- The TABLE_MAP registry evicts least-recently-used entries instead of clearing
  itself, and a byte-identical repeated TABLE_MAP skips re-parsing
- A parse error raised after an event was accepted no longer makes `mes_feed()`
  reprocess the same event indefinitely, and an artificial ROTATE no longer
  clears the resume filename
- Metadata resolution caches failures and throttles reconnects, so a missing
  SELECT privilege no longer causes a query storm
- Transaction boundaries: `ROLLBACK TO SAVEPOINT` and DDL keywords inside an
  open transaction no longer promote a checkpoint, and MariaDB standalone GTID
  groups are committed immediately
- Node: a throwing log handler can no longer surface as an uncaught exception,
  `connect()` / `start()` / `disconnect()` are rejected while a poll is in
  flight, and the column-name cache verifies the underlying bytes before reuse
- Python: unconsumed `feed()` bytes are retained instead of dropped, and close,
  stop, and property reads are serialized against the poll lock
- The E2E matrix runner reads ctest's exit status through the pipe and fails
  when every test was skipped

### Documentation

- Error-code table with retry guidance, and pointers to `MesErrorCode` /
  `mes_error_string()` instead of message-text matching
- `feed()` examples corrected to a consume loop that retains unconsumed bytes
- TLS mode guidance, supported authentication plugins, and the public-key
  retrieval opt-in
- Filter semantics (case sensitivity, prefix wildcards, matched-nothing warning)
  and the server binlog settings required for column-name resolution
- Client lifecycle expectations for bindings, and corrected npm install wording

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

[Unreleased]: https://github.com/libraz/mysql-event-stream/compare/v1.7.0...HEAD
[1.7.0]: https://github.com/libraz/mysql-event-stream/compare/v1.6.1...v1.7.0
[1.6.1]: https://github.com/libraz/mysql-event-stream/compare/v1.6.0...v1.6.1
[1.6.0]: https://github.com/libraz/mysql-event-stream/compare/v1.5.0...v1.6.0
[1.5.0]: https://github.com/libraz/mysql-event-stream/compare/v1.4.0...v1.5.0
[1.4.0]: https://github.com/libraz/mysql-event-stream/compare/v1.3.2...v1.4.0
[1.3.2]: https://github.com/libraz/mysql-event-stream/compare/v1.3.1...v1.3.2
[1.3.1]: https://github.com/libraz/mysql-event-stream/compare/v1.3.0...v1.3.1
[1.3.0]: https://github.com/libraz/mysql-event-stream/compare/v1.2.0...v1.3.0
[1.2.0]: https://github.com/libraz/mysql-event-stream/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/libraz/mysql-event-stream/compare/v1.0.1...v1.1.0
[1.0.1]: https://github.com/libraz/mysql-event-stream/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/libraz/mysql-event-stream/releases/tag/v1.0.0
