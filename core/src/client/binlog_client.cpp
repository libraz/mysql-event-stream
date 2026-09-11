// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include "client/binlog_client.h"

#include <algorithm>
#include <cctype>
#include <cstdio>

#include "binary_util.h"
#include "client/connection_validator.h"
#include "client/gtid_encoder.h"
#include "crc32.h"
#include "event_header.h"
#include "logger.h"
#include "mariadb_event_parser.h"
#include "protocol/mysql_query.h"
#include "secure_cleanse.h"
#include "state_machine.h"

namespace mes {

namespace {

/** @brief Validate MariaDB GTID set format to prevent SQL injection.
 *
 * A valid MariaDB GTID set contains only digits, hyphens, commas, and spaces
 * (e.g., "0-1-42" or "0-1-42,1-2-100").
 */
bool IsValidMariaDBGtidSet(const std::string& gtid) {
  if (gtid.empty()) return true;
  for (char c : gtid) {
    if (c != '-' && c != ',' && c != ' ' && (c < '0' || c > '9')) {
      return false;
    }
  }
  return true;
}

/// Binlog files begin with a 4-byte magic number (0xFE 0x62 0x69 0x6E).
/// Streaming starts at offset 4 to skip the magic header.
constexpr uint32_t kBinlogMagicOffset = 4;

/// Default heartbeat period in nanoseconds (3 seconds).
constexpr uint64_t kDefaultHeartbeatPeriodNs = 3'000'000'000ULL;

uint64_t HeartbeatPeriodNs(uint32_t read_timeout_s) {
  if (read_timeout_s == 0) return kDefaultHeartbeatPeriodNs;
  const uint64_t half_timeout_ns = static_cast<uint64_t>(read_timeout_s) * 1'000'000'000ULL / 2U;
  return std::min(kDefaultHeartbeatPeriodNs, half_timeout_ns);
}

}  // namespace

BinlogClient::BinlogClient() = default;

BinlogClient::~BinlogClient() { Disconnect(); }

void BinlogClient::SetLastError(const std::string& msg) {
  std::lock_guard<std::mutex> lock(last_error_mutex_);
  last_error_ = msg;
}

mes_error_t BinlogClient::Connect(const BinlogClientConfig& config) {
  // Tear down any previous stream first
  if (conn_.IsConnected()) {
    Disconnect();
  }
  // Reset stop flag after previous stream is fully torn down
  stop_requested_.store(false, std::memory_order_release);
  connected_.store(false, std::memory_order_release);
  // Every buffer the previous stream handed to the consumer dies here, so no
  // checkpoint of that stream stays reachable by PromoteDeliveredCheckpoint().
  // current_gtid_ itself is deliberately kept: a published checkpoint is the
  // successor connection's start position (see EstablishStartState()), and
  // clearing it here would be the difference between resuming the stream and
  // re-resolving the configured position.
  ResetDeliveredEvents();

  if (config.ssl_mode > MES_SSL_VERIFY_IDENTITY) {
    SetLastError("Invalid ssl_mode value");
    return MES_ERR_INVALID_ARG;
  }
  if (config.server_id == 0) {
    SetLastError("server_id must be non-zero");
    return MES_ERR_INVALID_ARG;
  }
  if (config.start_at_file_position &&
      (config.binlog_file.empty() || config.binlog_position < kBinlogMagicOffset ||
       config.binlog_position > UINT32_MAX)) {
    SetLastError("binlog_file and binlog_position (4 through UINT32_MAX) are required");
    return MES_ERR_INVALID_ARG;
  }

  mes_error_t rc =
      conn_.Connect(config.host, config.port, config.user, config.password,
                    config.connect_timeout_s, config.read_timeout_s, config.ssl_mode, config.ssl_ca,
                    config.ssl_cert, config.ssl_key, config.allow_public_key_retrieval);
  if (rc != MES_OK) {
    std::string err = conn_.GetLastError();
    SetLastError(err);
    LogMySQLConnectionError(config.host, config.port, err);
    return rc;
  }

  // Detect server flavor (MySQL vs MariaDB)
  server_flavor_ = conn_.GetServerFlavor();

  // Validate server configuration
  ValidationResult validation = ConnectionValidator::Validate(&conn_, server_flavor_);
  if (validation.error != MES_OK) {
    SetLastError(validation.message);
    conn_.Disconnect();
    return MES_ERR_VALIDATION;
  }

  StructuredLog()
      .Event("mysql_connected")
      .Field("host", config.host)
      .Field("port", static_cast<int>(config.port))
      .Field("flavor", GetServerFlavorName(server_flavor_))
      .Info();
  config_ = config;
  // Preserve the direct C++ configuration contract: a non-empty start_gtid
  // selects GTID resume unless the caller explicitly requested a file offset.
  if (config_.start_at_current && !config_.start_gtid.empty()) {
    config_.start_at_current = false;
  }
  // The retained copy is not needed after authentication. shrink_to_fit()
  // hands the buffer back to the allocator immediately, so the wipe has to be
  // one the compiler may not treat as a dead store.
  if (!config_.password.empty()) {
    SecureWipe(config_.password);
    config_.password.clear();
    config_.password.shrink_to_fit();
  }
  connected_.store(true, std::memory_order_release);
  return MES_OK;
}

mes_error_t BinlogClient::StartStream() {
  // Setup spans five to eight blocking round trips. Holding stop_mutex_ across
  // them would make Stop() -- the one entry point documented as callable from
  // another thread -- wait for exactly the I/O it exists to interrupt. So the
  // lifecycle lock is taken only for the two short sections that touch shared
  // lifecycle state (reaping a finished reader, publishing the new queue and
  // reader), and setup_in_progress_ excludes a second concurrent setup instead.
  if (setup_in_progress_.exchange(true, std::memory_order_acq_rel)) {
    SetLastError("Binlog stream setup is already in progress");
    return MES_ERR_STREAM;
  }
  struct SetupScope {
    std::atomic<bool>* flag;
    ~SetupScope() { flag->store(false, std::memory_order_release); }
  } setup_scope{&setup_in_progress_};

  if (!conn_.IsConnected()) {
    SetLastError("Not connected");
    return MES_ERR_DISCONNECTED;
  }

  if (streaming_.load(std::memory_order_acquire)) {
    return MES_OK;
  }

  // Stop() shuts the transport down permanently; the socket is only replaced by
  // a fresh Connect(), which is also what clears this flag. Refusing here keeps
  // a post-Stop start from spending a round trip on a dead socket.
  if (stop_requested_.load(std::memory_order_acquire)) {
    SetLastError("Client was stopped; reconnect before starting a new stream");
    return MES_ERR_DISCONNECTED;
  }

  // Placed ahead of every remaining exit path -- including the queue-budget
  // rejection below -- so a start that fails part-way cannot leave a prior
  // stream's events reachable. It deliberately sits after the two checks above:
  // neither begins a new stream, and a start refused while one is still running
  // must not free the buffer that stream's last poll returned.
  gtid_tracker_.Reset();
  ResetDeliveredEvents();

  {
    // A reader that terminated after delivering a terminal error remains
    // joinable until the owner consumes that error. Reap it before assigning a
    // new std::thread below: move-assigning over a joinable thread terminates
    // the process. This may briefly wait for the reader's final return, but it
    // is safe because streaming_ is false only after it has begun shutdown.
    std::lock_guard<std::mutex> lock(stop_mutex_);
    if (reader_thread_.joinable()) {
      reader_thread_.join();
    }
  }

  // An event at the configured ceiling, together with the checkpoint queued
  // beside it, must always fit. Both sides of that promise come from
  // MinQueueBytesForEvent(): the queue charges an entry through
  // QueuedEventCharge(), the same rule this guard budgets for.
  if (max_queue_bytes_ < MinQueueBytesForEvent(max_event_size_)) {
    SetLastError("max_queue_bytes is smaller than one max_event_size event plus its checkpoint");
    return MES_ERR_INVALID_ARG;
  }

  // Re-checked after every stage so a Stop() that unblocked the round trip is
  // reported as such instead of as whatever transport error the shutdown
  // produced, and so no later stage is attempted on a socket already torn down.
  const auto stopped_during_setup = [this] {
    if (!stop_requested_.load(std::memory_order_acquire)) return false;
    SetLastError("Binlog stream setup was interrupted by Stop()");
    return true;
  };

  // Stream startup is three ordered stages, and the middle one is common to
  // every flavor and every start mode. Keeping the split here (rather than
  // inside each flavor's setup) means a future start mode is added inside
  // SendBinlogDump*, which runs strictly after the state establishment it
  // depends on and therefore cannot skip it.
  const bool is_mariadb = server_flavor_ == ServerFlavor::kMariaDB;

  mes_error_t rc = is_mariadb ? BeginSessionMariaDB() : BeginSessionMySQL();
  if (stopped_during_setup()) return MES_ERR_DISCONNECTED;
  if (rc != MES_OK) {
    return rc;
  }

  StartState start_state;
  rc = EstablishStartState(&start_state);
  if (stopped_during_setup()) return MES_ERR_DISCONNECTED;
  if (rc != MES_OK) {
    return rc;
  }

  rc = is_mariadb ? SendBinlogDumpMariaDB(start_state) : SendBinlogDumpMySQL(start_state);
  if (stopped_during_setup()) return MES_ERR_DISCONNECTED;
  if (rc != MES_OK) {
    return rc;
  }

  const size_t queue_size =
      config_.max_queue_size > 0 ? config_.max_queue_size : MES_DEFAULT_QUEUE_SIZE;

  // Publish the queue and the reader as one step under the lifecycle lock, with
  // the stop flag re-checked inside it. A Stop() that already ran must not be
  // followed by a live reader; a Stop() that runs after this section sees both
  // the queue and the thread, so it can never close an old queue and then leave
  // a newly created reader owning an open one.
  std::lock_guard<std::mutex> lock(stop_mutex_);
  if (stopped_during_setup()) return MES_ERR_DISCONNECTED;
  {
    // Installing the replacement destroys the queue the previous stream used,
    // so publish the pointer under the lock every cross-thread read of it
    // takes. Poll() and the reader read it unlocked, which is safe: Poll() runs
    // on the owner thread that is executing this function, and the reader only
    // starts below.
    std::lock_guard<std::mutex> queue_lock(queue_ptr_mutex_);
    event_queue_ = std::make_unique<EventQueue>(queue_size, max_queue_bytes_);
  }

  // streaming_ is published only once the reader exists, so the flag is never
  // true while nothing can push to or close the queue -- a Poll() in that state
  // would block in Pop() forever. The core is built -fno-exceptions, so a
  // std::thread construction failure (thread-resource exhaustion) aborts here
  // instead of unwinding out through the C ABI; a caller that does survive it
  // observes a client that is not streaming rather than one that hangs.
  reader_thread_ = std::thread(&BinlogClient::ReaderLoop, this);
  streaming_.store(true, std::memory_order_release);

  return MES_OK;
}

mes_error_t BinlogClient::BeginSessionMySQL() {
  // A BinlogClient instance may be disconnected and reused with a different
  // start position. Never let a prior stream's encoded GTID set leak into the
  // next COM_BINLOG_DUMP_GTID packet, including when setup fails part-way.
  gtid_encoded_.clear();

  // Advertise checksum support. The session SET does not rewrite binlogs
  // produced with checksum=NONE, so the source's actual storage mode is read
  // separately by DetectBinlogChecksum().
  protocol::QueryResult qr;
  std::string err;
  if (protocol::ExecuteQuery(conn_.Socket(), "SET @source_binlog_checksum='CRC32'", &qr, &err,
                             conn_.DeprecateEofNegotiated()) != MES_OK) {
    SetLastError(err);
    return MES_ERR_STREAM;
  }
  return MES_OK;
}

mes_error_t BinlogClient::BeginSessionMariaDB() {
  gtid_encoded_.clear();

  protocol::QueryResult qr;
  std::string err;

  // Advertise MariaDB slave capability so the server sends GTID events
  // (type 162) instead of the legacy replication format that omits
  // per-transaction GTID events. The capability on its own does not enable
  // ANNOTATE_ROWS: the server still withholds those events unless the dump
  // request carries kBinlogSendAnnotateRows (see SendBinlogDumpMariaDB()).
  // Capability 4 = MARIA_SLAVE_CAPABILITY_GTID (MariaDB 10.0.2+)
  if (protocol::ExecuteQuery(conn_.Socket(), "SET @mariadb_slave_capability = 4", &qr, &err,
                             conn_.DeprecateEofNegotiated()) != MES_OK) {
    SetLastError("Failed to set MariaDB slave capability: " + err);
    StructuredLog().Event("mariadb_slave_capability_failed").Field("error", err).Error();
    return MES_ERR_STREAM;
  }

  // MariaDB uses @master_binlog_checksum (not @source_binlog_checksum)
  if (protocol::ExecuteQuery(conn_.Socket(),
                             "SET @master_binlog_checksum = @@global.binlog_checksum", &qr, &err,
                             conn_.DeprecateEofNegotiated()) != MES_OK) {
    SetLastError("Failed to set MariaDB binlog checksum: " + err);
    return MES_ERR_STREAM;
  }

  // Strict GTID mode: fail on GTID gap rather than silently skipping
  if (protocol::ExecuteQuery(conn_.Socket(), "SET @slave_gtid_strict_mode = 1", &qr, &err,
                             conn_.DeprecateEofNegotiated()) != MES_OK) {
    StructuredLog().Event("mariadb_strict_mode_failed").Field("error", err).Warn();
  }

  // Don't skip duplicate GTIDs
  if (protocol::ExecuteQuery(conn_.Socket(), "SET @slave_gtid_ignore_duplicates = 0", &qr, &err,
                             conn_.DeprecateEofNegotiated()) != MES_OK) {
    StructuredLog().Event("mariadb_ignore_duplicates_failed").Field("error", err).Warn();
  }

  return MES_OK;
}

mes_error_t BinlogClient::DetectBinlogChecksum() {
  // Assume "no checksum" until the server says otherwise, so a stale `true`
  // left by a previous stream on a reused object cannot survive a partial
  // detection failure. Detection must succeed: without the server's setting we
  // would not know whether to strip and validate the trailing CRC32, leading to
  // silent corruption or spurious checksum failures.
  checksum_enabled_ = false;

  protocol::QueryResult qr;
  std::string err;
  if (protocol::ExecuteQuery(conn_.Socket(), "SELECT @@GLOBAL.binlog_checksum", &qr, &err,
                             conn_.DeprecateEofNegotiated()) != MES_OK ||
      qr.rows.size() != 1 || qr.rows[0].values.size() != 1 || qr.rows[0].is_null.size() != 1 ||
      qr.rows[0].is_null[0]) {
    SetLastError(std::string("Failed to detect ") + GetServerFlavorName(server_flavor_) +
                 " binlog checksum setting: " + err);
    return MES_ERR_STREAM;
  }

  std::string value = qr.rows[0].values[0];
  std::transform(value.begin(), value.end(), value.begin(),
                 [](unsigned char ch) { return static_cast<char>(std::toupper(ch)); });
  if (value == "CRC32") {
    checksum_enabled_ = true;
    return MES_OK;
  }
  if (value == "NONE") {
    checksum_enabled_ = false;
    return MES_OK;
  }
  SetLastError(std::string("Unsupported ") + GetServerFlavorName(server_flavor_) +
               " binlog checksum setting: " + value);
  return MES_ERR_STREAM;
}

mes_error_t BinlogClient::EstablishStartState(StartState* state) {
  mes_error_t rc = DetectBinlogChecksum();
  if (rc != MES_OK) return rc;
  if (ConfigureHeartbeat() != MES_OK) return MES_ERR_STREAM;

  // A non-empty checkpoint is a position this client instance published, so it
  // is the only start position that neither skips the transactions committed
  // while the previous stream was down nor re-delivers the ones already handed
  // to the consumer. It therefore outranks every configured start mode,
  // including a file/offset anchor: the anchor says where that earlier stream
  // began, not where it stopped. An empty checkpoint means none was ever
  // published -- never "the empty GTID set" -- so the configuration is used
  // verbatim; forwarding an empty set would request every retained binlog.
  std::string resume_checkpoint;
  {
    std::lock_guard<std::mutex> lock(gtid_mutex_);
    resume_checkpoint = current_gtid_;
  }
  const std::string* resume = resume_checkpoint.empty() ? nullptr : &resume_checkpoint;

  state->gtid_set.clear();
  state->from_file_position = resume == nullptr && config_.start_at_file_position;
  if (!state->from_file_position) {
    rc = server_flavor_ == ServerFlavor::kMariaDB
             ? ResolveStartGtidMariaDB(resume, &state->gtid_set)
             : ResolveStartGtidMySQL(resume, &state->gtid_set);
    if (rc != MES_OK) return rc;
  }

  // A file/position start carries no GTID, but the tracker still has to be
  // bound to this flavor so the first observed transaction yields a valid
  // checkpoint. current_gtid_ stays empty until then, and an empty checkpoint
  // means "none established yet" -- never "the empty GTID set".
  if (!gtid_tracker_.Reset(state->gtid_set, server_flavor_)) {
    SetLastError(std::string("Invalid ") + GetServerFlavorName(server_flavor_) +
                 " GTID checkpoint set");
    return MES_ERR_INVALID_ARG;
  }
  // Publishing the resolved set cannot move the checkpoint backwards: it is
  // either the value current_gtid_ already held (the resume above) or the first
  // position this instance has ever established.
  {
    std::lock_guard<std::mutex> lock(gtid_mutex_);
    current_gtid_ = state->gtid_set;
  }
  return MES_OK;
}

mes_error_t BinlogClient::ResolveStartGtidMySQL(const std::string* resume_checkpoint,
                                                std::string* gtid_set) {
  // An omitted start GTID means "from the connection's current executed
  // position", not "from the beginning". Snapshot the full set immediately
  // before starting the dump so transactions committed afterwards are sent.
  // A resume checkpoint replaces both, and still goes through the encoding and
  // the purged preflight below: a checkpoint the source has since purged must
  // fail with MES_ERR_GTID_PURGED rather than silently skip transactions.
  std::string requested_gtid = config_.start_gtid;
  if (resume_checkpoint != nullptr) {
    requested_gtid = *resume_checkpoint;
  } else if (config_.start_at_current) {
    protocol::QueryResult current_qr;
    std::string current_err;
    mes_error_t current_rc =
        protocol::ExecuteQuery(conn_.Socket(), "SELECT @@GLOBAL.gtid_executed", &current_qr,
                               &current_err, conn_.DeprecateEofNegotiated());
    if (current_rc != MES_OK || current_qr.rows.size() != 1 ||
        current_qr.rows[0].values.size() != 1 || current_qr.rows[0].is_null.size() != 1 ||
        current_qr.rows[0].is_null[0]) {
      SetLastError("Failed to get current MySQL GTID position: " + current_err);
      return MES_ERR_STREAM;
    }
    requested_gtid = current_qr.rows[0].values[0];
  }

  const std::string start_gtid = GtidEncoder::ConvertSingleGtidToRange(requested_gtid);
  mes_error_t encode_rc = GtidEncoder::Encode(start_gtid.c_str(), &gtid_encoded_);
  if (encode_rc != MES_OK) {
    SetLastError(std::string("Failed to encode GTID set: ") + mes_error_string(encode_rc));
    return encode_rc;
  }

  // MySQL closes the replication connection for a request behind gtid_purged
  // instead of reliably returning an ERR packet. Check the server's advertised
  // purged set before COM_BINLOG_DUMP_GTID so callers get a useful, stable
  // diagnosis rather than a generic read failure.
  protocol::QueryResult purged_qr;
  std::string purged_err;
  if (protocol::ExecuteQuery(conn_.Socket(), "SELECT @@GLOBAL.gtid_purged", &purged_qr, &purged_err,
                             conn_.DeprecateEofNegotiated()) != MES_OK ||
      purged_qr.rows.size() != 1 || purged_qr.rows[0].values.size() != 1 ||
      purged_qr.rows[0].is_null.size() != 1 || purged_qr.rows[0].is_null[0]) {
    SetLastError("Failed to get MySQL purged GTID set: " + purged_err);
    return MES_ERR_STREAM;
  }
  if (!purged_qr.rows[0].values[0].empty()) {
    GtidSet requested_set;
    GtidSet purged_set;
    if (GtidSet::Parse(start_gtid, &requested_set) != MES_OK ||
        GtidSet::Parse(purged_qr.rows[0].values[0], &purged_set) != MES_OK) {
      SetLastError("Failed to parse MySQL purged GTID set");
      return MES_ERR_STREAM;
    }
    if (!purged_set.IsSubsetOf(requested_set)) {
      const std::string diagnostic =
          "Requested GTID position is missing transactions purged by the source: " +
          purged_set.ToString();
      SetLastError(diagnostic);
      StructuredLog()
          .Event("gtid_purged_preflight")
          .Field("purged_gtid", purged_set.ToString())
          .Error();
      return MES_ERR_GTID_PURGED;
    }
  }

  *gtid_set = start_gtid;
  return MES_OK;
}

mes_error_t BinlogClient::ResolveStartGtidMariaDB(const std::string* resume_checkpoint,
                                                  std::string* gtid_set) {
  std::string gtid = config_.start_gtid;
  if (resume_checkpoint != nullptr) {
    // A resume checkpoint is the domain high-water set this client last
    // published, which is exactly what @slave_connect_state expects.
    gtid = *resume_checkpoint;
  } else if (config_.start_at_current) {
    auto query_gtid_position = [this](const char* query, std::string* value,
                                      std::string* query_error) -> bool {
      protocol::QueryResult result;
      if (protocol::ExecuteQuery(conn_.Socket(), query, &result, query_error,
                                 conn_.DeprecateEofNegotiated()) != MES_OK ||
          result.rows.size() != 1 || result.rows[0].values.size() != 1 ||
          result.rows[0].is_null.size() != 1 || result.rows[0].is_null[0]) {
        return false;
      }
      *value = result.rows[0].values[0];
      return true;
    };

    // A replica can have GTIDs in gtid_current_pos that were received from
    // upstream but never written to its own binlog. Only gtid_binlog_pos is
    // a safe default for requesting this server's binlog; use current_pos
    // solely when this server has not written any GTID yet.
    std::string position_error;
    if (!query_gtid_position("SELECT @@GLOBAL.gtid_binlog_pos", &gtid, &position_error)) {
      SetLastError("Failed to get MariaDB binlog GTID position: " + position_error);
      return MES_ERR_STREAM;
    }
    if (gtid.empty() &&
        !query_gtid_position("SELECT @@GLOBAL.gtid_current_pos", &gtid, &position_error)) {
      SetLastError("Failed to get current MariaDB GTID position: " + position_error);
      return MES_ERR_STREAM;
    }
  }
  if (!IsValidMariaDBGtidSet(gtid)) {
    SetLastError("Invalid MariaDB GTID format: contains disallowed characters");
    return MES_ERR_INVALID_ARG;
  }

  *gtid_set = gtid;
  return MES_OK;
}

mes_error_t BinlogClient::SendBinlogDumpMySQL(const StartState& state) {
  protocol::BinlogStreamConfig stream_config;
  stream_config.server_id = config_.server_id;

  if (state.from_file_position) {
    stream_config.binlog_filename = config_.binlog_file;
    stream_config.binlog_position = config_.binlog_position;
    const mes_error_t rc = binlog_stream_.StartComBinlogDump(conn_.Socket(), stream_config);
    if (rc != MES_OK) {
      SetLastError("Failed to start binlog stream at requested position");
      return rc;
    }
    return MES_OK;
  }

  // Start binlog stream via COM_BINLOG_DUMP_GTID
  stream_config.binlog_position = kBinlogMagicOffset;
  stream_config.gtid_encoded = gtid_encoded_;
  if (binlog_stream_.Start(conn_.Socket(), stream_config) != MES_OK) {
    SetLastError("Failed to start binlog stream");
    return MES_ERR_STREAM;
  }
  return MES_OK;
}

mes_error_t BinlogClient::SendBinlogDumpMariaDB(const StartState& state) {
  protocol::BinlogStreamConfig stream_config;
  stream_config.server_id = config_.server_id;
  // ANNOTATE_ROWS carries the originating statement published as
  // mes_event_t.source_sql. MariaDB drops those events unless every dump
  // request -- GTID or file/position -- asks for them explicitly.
  stream_config.flags |= protocol::kBinlogSendAnnotateRows;

  if (state.from_file_position) {
    stream_config.binlog_filename = config_.binlog_file;
    stream_config.binlog_position = config_.binlog_position;
    const mes_error_t rc = binlog_stream_.StartComBinlogDump(conn_.Socket(), stream_config);
    if (rc != MES_OK) {
      SetLastError("Failed to start MariaDB binlog stream at requested position");
      return rc;
    }
    return MES_OK;
  }

  // MariaDB reads @slave_connect_state to know which GTIDs the replica has.
  // An empty string means the replica holds nothing, so the server streams from
  // the oldest retained binlog. Re-check the character set next to the
  // interpolation itself; the set reaches here from ResolveStartGtidMariaDB(),
  // and this query is the injection sink.
  if (!IsValidMariaDBGtidSet(state.gtid_set)) {
    SetLastError("Invalid MariaDB GTID format: contains disallowed characters");
    return MES_ERR_INVALID_ARG;
  }
  protocol::QueryResult qr;
  std::string err;
  const std::string gtid_query = "SET @slave_connect_state = '" + state.gtid_set + "'";
  if (protocol::ExecuteQuery(conn_.Socket(), gtid_query, &qr, &err,
                             conn_.DeprecateEofNegotiated()) != MES_OK) {
    SetLastError("Failed to set slave_connect_state: " + err);
    return MES_ERR_STREAM;
  }
  StructuredLog().Event("mariadb_gtid_state_set").Field("gtid", state.gtid_set).Debug();

  // Start binlog stream via COM_BINLOG_DUMP (not COM_BINLOG_DUMP_GTID)
  stream_config.binlog_position = kBinlogMagicOffset;
  if (binlog_stream_.StartComBinlogDump(conn_.Socket(), stream_config) != MES_OK) {
    SetLastError("Failed to start MariaDB binlog stream");
    return MES_ERR_STREAM;
  }
  return MES_OK;
}

mes_error_t BinlogClient::ConfigureHeartbeat() {
  const uint64_t heartbeat_period_ns = HeartbeatPeriodNs(config_.read_timeout_s);
  protocol::QueryResult qr;
  std::string err;
  const std::string query = "SET @master_heartbeat_period = " + std::to_string(heartbeat_period_ns);
  const mes_error_t rc =
      protocol::ExecuteQuery(conn_.Socket(), query, &qr, &err, conn_.DeprecateEofNegotiated());
  if (rc != MES_OK) {
    SetLastError("Failed to configure binlog heartbeat: " + err);
    StructuredLog()
        .Event("heartbeat_setup_failed")
        .Field("period_ns", heartbeat_period_ns)
        .Field("error", err)
        .Error();
    return MES_ERR_STREAM;
  }
  StructuredLog().Event("heartbeat_configured").Field("period_ns", heartbeat_period_ns).Debug();
  return MES_OK;
}

void BinlogClient::ReaderLoop() {
  struct CloseQueueOnExit {
    EventQueue* queue;
    ~CloseQueueOnExit() { queue->Close(); }
  } close_queue{event_queue_.get()};

  StructuredLog().Event("binlog_reader_started").Debug();
  while (!stop_requested_.load(std::memory_order_acquire)) {
    protocol::BinlogEventPacket event_pkt;
    mes_error_t rc =
        binlog_stream_.FetchEvent(conn_.Socket(), &reader_scratch_, &event_pkt, max_event_size_);

    // Check stop flag after blocking call returns
    if (stop_requested_.load(std::memory_order_acquire)) {
      break;
    }

    if (rc != MES_OK) {
      connected_.store(false, std::memory_order_release);
      // Push error sentinel so Poll() can surface the error. If the push fails
      // the queue was closed concurrently (shutdown race); the specific code
      // would otherwise be lost, so record it for diagnostics.
      QueuedEvent err_event;
      err_event.error = rc;
      err_event.server_error_code = event_pkt.server_error_code;
      err_event.error_message = std::move(event_pkt.error_message);
      if (!event_queue_->Push(std::move(err_event))) {
        StructuredLog()
            .Event("binlog_error")
            .Field("type", "error_sentinel_push_dropped")
            .Field("error_code", static_cast<int64_t>(rc))
            .Warn();
      }
      return;
    }

    // Heartbeat: surface to the consumer as an empty event with
    // is_heartbeat=true. The public mes_poll_result_t contract (see mes.h)
    // documents is_heartbeat as a first-class signal; dropping heartbeats
    // here would make that field unreachable. Consumers that do not care
    // about heartbeats can filter on `data == nullptr` (matches the Node
    // and Python high-level streams, which already skip null-data results).
    if (event_pkt.is_heartbeat) {
      QueuedEvent hb;
      hb.is_heartbeat = true;
      hb.error = MES_OK;
      if (!event_queue_->Push(std::move(hb))) {
        return;  // queue closed
      }
      continue;
    }

    // Every binlog file carries its authoritative algorithm in the FDE. This
    // can differ from the current global setting for historical files, so let
    // the FDE update the wire verifier before checking this event.
    EventHeader wire_header;
    const bool has_wire_header = ParseEventHeader(event_pkt.data, event_pkt.size, &wire_header) &&
                                 wire_header.event_length == event_pkt.size;
    if (has_wire_header &&
        wire_header.type_code == static_cast<uint8_t>(BinlogEventType::kFormatDescriptionEvent)) {
      switch (DetectFormatDescriptionChecksum(event_pkt.data, event_pkt.size)) {
        case BinlogChecksumAlgorithm::kCrc32:
          checksum_enabled_ = true;
          break;
        case BinlogChecksumAlgorithm::kOff:
          checksum_enabled_ = false;
          break;
        case BinlogChecksumAlgorithm::kUnknown:
          break;
      }
    }

    bool event_has_checksum = checksum_enabled_.load(std::memory_order_acquire);
    // MySQL sends the synthetic dump-start ROTATE with CRC32 even when the
    // persisted binlog mode is NONE. Detect that one artificial event by its
    // flag and a valid trailer; the following FDE remains authoritative for
    // the rest of the file.
    if (!event_has_checksum && has_wire_header &&
        wire_header.type_code == static_cast<uint8_t>(BinlogEventType::kRotateEvent) &&
        (wire_header.flags & kLogEventArtificialFlag) != 0 &&
        event_pkt.size >= kEventHeaderSize + kChecksumSize) {
      const size_t data_length = event_pkt.size - kChecksumSize;
      event_has_checksum = ComputeCRC32(event_pkt.data, data_length) ==
                           binary::ReadU32Le(event_pkt.data + data_length);
    }

    // Verify CRC32 checksum for data integrity.
    // MySQL appends a 4-byte CRC32 to every event when @source_binlog_checksum='CRC32'.
    // MariaDB may have binlog_checksum=NONE, in which case we skip verification.
    if (event_has_checksum && event_pkt.size >= kEventHeaderSize + kChecksumSize) {
      const size_t data_length = event_pkt.size - kChecksumSize;
      uint32_t computed_crc = ComputeCRC32(event_pkt.data, data_length);
      uint32_t stored_crc = binary::ReadU32Le(event_pkt.data + data_length);
      if (computed_crc != stored_crc) {
        crc_errors_.fetch_add(1, std::memory_order_relaxed);
        StructuredLog()
            .Event("binlog_error")
            .Field("type", "crc32_checksum_mismatch")
            .Field("computed_crc", static_cast<uint64_t>(computed_crc))
            .Field("stored_crc", static_cast<uint64_t>(stored_crc))
            .Field("event_length", static_cast<uint64_t>(event_pkt.size))
            .Error();
        // Push error event so the consumer can detect the corrupted event. If
        // the push fails (queue closed during shutdown) the code is lost from
        // the queue, so record it for diagnostics.
        connected_.store(false, std::memory_order_release);
        QueuedEvent crc_err;
        crc_err.error = MES_ERR_CHECKSUM;
        crc_err.error_message = "CRC32 checksum mismatch";
        if (!event_queue_->Push(std::move(crc_err))) {
          StructuredLog()
              .Event("binlog_error")
              .Field("type", "error_sentinel_push_dropped")
              .Field("error_code", static_cast<int64_t>(MES_ERR_CHECKSUM))
              .Warn();
        }
        return;  // Stop reader; consistent with stream error handling above
      }
    }

    // Keep received GTIDs on the reader thread. A checkpoint is attached only
    // when this event proves the transaction committed; Poll() promotes it
    // after the consumer has finished the event, never while it is queued.
    std::string checkpoint_gtid =
        gtid_tracker_.Observe(event_pkt.data, event_pkt.size, event_has_checksum);

    // Move the packet buffer into the queue. event_pkt.data_offset (typically
    // 1 to skip the OK byte) lets the consumer locate the real event bytes
    // inside the moved buffer without an intermediate copy. After the move,
    // reader_scratch_ is left in a valid-but-unspecified (empty) state; the
    // next FetchEvent() will resize it as needed.
    QueuedEvent qe;
    qe.data = std::move(reader_scratch_);
    qe.data_offset = event_pkt.data_offset;
    qe.error = MES_OK;
    qe.checkpoint_gtid = std::move(checkpoint_gtid);

    const EventQueue::PushResult push_result = event_queue_->PushWithStatus(std::move(qe));
    if (push_result == EventQueue::PushResult::kEventTooLarge) {
      StructuredLog()
          .Event("binlog_error")
          .Field("type", "event_exceeds_queue_byte_budget")
          .Field("max_queue_bytes", static_cast<uint64_t>(max_queue_bytes_))
          .Error();
      connected_.store(false, std::memory_order_release);
      QueuedEvent budget_error;
      budget_error.error = MES_ERR_QUEUE_FULL;
      budget_error.error_message = "Binlog event exceeds max_queue_bytes";
      event_queue_->Push(std::move(budget_error));
      return;
    }
    if (push_result == EventQueue::PushResult::kClosed) {
      // Queue was closed (shutdown in progress)
      return;
    }
  }
}

void BinlogClient::SetMaxEventSize(uint32_t max_event_size) {
  max_event_size_ = NormalizeMaxEventSize(max_event_size);
}

uint32_t BinlogClient::MaxEventSize() const { return max_event_size_; }

void BinlogClient::SetMaxQueueBytes(size_t max_queue_bytes) {
  max_queue_bytes_ = max_queue_bytes == 0 ? kDefaultEventQueueBytes : max_queue_bytes;
}

size_t BinlogClient::MaxQueueBytes() const { return max_queue_bytes_; }

size_t BinlogClient::QueuedBytes() const {
  // Held across the queue's own accessor so the pointer cannot be replaced --
  // and the queue behind it destroyed -- between the test and the call.
  std::lock_guard<std::mutex> queue_lock(queue_ptr_mutex_);
  return event_queue_ ? event_queue_->QueuedBytes() : 0;
}

PollResult BinlogClient::Poll() {
  // Calling Poll() again is the implicit acknowledgement that the caller has
  // finished using the previous event buffer. Promote a commit checkpoint at
  // that point, before a subsequent error can trigger reconnect logic.
  PromoteDeliveredCheckpoint();
  // The contract makes the pointer returned by the previous call invalid as
  // soon as this one begins, so every buffer it handed out is released here --
  // on every exit path below. Deferring it to the next data event would keep a
  // full event payload resident for the whole life of a client that goes on to
  // see only heartbeats, a terminal error, or nothing at all.
  ResetDeliveredEvents();

  // Both disconnect paths record a message: the C ABI exposes the last error
  // as the only description a binding can attach to the rejected poll, and an
  // empty one leaves the consumer with a bare error code.
  if (!streaming_.load(std::memory_order_acquire) || !event_queue_) {
    // Stop() latches until a fresh Connect(), so after a stop every start is
    // refused. Naming start() there would send the caller to the one action the
    // client has already ruled out.
    SetLastError(stop_requested_.load(std::memory_order_acquire)
                     ? "Poll on a client that was stopped; reconnect before polling again"
                     : "Poll on a client that is not streaming; call start() first");
    return {MES_ERR_DISCONNECTED, nullptr, 0, false};
  }

  QueuedEvent event;
  if (!event_queue_->Pop(&event)) {
    // Queue closed (shutdown)
    streaming_.store(false, std::memory_order_release);
    SetLastError("Binlog stream stopped while polling");
    return {MES_ERR_DISCONNECTED, nullptr, 0, false};
  }

  // Heartbeat: empty data, is_heartbeat=true. No current_event_ buffer
  // update because there is no data to retain.
  if (event.is_heartbeat) {
    return {MES_OK, nullptr, 0, true};
  }

  if (event.error != MES_OK) {
    // Error from reader thread. Snapshot the current GTID under the mutex
    // directly (instead of routing through GetCurrentGtid() which returns
    // a pointer into a shared buffer) so the log line's GTID remains valid
    // for the duration of the structured log build-up, and so we avoid the
    // "valid-until-next-call" contract that GetCurrentGtid() carries.
    std::string gtid_snap;
    {
      std::lock_guard<std::mutex> lock(gtid_mutex_);
      gtid_snap = current_gtid_;
    }
    const std::string err_msg =
        event.error_message.empty() ? "Binlog stream read error" : event.error_message;
    SetLastError(err_msg);
    StructuredLog()
        .Event("binlog_error")
        .Field("type", "poll_error")
        .Field("gtid", gtid_snap)
        .Field("error", err_msg)
        .Field("error_code", static_cast<int64_t>(event.error))
        .Field("server_error_code", static_cast<uint64_t>(event.server_error_code))
        .Error();
    streaming_.store(false, std::memory_order_release);
    return {event.error, nullptr, 0, false};
  }

  // Store event data so pointer remains valid until next Poll().
  // Apply data_offset to skip the OK byte prefix kept in the buffer so
  // ownership could be moved in from the reader thread without a copy.
  current_event_ = std::move(event);
  const size_t offset = current_event_.data_offset;
  const uint8_t* payload = current_event_.data.data() + offset;
  const size_t payload_size = current_event_.data.size() - offset;
  return {MES_OK, payload, payload_size, false};
}

size_t BinlogClient::PollBatch(size_t max_events, std::vector<PollResult>* results) {
  if (results == nullptr || max_events == 0) return 0;
  results->clear();

  // The first Poll() is also this batch's acknowledgement boundary: it promotes
  // the checkpoints of the events the prior call delivered and releases their
  // buffers, so nothing from that call is still held once this one starts
  // filling batch_events_ again.
  PollResult first = Poll();
  if (first.data == nullptr) {
    results->push_back(first);
    return results->size();
  }
  // `max_events` is supplied by C ABI callers. Avoid reserving an
  // unbounded amount of memory up front; the vector grows only as events are
  // actually available.
  batch_events_.reserve(std::min(max_events, static_cast<size_t>(1024)));
  batch_events_.push_back(std::move(current_event_));
  {
    const QueuedEvent& held = batch_events_.back();
    results->push_back(
        {MES_OK, held.data.data() + held.data_offset, held.data.size() - held.data_offset, false});
  }

  while (results->size() < max_events) {
    QueuedEvent event;
    if (!event_queue_ || !event_queue_->TryPop(&event)) break;
    if (event.is_heartbeat) {
      results->push_back({MES_OK, nullptr, 0, true});
      continue;
    }
    if (event.error != MES_OK) {
      const std::string message =
          event.error_message.empty() ? "Binlog stream read error" : event.error_message;
      SetLastError(message);
      streaming_.store(false, std::memory_order_release);
      results->push_back({event.error, nullptr, 0, false});
      break;
    }
    batch_events_.push_back(std::move(event));
    const QueuedEvent& held = batch_events_.back();
    results->push_back(
        {MES_OK, held.data.data() + held.data_offset, held.data.size() - held.data_offset, false});
  }
  return results->size();
}

void BinlogClient::Stop() {
  std::lock_guard<std::mutex> lock(stop_mutex_);
  StopReaderThread();
}

void BinlogClient::StopReaderThread() {
  // Shutdown order (must match FetchEvent unblock contract):
  //   1. stop_requested_  -> reader loop condition flips to break on next iter
  //   2. streaming_       -> Poll() fast-path short-circuits for any concurrent
  //                          caller before event_queue_ is destroyed below
  //   3. event_queue_.Close() -> unblocks Pop() in Poll() and Push() in reader
  //   4. socket.Shutdown() -> unblocks a FetchEvent() currently in recv(). The
  //      protocol layer relies on the socket's half-close causing the pending
  //      syscall to return with an error; reader thread then sees
  //      stop_requested_ and exits cleanly.
  //   5. join() the reader.
  stop_requested_.store(true, std::memory_order_release);
  streaming_.store(false, std::memory_order_release);
  connected_.store(false, std::memory_order_release);

  if (event_queue_) {
    event_queue_->Close();
  }

  // Connect() can be blocked after TCP establishment but before it marks the
  // logical connection as connected. Shutdown the valid transport too, so a
  // concurrent Stop() can interrupt that handshake/authentication path.
  if (conn_.Socket()->IsValid()) {
    conn_.Socket()->Shutdown();
  }

  if (reader_thread_.joinable()) {
    reader_thread_.join();
  }

  // NOTE(thread-safety): event_queue_ is intentionally NOT reset here.
  // Poll() reads event_queue_ without a lock (only streaming_ is atomic),
  // and Stop() can be called from any thread. Resetting event_queue_ here
  // would race with Poll()'s non-atomic read. The closed queue stays alive
  // until StartStream() replaces it or the destructor runs.
  //
  // current_event_ and batch_events_ are held back for the same reason: Poll()
  // writes them without a lock, so resetting them from a Stop() on another
  // thread would be a data race. They are released by the owner thread instead,
  // at the next Poll()/PollBatch(), at Connect()/StartStream(), by Disconnect()
  // below this call, or at destruction.
  if (event_queue_) {
    event_queue_->Clear();
  }
}

void BinlogClient::Disconnect() {
  std::lock_guard<std::mutex> lock(stop_mutex_);
  StopReaderThread();
  // Safe here where it is not inside Stop(): Disconnect() is an owner-thread
  // entry point, and the reader has just been joined.
  ResetDeliveredEvents();
  // Keep descriptor destruction in the same critical section as Stop's
  // shutdown. SocketHandle additionally serializes shutdown() with close(),
  // so a descriptor number cannot be reused between those operations.
  conn_.Disconnect();
  connected_.store(false, std::memory_order_release);
  StructuredLog().Event("mysql_disconnected").Info();
}

bool BinlogClient::IsConnected() const { return connected_.load(std::memory_order_acquire); }

bool BinlogClient::IsStreaming() const { return streaming_.load(std::memory_order_acquire); }

ServerFlavor BinlogClient::GetServerFlavor() const { return server_flavor_; }

const char* BinlogClient::GetLastError() const {
  // Snapshot last_error_ into a separate buffer so the returned c_str() is
  // not invalidated by a concurrent writer resizing last_error_. Per mes.h
  // contract, the pointer is valid until the next GetLastError() call on
  // this BinlogClient.
  std::lock_guard<std::mutex> lock(last_error_mutex_);
  last_error_snapshot_ = last_error_;
  return last_error_snapshot_.c_str();
}

const char* BinlogClient::GetCurrentGtid() const {
  // Note: gtid_snapshot_ is a shared buffer protected by
  // gtid_mutex_. The returned pointer is valid only until the next
  // GetCurrentGtid() call on this BinlogClient. If multiple threads call
  // this method concurrently, they must synchronize externally to avoid
  // one caller's pointer being invalidated by another's assignment.
  std::lock_guard<std::mutex> lock(gtid_mutex_);
  gtid_snapshot_ = current_gtid_;
  return gtid_snapshot_.c_str();
}

void BinlogClient::PromoteDeliveredCheckpoint() {
  for (auto& event : batch_events_) {
    if (event.checkpoint_gtid.empty()) continue;
    std::lock_guard<std::mutex> lock(gtid_mutex_);
    current_gtid_ = std::move(event.checkpoint_gtid);
    event.checkpoint_gtid.clear();
  }
  if (current_event_.checkpoint_gtid.empty()) return;
  std::lock_guard<std::mutex> lock(gtid_mutex_);
  current_gtid_ = std::move(current_event_.checkpoint_gtid);
  current_event_.checkpoint_gtid.clear();
}

void BinlogClient::ResetDeliveredEvents() {
  // Assigning a fresh QueuedEvent hands the payload back to the allocator; the
  // batch keeps its slot capacity, which holds no payload once cleared, so a
  // steady stream of batches does not reallocate the array on every call.
  current_event_ = {};
  batch_events_.clear();
}

uint64_t BinlogClient::GetCRCErrors() const { return crc_errors_.load(std::memory_order_relaxed); }

bool BinlogClient::ChecksumEnabled() const {
  return checksum_enabled_.load(std::memory_order_acquire);
}

}  // namespace mes
