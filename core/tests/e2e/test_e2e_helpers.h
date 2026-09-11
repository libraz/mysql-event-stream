// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @file test_e2e_helpers.h
 * @brief Shared helpers for E2E protocol tests
 */

#ifndef MES_TEST_E2E_HELPERS_H_
#define MES_TEST_E2E_HELPERS_H_

#include <atomic>
#include <cctype>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <string>
#include <thread>
#include <vector>

#include "mes.h"
#include "protocol/mysql_connection.h"
#include "protocol/mysql_query.h"
#include "server_flavor.h"

namespace e2e {

// Each E2E test uses a unique server_id to avoid binlog session conflicts.
// Ranges: Protocol 100-199, SSL 200-299, Auth 300-399,
//         DML 500-599, Engine 600-699, Buffer 700-799, Start modes 800-899
namespace server_ids {
// test_e2e_start_mode.cpp
constexpr uint32_t kStartModeCurrent = 800;
constexpr uint32_t kStartModeExplicitGtid = 801;
constexpr uint32_t kStartModeFilePosition = 802;
constexpr uint32_t kStartModeFilePositionCheckpoint = 803;

// test_e2e_binlog_dml.cpp
constexpr uint32_t kDmlInsertAllColumns = 500;
constexpr uint32_t kDmlInsertWithNulls = 501;
constexpr uint32_t kDmlInsertMultiRow = 502;
constexpr uint32_t kDmlUpdateSubset = 503;
constexpr uint32_t kDmlUpdateAllColumns = 504;
constexpr uint32_t kDmlDeleteSingleRow = 505;
constexpr uint32_t kDmlMultiRowUpdate = 506;
constexpr uint32_t kDmlMultiRowDelete = 507;
constexpr uint32_t kDmlTransactionMultipleDml = 508;
constexpr uint32_t kDmlLargeTextValue = 509;
constexpr uint32_t kDmlLargeBlobValue = 510;
constexpr uint32_t kDmlUnicodeAndEmoji = 511;
constexpr uint32_t kDmlDecimalMaxPrecision = 512;
constexpr uint32_t kDmlDecimalNegative = 513;
constexpr uint32_t kDmlNullToNonNullUpdate = 514;
constexpr uint32_t kDmlNonNullToNullUpdate = 515;
constexpr uint32_t kDmlEmptyStringVsNull = 516;
constexpr uint32_t kDmlBooleanValues = 517;
constexpr uint32_t kDmlMariaCompressedColumns = 518;
constexpr uint32_t kDmlMysqlMultiValuedIndex = 519;
constexpr uint32_t kDmlVectorInsert = 520;
constexpr uint32_t kDmlVectorUpdate = 521;
constexpr uint32_t kDmlVectorDelete = 522;
constexpr uint32_t kDmlClientEventSizeLimit = 523;
constexpr uint32_t kDmlClientQueueByteBudget = 524;
constexpr uint32_t kDmlYearSignedness = 525;
constexpr uint32_t kDmlCharsetMetadata = 526;
constexpr uint32_t kDmlExtendedTypes = 527;
constexpr uint32_t kDmlMariaAnnotateSourceSql = 528;
constexpr uint32_t kDmlFullRowMetadata = 529;
constexpr uint32_t kDmlVectorCharsetSlots = 530;
}  // namespace server_ids

// Connection defaults
constexpr const char* kHost = "127.0.0.1";
constexpr uint16_t kPort = 13308;
constexpr const char* kRootUser = "root";
constexpr const char* kRootPass = "test_root_password";
constexpr const char* kReplUser = "repl_user";
constexpr const char* kReplPass = "test_password";
constexpr uint32_t kTimeout = 5;

// Detect DB flavor from environment (set by run-matrix.sh)
inline mes::ServerFlavor GetDbFlavor() {
  const char* env = std::getenv("DB_FLAVOR");
  if (env && std::string(env) == "mariadb") {
    return mes::ServerFlavor::kMariaDB;
  }
  return mes::ServerFlavor::kMySQL;
}

inline bool IsMariaDB() { return GetDbFlavor() == mes::ServerFlavor::kMariaDB; }

// MariaDB uses mysql_native_password (no TLS required for auth).
// MySQL 8.4+ uses caching_sha2_password (certificate-verified TLS is needed for full auth).
inline uint32_t DefaultSslMode() { return IsMariaDB() ? MES_SSL_DISABLED : MES_SSL_VERIFY_CA; }

// SSL cert paths (relative to project root)
// Use absolute paths derived at compile time or pass from environment
inline std::string CertDir() {
  // Walk up from the build directory to find the certs
  const char* env = std::getenv("MES_CERT_DIR");
  if (env && env[0]) return env;
  // Default: relative to typical build location
  return std::string(MES_PROJECT_ROOT) + "/e2e/docker/certs";
}

inline std::string CaCert() { return CertDir() + "/ca.pem"; }
inline std::string ClientCert() { return CertDir() + "/client-cert.pem"; }
inline std::string ClientKey() { return CertDir() + "/client-key.pem"; }
inline std::string WrongCa() { return CertDir() + "/wrong-ca.pem"; }

inline std::string DefaultCa() { return IsMariaDB() ? "" : CaCert(); }

inline bool IsE2eServerAvailable(std::string* error = nullptr) {
  mes::protocol::MysqlConnection conn;
  auto rc = conn.Connect(kHost, kPort, kRootUser, kRootPass, kTimeout, kTimeout, DefaultSslMode(),
                         DefaultCa(), "", "");
  if (rc != MES_OK) {
    if (error) *error = conn.GetLastError();
    return false;
  }
  conn.Disconnect();
  return true;
}

// Captured column value (deep-copied from transient mes_column_t)
struct CapturedColumn {
  mes_col_type_t type = MES_COL_NULL;
  int64_t int_val = 0;
  double double_val = 0.0;
  std::string str_data;
  std::string col_name;
};

// Captured event (deep-copied from transient mes_event_t)
struct CapturedEvent {
  mes_event_type_t type = MES_EVENT_INSERT;
  std::string database;
  std::string table;
  std::vector<CapturedColumn> before;
  std::vector<CapturedColumn> after;
  uint32_t timestamp = 0;
  bool names_resolved = false;
  std::string source_sql;
};

inline CapturedColumn CopyColumn(const mes_column_t& c) {
  CapturedColumn cc;
  cc.type = c.type;
  cc.int_val = c.int_val;
  cc.double_val = c.double_val;
  if (c.str_data && c.str_len > 0) {
    cc.str_data.assign(c.str_data, c.str_len);
  }
  cc.col_name = c.col_name ? c.col_name : "";
  return cc;
}

inline CapturedEvent CopyEvent(const mes_event_t* e) {
  CapturedEvent ce;
  ce.type = e->type;
  ce.database = e->database ? e->database : "";
  ce.table = e->table ? e->table : "";
  ce.timestamp = e->timestamp;
  ce.names_resolved = e->names_resolved != 0;
  ce.source_sql = e->source_sql ? e->source_sql : "";
  for (uint32_t i = 0; i < e->before_count; i++) {
    ce.before.push_back(CopyColumn(e->before_columns[i]));
  }
  for (uint32_t i = 0; i < e->after_count; i++) {
    ce.after.push_back(CopyColumn(e->after_columns[i]));
  }
  return ce;
}

// Get current GTID executed from server (flavor-aware)
inline std::string GetCurrentGtid() {
  mes::protocol::MysqlConnection conn;
  if (conn.Connect(kHost, kPort, kRootUser, kRootPass, kTimeout, kTimeout, DefaultSslMode(),
                   DefaultCa(), "", "") != MES_OK)
    return "";
  mes::protocol::QueryResult qr;
  std::string err;
  // MariaDB: @@GLOBAL.gtid_current_pos; MySQL: @@GLOBAL.gtid_executed
  const char* query =
      IsMariaDB() ? "SELECT @@GLOBAL.gtid_current_pos" : "SELECT @@GLOBAL.gtid_executed";
  if (mes::protocol::ExecuteQuery(conn.Socket(), query, &qr, &err) != MES_OK) return "";
  if (qr.rows.empty()) return "";
  return qr.rows[0].values[0];
}

// Read one scalar column from a single-row query as root (flavor-aware TLS).
// Returns an empty string when the query fails or returns no row.
inline std::string QueryScalar(const std::string& sql) {
  mes::protocol::MysqlConnection conn;
  if (conn.Connect(kHost, kPort, kRootUser, kRootPass, kTimeout, kTimeout, DefaultSslMode(),
                   DefaultCa(), "", "") != MES_OK)
    return "";
  mes::protocol::QueryResult qr;
  std::string err;
  if (mes::protocol::ExecuteQuery(conn.Socket(), sql, &qr, &err) != MES_OK) return "";
  if (qr.rows.empty() || qr.rows[0].values.empty()) return "";
  return qr.rows[0].values[0];
}

// Server-side binlog_checksum setting, uppercased ("CRC32" or "NONE").
inline std::string GetBinlogChecksumSetting() {
  std::string value = QueryScalar("SELECT @@GLOBAL.binlog_checksum");
  for (char& ch : value) {
    ch = static_cast<char>(std::toupper(static_cast<unsigned char>(ch)));
  }
  return value;
}

// Current binlog file name and offset, for MES_START_AT_POSITION starts.
struct BinlogCoordinates {
  std::string file;
  uint64_t position = 0;
};

inline BinlogCoordinates GetCurrentBinlogCoordinates() {
  BinlogCoordinates coords;
  mes::protocol::MysqlConnection conn;
  if (conn.Connect(kHost, kPort, kRootUser, kRootPass, kTimeout, kTimeout, DefaultSslMode(),
                   DefaultCa(), "", "") != MES_OK)
    return coords;
  mes::protocol::QueryResult qr;
  std::string err;
  // SHOW MASTER STATUS was removed in MySQL 8.4 in favour of
  // SHOW BINARY LOG STATUS; MariaDB only knows the former.
  const char* query = IsMariaDB() ? "SHOW MASTER STATUS" : "SHOW BINARY LOG STATUS";
  if (mes::protocol::ExecuteQuery(conn.Socket(), query, &qr, &err) != MES_OK) return coords;
  if (qr.rows.empty() || qr.rows[0].values.size() < 2) return coords;
  coords.file = qr.rows[0].values[0];
  coords.position = std::strtoull(qr.rows[0].values[1].c_str(), nullptr, 10);
  return coords;
}

// Execute a DML/DDL statement as root (flavor-aware TLS)
inline mes_error_t ExecuteDML(const std::string& sql) {
  mes::protocol::MysqlConnection conn;
  auto rc = conn.Connect(kHost, kPort, kRootUser, kRootPass, kTimeout, kTimeout, DefaultSslMode(),
                         DefaultCa(), "", "");
  if (rc != MES_OK) return rc;
  mes::protocol::QueryResult qr;
  std::string err;
  return mes::protocol::ExecuteQuery(conn.Socket(), sql, &qr, &err);
}

// Capture events from binlog stream using a predicate.
// Returns captured events. Stops when predicate returns true or max_polls reached.
using EventPredicate = std::function<bool(const std::vector<CapturedEvent>&)>;

inline std::vector<CapturedEvent> CaptureEvents(const std::string& start_gtid, uint32_t server_id,
                                                const EventPredicate& done_pred,
                                                int max_polls = 100,
                                                mes_engine_t* external_engine = nullptr,
                                                mes_error_t* feed_error = nullptr) {
  std::vector<CapturedEvent> events;

  mes_client_t* client = mes_client_create();
  if (!client) return events;

  mes_client_config_t config{};
  config.host = kHost;
  config.port = kPort;
  config.user = kReplUser;
  config.password = kReplPass;
  config.server_id = server_id;
  config.start_gtid = start_gtid.c_str();
  config.connect_timeout_s = kTimeout;
  config.read_timeout_s = 3;
  config.ssl_mode = static_cast<mes_ssl_mode_t>(DefaultSslMode());
  std::string ca_path = DefaultCa();
  config.ssl_ca = ca_path.empty() ? nullptr : ca_path.c_str();
  config.ssl_cert = nullptr;
  config.ssl_key = nullptr;

  if (mes_client_connect(client, &config) != MES_OK) {
    mes_client_destroy(client);
    return events;
  }
  if (mes_client_start(client) != MES_OK) {
    mes_client_disconnect(client);
    mes_client_destroy(client);
    return events;
  }

  mes_engine_t* engine = external_engine ? external_engine : mes_create();
  if (!engine) {
    mes_client_stop(client);
    mes_client_disconnect(client);
    mes_client_destroy(client);
    return events;
  }

  for (int i = 0; i < max_polls; i++) {
    auto result = mes_client_poll(client);
    if (result.error != MES_OK) break;
    if (result.is_heartbeat || result.data == nullptr) continue;

    size_t consumed = 0;
    // A decode failure cannot be recovered by waiting for more packets. Stop
    // immediately so an E2E assertion reports the triggering event rather
    // than spending the remaining poll budget on heartbeats.
    const mes_error_t rc = mes_feed(engine, result.data, result.size, &consumed);
    if (rc != MES_OK) {
      if (feed_error) *feed_error = rc;
      break;
    }

    const mes_event_t* event = nullptr;
    while (mes_next_event(engine, &event) == MES_OK) {
      events.push_back(CopyEvent(event));
    }

    if (done_pred(events)) break;
  }

  mes_client_stop(client);
  mes_client_disconnect(client);
  mes_client_destroy(client);
  if (!external_engine) mes_destroy(engine);

  return events;
}

// Convenience: capture events for a specific table, wait for N events
inline std::vector<CapturedEvent> CaptureTableEvents(const std::string& start_gtid,
                                                     uint32_t server_id,
                                                     const std::string& table_name, size_t count,
                                                     mes_engine_t* engine = nullptr,
                                                     mes_error_t* feed_error = nullptr) {
  return CaptureEvents(
      start_gtid, server_id,
      [&](const std::vector<CapturedEvent>& evts) {
        size_t n = 0;
        for (const auto& e : evts) {
          if (e.table == table_name) n++;
        }
        return n >= count;
      },
      200, engine, feed_error);
}

// Get MySQL/MariaDB major version from server
inline int GetMysqlMajorVersion() {
  mes::protocol::MysqlConnection conn;
  if (conn.Connect(kHost, kPort, kRootUser, kRootPass, kTimeout, kTimeout, DefaultSslMode(),
                   DefaultCa(), "", "") != MES_OK)
    return 0;
  mes::protocol::QueryResult qr;
  std::string err;
  if (mes::protocol::ExecuteQuery(conn.Socket(), "SELECT @@version", &qr, &err) != MES_OK) return 0;
  if (qr.rows.empty()) return 0;
  const std::string& ver = qr.rows[0].values[0];
  return std::atoi(ver.c_str());
}

inline bool IsMysql9OrLater() { return !IsMariaDB() && GetMysqlMajorVersion() >= 9; }

// Filter events by table name
inline std::vector<CapturedEvent> FilterByTable(const std::vector<CapturedEvent>& events,
                                                const std::string& table) {
  std::vector<CapturedEvent> filtered;
  for (const auto& e : events) {
    if (e.table == table) filtered.push_back(e);
  }
  return filtered;
}

/// @brief What a poll parked on an empty queue did when the client was stopped.
struct StopUnblockObservation {
  bool parked = false;          ///< The in-flight poll had not returned when stop was called.
  long long unblock_ms = -1;    ///< Stop to that poll's return, -1 if it never returned.
  int blocked_poll_error = -1;  ///< Error that poll reported, -1 if it never returned.
};

/// @brief Park a poller thread inside a blocking poll, then stop the client.
///
/// A single poll right after start proves nothing about stop: the stream's
/// startup burst (format description, rotate, previous GTIDs) is queued
/// unconditionally and satisfies it within milliseconds. So this polls in a loop
/// until the stream has been quiet for @p quiet_ms, which leaves the poller in a
/// poll it cannot leave on its own, and only then stops the client from this
/// thread. The caller asserts on all three fields: a stop that never unblocked
/// the poll, and a poll woken by a heartbeat or a late event rather than by the
/// stop, both have to be distinguishable from the real claim.
///
/// The client is left stopped and the poller joined; the caller still owns
/// disconnect and destroy.
inline StopUnblockObservation ObserveStopUnblocksPoll(mes_client_t* client,
                                                      long long quiet_ms = 500,
                                                      long long deadline_ms = 3000) {
  const auto now_ms = []() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
  };

  // False while the poller sits inside mes_client_poll(), true once that call
  // has returned.
  std::atomic<bool> poll_returned{false};
  std::atomic<int> polls_returned{0};
  std::atomic<long long> last_return_ms{now_ms()};
  std::atomic<bool> stop_requested{false};
  std::atomic<int> blocked_poll_error{-1};

  std::thread poller([&]() {
    for (;;) {
      const bool began_before_stop = !stop_requested.load(std::memory_order_acquire);
      poll_returned.store(false, std::memory_order_release);
      mes_poll_result_t result = mes_client_poll(client);
      poll_returned.store(true, std::memory_order_release);
      last_return_ms.store(now_ms(), std::memory_order_release);
      polls_returned.fetch_add(1, std::memory_order_acq_rel);
      if (began_before_stop && stop_requested.load(std::memory_order_acquire)) {
        blocked_poll_error.store(static_cast<int>(result.error), std::memory_order_release);
      }
      if (result.error != MES_OK) return;
    }
  });

  StopUnblockObservation obs;
  // The server's heartbeat period is seconds, so a quiet window of a few hundred
  // milliseconds sits between heartbeats.
  for (int i = 0; i < 400 && !obs.parked; i++) {  // up to ~4 s
    obs.parked = polls_returned.load(std::memory_order_acquire) > 0 &&
                 !poll_returned.load(std::memory_order_acquire) &&
                 now_ms() - last_return_ms.load(std::memory_order_acquire) >= quiet_ms;
    if (!obs.parked) std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  stop_requested.store(true, std::memory_order_release);
  const auto stop_called_at = std::chrono::steady_clock::now();
  mes_client_stop(client);

  for (long long waited = 0; waited < deadline_ms; waited += 5) {
    if (blocked_poll_error.load(std::memory_order_acquire) >= 0) {
      obs.unblock_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                           std::chrono::steady_clock::now() - stop_called_at)
                           .count();
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  obs.blocked_poll_error = blocked_poll_error.load(std::memory_order_acquire);

  poller.join();
  return obs;
}

/// @brief RAII helper that executes a cleanup SQL statement on destruction.
///
/// Ensures cleanup DML runs even when a test assertion fails mid-test.
/// Errors during cleanup are silently ignored.
class ScopedCleanup {
 public:
  ScopedCleanup(std::string sql) : sql_(std::move(sql)) {}
  ~ScopedCleanup() { ExecuteDML(sql_); }

  ScopedCleanup(const ScopedCleanup&) = delete;
  ScopedCleanup& operator=(const ScopedCleanup&) = delete;

 private:
  std::string sql_;
};

}  // namespace e2e

#endif  // MES_TEST_E2E_HELPERS_H_
