// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @file test_e2e_helpers.h
 * @brief Shared helpers for E2E protocol tests
 */

#ifndef MES_TEST_E2E_HELPERS_H_
#define MES_TEST_E2E_HELPERS_H_

#include <cstring>
#include <functional>
#include <string>
#include <vector>

#include "mes.h"
#include "protocol/mysql_connection.h"
#include "protocol/mysql_query.h"

namespace e2e {

// Connection defaults
constexpr const char* kHost = "127.0.0.1";
constexpr uint16_t kPort = 13307;
constexpr const char* kRootUser = "root";
constexpr const char* kRootPass = "test_root_password";
constexpr const char* kReplUser = "repl_user";
constexpr const char* kReplPass = "test_password";
constexpr uint32_t kTimeout = 5;

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
  for (uint32_t i = 0; i < e->before_count; i++) {
    ce.before.push_back(CopyColumn(e->before_columns[i]));
  }
  for (uint32_t i = 0; i < e->after_count; i++) {
    ce.after.push_back(CopyColumn(e->after_columns[i]));
  }
  return ce;
}

// Get current GTID executed from server
inline std::string GetCurrentGtid() {
  mes::protocol::MysqlConnection conn;
  if (conn.Connect(kHost, kPort, kRootUser, kRootPass, kTimeout, kTimeout, 0,
                   "", "", "") != MES_OK)
    return "";
  mes::protocol::QueryResult qr;
  std::string err;
  if (mes::protocol::ExecuteQuery(conn.Socket(),
                                  "SELECT @@GLOBAL.gtid_executed", &qr,
                                  &err) != MES_OK)
    return "";
  if (qr.rows.empty()) return "";
  return qr.rows[0].values[0];
}

// Execute a DML/DDL statement as root
inline mes_error_t ExecuteDML(const std::string& sql) {
  mes::protocol::MysqlConnection conn;
  auto rc = conn.Connect(kHost, kPort, kRootUser, kRootPass, kTimeout, kTimeout,
                         0, "", "", "");
  if (rc != MES_OK) return rc;
  mes::protocol::QueryResult qr;
  std::string err;
  return mes::protocol::ExecuteQuery(conn.Socket(), sql, &qr, &err);
}

// Capture events from binlog stream using a predicate.
// Returns captured events. Stops when predicate returns true or max_polls reached.
using EventPredicate = std::function<bool(const std::vector<CapturedEvent>&)>;

inline std::vector<CapturedEvent> CaptureEvents(
    const std::string& start_gtid, uint32_t server_id,
    const EventPredicate& done_pred, int max_polls = 100,
    mes_engine_t* external_engine = nullptr) {
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
  config.ssl_mode = MES_SSL_DISABLED;
  config.ssl_ca = nullptr;
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
    mes_feed(engine, result.data, result.size, &consumed);

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
inline std::vector<CapturedEvent> CaptureTableEvents(
    const std::string& start_gtid, uint32_t server_id,
    const std::string& table_name, size_t count,
    mes_engine_t* engine = nullptr) {
  return CaptureEvents(
      start_gtid, server_id,
      [&](const std::vector<CapturedEvent>& evts) {
        size_t n = 0;
        for (const auto& e : evts) {
          if (e.table == table_name) n++;
        }
        return n >= count;
      },
      200, engine);
}

// Filter events by table name
inline std::vector<CapturedEvent> FilterByTable(
    const std::vector<CapturedEvent>& events, const std::string& table) {
  std::vector<CapturedEvent> filtered;
  for (const auto& e : events) {
    if (e.table == table) filtered.push_back(e);
  }
  return filtered;
}

}  // namespace e2e

#endif  // MES_TEST_E2E_HELPERS_H_
