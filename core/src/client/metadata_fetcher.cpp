// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include "client/metadata_fetcher.h"

#include <algorithm>
#include <cstring>
#include <string>

#include "logger.h"
#include "protocol/mysql_query.h"

namespace mes {

MetadataFetcher::MetadataFetcher() = default;

MetadataFetcher::~MetadataFetcher() { Disconnect(); }

mes_error_t MetadataFetcher::Connect(const std::string& host, uint16_t port,
                                     const std::string& user, const std::string& password,
                                     uint32_t connect_timeout_s, uint32_t read_timeout_s,
                                     uint32_t ssl_mode, const std::string& ssl_ca,
                                     const std::string& ssl_cert, const std::string& ssl_key,
                                     bool allow_public_key_retrieval) {
  if (conn_.IsConnected()) {
    Disconnect();
  }

  // Store connection parameters for reconnection
  host_ = host;
  port_ = port;
  user_ = user;
  password_ = password;
  connect_timeout_s_ = connect_timeout_s;
  read_timeout_s_ = read_timeout_s;
  ssl_mode_ = ssl_mode;
  ssl_ca_ = ssl_ca;
  ssl_cert_ = ssl_cert;
  ssl_key_ = ssl_key;
  allow_public_key_retrieval_ = allow_public_key_retrieval;

  return conn_.Connect(host, port, user, password, connect_timeout_s, read_timeout_s, ssl_mode,
                       ssl_ca, ssl_cert, ssl_key, allow_public_key_retrieval);
}

void MetadataFetcher::Disconnect() {
  cache_.clear();
  conn_.Disconnect();

  // Scrub the retained plaintext password. It is held between Connect() and
  // Disconnect() so FetchColumnInfo() can reconnect once on connection loss
  // (the retry path uses conn_.Disconnect(), not this method, so it stays
  // available there). At final teardown, overwrite the backing bytes before
  // releasing them so the secret does not linger in freed heap memory.
  if (!password_.empty()) {
    std::fill(password_.begin(), password_.end(), '\0');
  }
  password_.clear();
  password_.shrink_to_fit();
}

std::vector<ColumnInfo> MetadataFetcher::FetchColumnInfo(const std::string& database,
                                                         const std::string& table,
                                                         size_t expected_count) {
  if (!conn_.IsConnected()) {
    return {};
  }

  // Check cache
  auto db_it = cache_.find(database);
  if (db_it != cache_.end()) {
    auto table_it = db_it->second.find(table);
    if (table_it != db_it->second.end() && table_it->second.size() == expected_count) {
      return table_it->second;
    }
  }

  // Cache miss or count mismatch -- query MySQL.
  // EscapeIdentifier returns "" when it detects an invalid identifier
  // (e.g. embedded NUL byte). Reject the fetch early instead of emitting a
  // malformed `SHOW COLUMNS FROM .` query. In practice MySQL never sends
  // such identifiers, so this is a defensive guard only.
  const std::string db_esc = EscapeIdentifier(database);
  const std::string tbl_esc = EscapeIdentifier(table);
  if (db_esc.empty() || tbl_esc.empty()) {
    StructuredLog()
        .Event("metadata_fetch_invalid_identifier")
        .Field("db", database)
        .Field("table", table)
        .Warn();
    return {};
  }
  std::string query = "SHOW COLUMNS FROM " + db_esc + "." + tbl_esc;

  // Try query, retry once on connection loss
  protocol::QueryResult qr;
  std::string err;
  bool query_ok = false;
  for (int attempt = 0; attempt < 2; attempt++) {
    if (attempt == 1) {
      // Reconnect and retry
      conn_.Disconnect();
      if (conn_.Connect(host_, port_, user_, password_, connect_timeout_s_, read_timeout_s_,
                        ssl_mode_, ssl_ca_, ssl_cert_, ssl_key_,
                        allow_public_key_retrieval_) != MES_OK) {
        break;
      }
    }
    if (protocol::ExecuteQuery(conn_.Socket(), query, &qr, &err, conn_.DeprecateEofNegotiated()) ==
        MES_OK) {
      query_ok = true;
      break;
    }
  }
  if (!query_ok) {
    if (db_it != cache_.end()) db_it->second.erase(table);
    StructuredLog()
        .Event("metadata_fetch_failed")
        .Field("db", database)
        .Field("table", table)
        .Field("error", err)
        .Warn();
    return {};
  }

  std::vector<ColumnInfo> infos;
  for (const auto& row : qr.rows) {
    if (row.values.size() < 2 || row.is_null.size() < 2) {
      continue;
    }
    ColumnInfo info;
    // row.values[0] = Field (column name)
    info.name = (!row.is_null[0]) ? row.values[0] : "";

    // row.values[1] = Type (e.g. "int unsigned", "varchar(255)")
    if (!row.is_null[1]) {
      std::string type_str = row.values[1];
      info.is_unsigned = type_str.find("unsigned") != std::string::npos;
    }

    infos.push_back(std::move(info));
  }

  // Verify column count matches expectation
  if (infos.size() != expected_count) {
    if (db_it != cache_.end()) db_it->second.erase(table);
    StructuredLog()
        .Event("metadata_fetch_column_count_mismatch")
        .Field("db", database)
        .Field("table", table)
        .Field("expected", static_cast<uint64_t>(expected_count))
        .Field("got", static_cast<uint64_t>(infos.size()))
        .Warn();
    return {};
  }

  cache_[database][table] = infos;
  return infos;
}

void MetadataFetcher::InvalidateCache(const std::string& database, const std::string& table) {
  auto db_it = cache_.find(database);
  if (db_it == cache_.end()) return;
  db_it->second.erase(table);
  if (db_it->second.empty()) cache_.erase(db_it);
}

void MetadataFetcher::ClearCache() { cache_.clear(); }

std::string MetadataFetcher::EscapeIdentifier(const std::string& id) {
  // Escape backticks within the identifier by doubling them.
  // Null bytes are skipped to prevent query truncation.
  std::string escaped = "`";
  for (char c : id) {
    if (c == '\0') {
      // Null bytes in identifiers are not valid; reject instead of silently modifying
      return {};
    }
    if (c == '`') {
      escaped += "``";
    } else {
      escaped += c;
    }
  }
  escaped += '`';
  return escaped;
}

}  // namespace mes
