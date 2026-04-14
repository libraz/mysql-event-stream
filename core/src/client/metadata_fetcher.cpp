// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include "client/metadata_fetcher.h"

#include <cstring>
#include <string>

#include "protocol/mysql_query.h"

namespace mes {

MetadataFetcher::MetadataFetcher() = default;

MetadataFetcher::~MetadataFetcher() { Disconnect(); }

mes_error_t MetadataFetcher::Connect(const std::string& host, uint16_t port,
                                     const std::string& user, const std::string& password,
                                     uint32_t connect_timeout_s, uint32_t ssl_mode,
                                     const std::string& ssl_ca, const std::string& ssl_cert,
                                     const std::string& ssl_key) {
  if (conn_.IsConnected()) {
    Disconnect();
  }

  // Store connection parameters for reconnection
  host_ = host;
  port_ = port;
  user_ = user;
  password_ = password;
  connect_timeout_s_ = connect_timeout_s;
  ssl_mode_ = ssl_mode;
  ssl_ca_ = ssl_ca;
  ssl_cert_ = ssl_cert;
  ssl_key_ = ssl_key;

  return conn_.Connect(host, port, user, password, connect_timeout_s,
                       0 /* read_timeout */, ssl_mode, ssl_ca, ssl_cert, ssl_key);
}

void MetadataFetcher::Disconnect() {
  cache_.clear();
  conn_.Disconnect();
}

std::vector<ColumnInfo> MetadataFetcher::FetchColumnInfo(const std::string& database,
                                                         const std::string& table,
                                                         size_t expected_count) {
  if (!conn_.IsConnected()) {
    return {};
  }

  std::string key = MakeCacheKey(database, table);

  // Check cache
  auto it = cache_.find(key);
  if (it != cache_.end() && it->second.size() == expected_count) {
    return it->second;
  }

  // Cache miss or count mismatch -- query MySQL
  std::string query =
      "SHOW COLUMNS FROM " + EscapeIdentifier(database) + "." + EscapeIdentifier(table);

  // Try query, retry once on connection loss
  protocol::QueryResult qr;
  std::string err;
  for (int attempt = 0; attempt < 2; attempt++) {
    if (protocol::ExecuteQuery(conn_.Socket(), query, &qr, &err) == MES_OK) {
      break;
    }
    if (attempt == 0) {
      // Reconnect and retry
      conn_.Disconnect();
      if (conn_.Connect(host_, port_, user_, password_, connect_timeout_s_,
                        0 /* read_timeout */, ssl_mode_, ssl_ca_, ssl_cert_,
                        ssl_key_) == MES_OK) {
        continue;
      }
    }
    // Query failed
    cache_.erase(key);
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
    cache_.erase(key);
    return {};
  }

  cache_[key] = infos;
  return infos;
}

void MetadataFetcher::InvalidateCache(const std::string& database, const std::string& table) {
  cache_.erase(MakeCacheKey(database, table));
}

std::string MetadataFetcher::MakeCacheKey(const std::string& db, const std::string& table) {
  return db + "." + table;
}

std::string MetadataFetcher::EscapeIdentifier(const std::string& id) {
  // Escape backticks within the identifier by doubling them
  std::string escaped = "`";
  for (char c : id) {
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
