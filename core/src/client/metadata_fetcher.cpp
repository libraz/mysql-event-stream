// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include "client/metadata_fetcher.h"

#ifdef MES_HAS_MYSQL

#include <cstring>
#include <string>

namespace mes {

MetadataFetcher::MetadataFetcher() = default;

MetadataFetcher::~MetadataFetcher() { Disconnect(); }

mes_error_t MetadataFetcher::Connect(const std::string& host, uint16_t port,
                                     const std::string& user, const std::string& password,
                                     uint32_t connect_timeout_s, uint32_t ssl_mode,
                                     const std::string& ssl_ca, const std::string& ssl_cert,
                                     const std::string& ssl_key) {
  if (conn_ != nullptr) {
    Disconnect();
  }

  conn_ = mysql_init(nullptr);
  if (conn_ == nullptr) {
    return MES_ERR_CONNECT;
  }

  mysql_options(conn_, MYSQL_OPT_CONNECT_TIMEOUT, &connect_timeout_s);

  // Apply SSL/TLS options
  if (ssl_mode > 0) {
    unsigned int ssl_mode_val;
    switch (ssl_mode) {
      case 1:
        ssl_mode_val = SSL_MODE_PREFERRED;
        break;
      case 2:
        ssl_mode_val = SSL_MODE_REQUIRED;
        break;
      case 3:
        ssl_mode_val = SSL_MODE_VERIFY_CA;
        break;
      case 4:
        ssl_mode_val = SSL_MODE_VERIFY_IDENTITY;
        break;
      default:
        ssl_mode_val = SSL_MODE_PREFERRED;
        break;
    }
    mysql_options(conn_, MYSQL_OPT_SSL_MODE, &ssl_mode_val);
  } else {
    // Explicitly disable SSL to avoid MySQL's default SSL_MODE_PREFERRED
    unsigned int ssl_mode_val = SSL_MODE_DISABLED;
    mysql_options(conn_, MYSQL_OPT_SSL_MODE, &ssl_mode_val);
  }
  if (!ssl_ca.empty()) {
    mysql_options(conn_, MYSQL_OPT_SSL_CA, ssl_ca.c_str());
  }
  if (!ssl_cert.empty()) {
    mysql_options(conn_, MYSQL_OPT_SSL_CERT, ssl_cert.c_str());
  }
  if (!ssl_key.empty()) {
    mysql_options(conn_, MYSQL_OPT_SSL_KEY, ssl_key.c_str());
  }

  if (mysql_real_connect(conn_, host.c_str(), user.c_str(), password.c_str(), nullptr, port,
                         nullptr, 0) == nullptr) {
    mysql_close(conn_);
    conn_ = nullptr;
    return MES_ERR_CONNECT;
  }

  return MES_OK;
}

void MetadataFetcher::Disconnect() {
  cache_.clear();
  if (conn_ != nullptr) {
    mysql_close(conn_);
    conn_ = nullptr;
  }
}

std::vector<ColumnInfo> MetadataFetcher::FetchColumnInfo(const std::string& database,
                                                         const std::string& table,
                                                         size_t expected_count) {
  if (conn_ == nullptr) {
    return {};
  }

  std::string key = MakeCacheKey(database, table);

  // Check cache
  auto it = cache_.find(key);
  if (it != cache_.end() && it->second.size() == expected_count) {
    return it->second;
  }

  // Cache miss or count mismatch — query MySQL
  std::string query =
      "SHOW COLUMNS FROM " + EscapeIdentifier(database) + "." + EscapeIdentifier(table);

  // Try query, retry once on connection loss
  for (int attempt = 0; attempt < 2; attempt++) {
    if (mysql_query(conn_, query.c_str()) == 0) {
      break;
    }
    if (attempt == 0 && mysql_ping(conn_) == 0) {
      continue;  // Reconnected, retry
    }
    // Query failed
    cache_.erase(key);
    return {};
  }

  MYSQL_RES* result = mysql_store_result(conn_);
  if (result == nullptr) {
    cache_.erase(key);
    return {};
  }

  std::vector<ColumnInfo> infos;
  MYSQL_ROW row;
  while ((row = mysql_fetch_row(result)) != nullptr) {
    ColumnInfo info;
    // row[0] = Field (column name)
    info.name = row[0] != nullptr ? row[0] : "";

    // row[1] = Type (e.g. "int unsigned", "varchar(255)")
    if (row[1] != nullptr) {
      std::string type_str = row[1];
      info.is_unsigned = type_str.find("unsigned") != std::string::npos;
    }

    infos.push_back(std::move(info));
  }
  mysql_free_result(result);

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

#endif  // MES_HAS_MYSQL
