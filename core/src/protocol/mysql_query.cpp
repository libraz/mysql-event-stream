// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include "protocol/mysql_query.h"

#include <cstdint>
#include <string>
#include <vector>

#include "protocol/mysql_packet.h"
#include "protocol/mysql_socket.h"

namespace mes::protocol {

namespace {

/** @brief Skip a length-encoded string at the current position */
void SkipLenEncString(const uint8_t* data, size_t len, size_t* pos) {
  uint64_t str_len = ReadLenEncInt(data, len, pos);
  // Subtraction-form bounds check: avoids overflow when str_len is huge
  // (up to 2^64) and size_t is 32-bit (WASM). `*pos <= len` is guaranteed
  // by ReadLenEncInt's own bounds check.
  if (str_len > static_cast<uint64_t>(len - *pos)) {
    *pos = len;
    return;
  }
  *pos += static_cast<size_t>(str_len);
}

/** @brief Read a length-encoded string at the current position */
std::string ReadLenEncString(const uint8_t* data, size_t len, size_t* pos) {
  uint64_t str_len = ReadLenEncInt(data, len, pos);
  if (str_len > static_cast<uint64_t>(len - *pos)) {
    *pos = len;
    return {};
  }
  std::string result(reinterpret_cast<const char*>(data + *pos), static_cast<size_t>(str_len));
  *pos += static_cast<size_t>(str_len);
  return result;
}

/** @brief Extract error message from an ERR packet payload */
void ParseErrPacket(const std::vector<uint8_t>& payload, std::string* error_msg) {
  uint16_t error_code = 0;
  std::string msg;
  ParseErrPacketPayload(payload.data(), payload.size(), &error_code, &msg);
  *error_msg = "MySQL error " + std::to_string(error_code) + ": " + msg;
}

/** @brief Parse column name from a column definition packet */
std::string ParseColumnName(const std::vector<uint8_t>& payload) {
  const uint8_t* data = payload.data();
  size_t len = payload.size();
  size_t pos = 0;

  // Column definition packet fields (all length-encoded strings):
  // catalog, schema, table, org_table, name, org_name, ...
  SkipLenEncString(data, len, &pos);  // catalog
  SkipLenEncString(data, len, &pos);  // schema
  SkipLenEncString(data, len, &pos);  // table
  SkipLenEncString(data, len, &pos);  // org_table

  return ReadLenEncString(data, len, &pos);  // name
}

/** @brief Parse a result set row into values and null flags */
void ParseRowData(const std::vector<uint8_t>& payload, size_t column_count, QueryResultRow* row) {
  const uint8_t* data = payload.data();
  size_t data_size = payload.size();
  size_t pos = 0;

  row->values.resize(column_count);
  row->is_null.resize(column_count);

  size_t i = 0;
  for (; i < column_count && pos < data_size; ++i) {
    if (data[pos] == kNullColumnMarker) {
      // NULL value: text-protocol row uses a dedicated 0xFB marker byte
      // *before* ReadLenEncInt is consulted. ReadLenEncInt's built-in
      // "return 0 on 0xFB" behavior is therefore not relied on here; see
      // the defensive comment in ReadLenEncInt for details.
      row->is_null[i] = true;
      row->values[i].clear();
      pos += 1;
    } else {
      row->is_null[i] = false;
      uint64_t str_len = ReadLenEncInt(data, data_size, &pos);
      // Subtraction-form bounds check to prevent size_t overflow when
      // str_len is attacker-controlled and size_t is 32-bit (WASM).
      // ReadLenEncInt guarantees pos <= data_size on return.
      if (str_len <= static_cast<uint64_t>(data_size - pos)) {
        row->values[i].assign(reinterpret_cast<const char*>(data + pos),
                              static_cast<size_t>(str_len));
        pos += static_cast<size_t>(str_len);
      } else {
        // Truncated row: stop parsing further columns.
        pos = data_size;
      }
    }
  }
  // Mark remaining columns as NULL if the row data was truncated
  for (; i < column_count; ++i) {
    row->is_null[i] = true;
    row->values[i].clear();
  }
}

}  // namespace

mes_error_t ExecuteQuery(SocketHandle* sock, const std::string& query, QueryResult* result,
                         std::string* error_msg, bool deprecate_eof) {
  // Build COM_QUERY payload: command byte + query bytes
  std::vector<uint8_t> cmd_payload;
  cmd_payload.reserve(1 + query.size());
  cmd_payload.push_back(kComQuery);
  cmd_payload.insert(cmd_payload.end(), query.begin(), query.end());

  // Send with sequence_id = 0
  PacketBuffer pkt_buf;
  uint8_t seq_id = 0;
  pkt_buf.WritePacket(cmd_payload.data(), cmd_payload.size(), &seq_id);

  mes_error_t rc = sock->WriteAll(pkt_buf.Data(), pkt_buf.Size());
  if (rc != MES_OK) {
    *error_msg = "Failed to send COM_QUERY packet";
    return rc;
  }

  // Read first response packet
  std::vector<uint8_t> payload;
  rc = ReadPacket(sock, &payload, &seq_id);
  if (rc != MES_OK) {
    *error_msg = "Failed to read query response";
    return rc;
  }

  if (payload.empty()) {
    *error_msg = "Empty response from server";
    return MES_ERR_STREAM;
  }

  uint8_t first_byte = payload[0];

  // OK packet (no result set) - for SET, USE, INSERT, etc.
  if (first_byte == kPacketOk) {
    *result = QueryResult{};
    return MES_OK;
  }

  // ERR packet (MySQL server rejected the query)
  if (first_byte == kPacketErr) {
    ParseErrPacket(payload, error_msg);
    return MES_ERR_VALIDATION;
  }

  // Result set: first packet contains column_count as len-enc-int
  static constexpr uint64_t kMaxColumnCount = 4096;
  size_t pos = 0;
  uint64_t column_count = ReadLenEncInt(payload.data(), payload.size(), &pos);
  if (column_count > kMaxColumnCount) {
    *error_msg = "Column count exceeds maximum (" + std::to_string(column_count) + ")";
    return MES_ERR_PARSE;
  }

  // Read column definition packets
  result->column_names.resize(column_count);
  for (uint64_t i = 0; i < column_count; ++i) {
    rc = ReadPacket(sock, &payload, &seq_id);
    if (rc != MES_OK) {
      *error_msg = "Failed to read column definition";
      return rc;
    }
    result->column_names[i] = ParseColumnName(payload);
  }

  // Without CLIENT_DEPRECATE_EOF, read intermediate EOF packet after column defs
  if (!deprecate_eof) {
    rc = ReadPacket(sock, &payload, &seq_id);
    if (rc != MES_OK) {
      *error_msg = "Failed to read intermediate EOF packet";
      return rc;
    }
    // Verify it's actually an EOF packet (0xFE with < 9 bytes)
    if (payload.empty() || payload[0] != kPacketEOF || payload.size() >= 9) {
      *error_msg = "Expected intermediate EOF packet";
      return MES_ERR_STREAM;
    }
  }

  // Read row data packets until end-of-rows marker.
  result->rows.clear();
  for (;;) {
    rc = ReadPacket(sock, &payload, &seq_id);
    if (rc != MES_OK) {
      *error_msg = "Failed to read row data";
      return rc;
    }

    if (payload.empty()) {
      continue;
    }

    // End-of-rows detection depends on CLIENT_DEPRECATE_EOF negotiation
    if (payload[0] == kPacketEOF) {
      if (deprecate_eof) {
        // OK-replacing-EOF: 0xFE with >= 7 bytes
        if (payload.size() >= 7) break;
      } else {
        // Traditional EOF: 0xFE with < 9 bytes (typically 5)
        if (payload.size() < 9) break;
      }
    }

    // ERR packet during row reading
    if (payload[0] == kPacketErr) {
      ParseErrPacket(payload, error_msg);
      return MES_ERR_STREAM;
    }

    // Parse row data
    QueryResultRow row;
    ParseRowData(payload, static_cast<size_t>(column_count), &row);
    result->rows.push_back(std::move(row));
  }

  return MES_OK;
}

}  // namespace mes::protocol
