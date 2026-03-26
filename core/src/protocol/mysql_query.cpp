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
  *pos += str_len;
}

/** @brief Read a length-encoded string at the current position */
std::string ReadLenEncString(const uint8_t* data, size_t len, size_t* pos) {
  uint64_t str_len = ReadLenEncInt(data, len, pos);
  if (*pos + str_len > len) {
    *pos = len;
    return {};
  }
  std::string result(reinterpret_cast<const char*>(data + *pos), str_len);
  *pos += str_len;
  return result;
}

/** @brief Extract error message from an ERR packet payload */
void ParseErrPacket(const std::vector<uint8_t>& payload,
                    std::string* error_msg) {
  // ERR packet: [0xFF] [error_code: 2] [sql_state_marker: 1] [sql_state: 5]
  // [message: rest]
  if (payload.size() < 4) {
    *error_msg = "Unknown MySQL error";
    return;
  }
  // Skip 0xFF marker byte, read error code
  size_t pos = 1;
  uint16_t error_code =
      static_cast<uint16_t>(ReadFixedInt(payload.data() + pos, 2));
  pos += 2;

  // Check for SQL state marker '#'
  if (pos < payload.size() && payload[pos] == '#') {
    pos += 1;  // '#'
    pos += 5;  // SQL state (5 bytes)
  }

  std::string msg;
  if (pos < payload.size()) {
    msg.assign(reinterpret_cast<const char*>(payload.data() + pos),
               payload.size() - pos);
  }
  *error_msg =
      "MySQL error " + std::to_string(error_code) + ": " + msg;
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
void ParseRowData(const std::vector<uint8_t>& payload, size_t column_count,
                  QueryResultRow* row) {
  const uint8_t* data = payload.data();
  size_t data_size = payload.size();
  size_t pos = 0;

  row->values.resize(column_count);
  row->is_null.resize(column_count);

  for (size_t i = 0; i < column_count && pos < data_size; ++i) {
    if (data[pos] == 0xFB) {
      // NULL value
      row->is_null[i] = true;
      row->values[i].clear();
      pos += 1;
    } else {
      row->is_null[i] = false;
      uint64_t str_len = ReadLenEncInt(data, data_size, &pos);
      if (pos + str_len <= data_size) {
        row->values[i].assign(reinterpret_cast<const char*>(data + pos),
                              str_len);
      }
      pos += str_len;
    }
  }
}

}  // namespace

mes_error_t ExecuteQuery(SocketHandle* sock, const std::string& query,
                         QueryResult* result, std::string* error_msg) {
  // Build COM_QUERY payload: [0x03] + query bytes
  std::vector<uint8_t> cmd_payload;
  cmd_payload.reserve(1 + query.size());
  cmd_payload.push_back(0x03);  // COM_QUERY
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
  if (first_byte == 0x00) {
    *result = QueryResult{};
    return MES_OK;
  }

  // ERR packet (MySQL server rejected the query)
  if (first_byte == 0xFF) {
    ParseErrPacket(payload, error_msg);
    return MES_ERR_VALIDATION;
  }

  // Result set: first packet contains column_count as len-enc-int
  size_t pos = 0;
  uint64_t column_count = ReadLenEncInt(payload.data(), payload.size(), &pos);

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

  // With CLIENT_DEPRECATE_EOF, no intermediate EOF packet.
  // Read row data packets until OK packet (0xFE with >= 7 bytes).
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

    // OK packet replacing EOF: starts with 0xFE and has >= 7 bytes
    if (payload[0] == 0xFE && payload.size() >= 7) {
      break;
    }

    // ERR packet during row reading
    if (payload[0] == 0xFF) {
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
