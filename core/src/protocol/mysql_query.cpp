// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include "protocol/mysql_query.h"

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "protocol/mysql_packet.h"
#include "protocol/mysql_socket.h"

namespace mes::protocol {

namespace {

// Result sets are retained in memory because callers need random access to
// the completed response. Bound both dimensions so a malicious or broken
// server cannot make the connection caller allocate indefinitely. The byte
// budget covers everything QueryResult retains: column names as well as row
// values.
constexpr size_t kMaxResultRows = 100000;
constexpr size_t kMaxResultBytes = 64u * 1024u * 1024u;

// A column definition packet carries six length-encoded identifiers plus a
// fixed tail, so it cannot legitimately approach the 64 MiB packet default.
// Capping it here bounds a single allocation independently of the cumulative
// byte budget below.
constexpr size_t kMaxColumnDefBytes = 64u * 1024u;

// A zero-length payload never appears in a well-formed result set: packet
// reassembly consumes the terminating empty packet of a multi-packet sequence
// internally. Tolerate a handful so a quirky server is not fatal, but bound
// them so a server emitting nothing but empty packets cannot spin the row loop
// forever while keeping the read timeout from firing.
constexpr size_t kMaxEmptyRowPackets = 4;

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

/**
 * @brief Parse a result set row into values and null flags
 *
 * Returns false if the row payload is truncated (fewer columns than declared,
 * or a value whose declared length runs past the payload end). Truncation must
 * be reported as a parse failure rather than fabricating NULLs, otherwise a
 * corrupt or partially-decoded payload would silently surface as missing data.
 */
}  // namespace

bool ParseTextResultRow(const std::vector<uint8_t>& payload, size_t column_count,
                        QueryResultRow* row) {
  const uint8_t* data = payload.data();
  size_t data_size = payload.size();
  size_t pos = 0;

  row->values.resize(column_count);
  row->is_null.resize(column_count);

  for (size_t i = 0; i < column_count; ++i) {
    if (pos >= data_size) {
      // Fewer column values present than the result set declared.
      return false;
    }
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
      if (str_len > static_cast<uint64_t>(data_size - pos)) {
        // Declared length runs past the payload end: treat as truncation.
        return false;
      }
      row->values[i].assign(reinterpret_cast<const char*>(data + pos),
                            static_cast<size_t>(str_len));
      pos += static_cast<size_t>(str_len);
    }
  }
  return true;
}

mes_error_t ExecuteQuery(SocketHandle* sock, const std::string& query, QueryResult* result,
                         std::string* error_msg, bool deprecate_eof) {
  // A malformed or partially consumed result set leaves packet boundaries
  // ambiguous. Do not allow the next COM_QUERY to consume its remaining
  // packets; force callers to reconnect instead.
  const auto fail_after_response = [sock](mes_error_t error) {
    sock->Poison();
    return error;
  };

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
    return fail_after_response(rc);
  }

  // Read first response packet
  std::vector<uint8_t> payload;
  const uint8_t expected_seq_id = seq_id;
  rc = ReadPacket(sock, &payload, &seq_id);
  if (rc != MES_OK) {
    *error_msg = "Failed to read query response";
    return fail_after_response(rc);
  }

  // A response opens on the sequence id that follows the request's own, so a
  // packet on any other sequence was not produced for this command. The case
  // that matters is a replication packet left behind by an aborted binlog
  // dump: it opens with the same 0x00 marker as a command-phase OK packet and
  // would otherwise be accepted as an empty result set. Only the first
  // response packet can be checked this way, because ReadPacket() reports the
  // sequence id of the last header it consumed and a reassembled multi-packet
  // payload therefore ends on a later one.
  if (seq_id != expected_seq_id) {
    *error_msg = "Query response arrived out of sequence";
    return fail_after_response(MES_ERR_STREAM);
  }

  if (payload.empty()) {
    *error_msg = "Empty response from server";
    return fail_after_response(MES_ERR_STREAM);
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
  //
  // This bound is deliberately independent of binary::kMaxTableColumns: that
  // one caps the columns a binlog table may declare, this one caps the columns
  // a COM_QUERY result set may return. They coincide only because both derive
  // from MySQL's field limit, so sharing a constant would make raising either
  // ceiling silently raise the other.
  static constexpr uint64_t kMaxColumnCount = 4096;
  size_t pos = 0;
  uint64_t column_count = ReadLenEncInt(payload.data(), payload.size(), &pos);
  if (column_count > kMaxColumnCount) {
    *error_msg = "Column count exceeds maximum (" + std::to_string(column_count) + ")";
    return fail_after_response(MES_ERR_STREAM);
  }
  // A result set always declares at least one column; the no-result-set case is
  // the OK packet handled above. Zero here means ReadLenEncInt could not decode
  // the prefix -- a LOCAL INFILE request (0xFB) or a truncated multi-byte
  // length -- and both return 0. Accepting it would declare an empty result set
  // and then read every following packet as a zero-column row.
  if (column_count == 0) {
    *error_msg = "Malformed result set header (zero column count)";
    return fail_after_response(MES_ERR_STREAM);
  }

  // Read column definition packets. Retained column names share the byte
  // budget with row values: the caller holds both until the QueryResult dies.
  size_t result_bytes = 0;
  result->column_names.resize(column_count);
  for (uint64_t i = 0; i < column_count; ++i) {
    rc = ReadPacket(sock, &payload, &seq_id, kMaxColumnDefBytes);
    if (rc != MES_OK) {
      *error_msg = "Failed to read column definition";
      return fail_after_response(rc);
    }
    // The server may abandon the result set it has already announced. An ERR
    // packet here carries the diagnostic the caller needs, and its 0xFF marker
    // decodes as a length-encoded string just as readily as a real definition
    // would, so the marker has to be inspected before the payload is parsed.
    if (!payload.empty() && payload[0] == kPacketErr) {
      ParseErrPacket(payload, error_msg);
      return MES_ERR_VALIDATION;
    }
    // Anything else that is not a column definition leaves the remaining
    // definitions unaccounted for, so packet boundaries can no longer be
    // matched to result set positions. A definition opens with the length of
    // its catalog identifier, never with 0xFE and never empty; the read above
    // is capped well below the maximum payload length, so 0xFE here cannot be
    // the leading byte of a long length-encoded string either.
    if (payload.empty() || payload[0] == kPacketEOF) {
      *error_msg = "Result set ended before all column definitions were sent";
      return fail_after_response(MES_ERR_STREAM);
    }
    std::string column_name = ParseColumnName(payload);
    // Subtraction form: result_bytes never exceeds kMaxResultBytes, so the
    // right-hand side cannot underflow.
    if (column_name.size() > kMaxResultBytes - result_bytes) {
      *error_msg = "Result set byte size exceeds maximum (" + std::to_string(kMaxResultBytes) + ")";
      return fail_after_response(MES_ERR_QUEUE_FULL);
    }
    result_bytes += column_name.size();
    result->column_names[i] = std::move(column_name);
  }

  // Without CLIENT_DEPRECATE_EOF, read intermediate EOF packet after column defs
  if (!deprecate_eof) {
    rc = ReadPacket(sock, &payload, &seq_id);
    if (rc != MES_OK) {
      *error_msg = "Failed to read intermediate EOF packet";
      return fail_after_response(rc);
    }
    // The server can replace the EOF that opens the row section with an ERR,
    // which reports why the announced rows are not coming.
    if (!payload.empty() && payload[0] == kPacketErr) {
      ParseErrPacket(payload, error_msg);
      return MES_ERR_VALIDATION;
    }
    // Verify it's actually an EOF packet (0xFE with < 9 bytes)
    if (payload.empty() || payload[0] != kPacketEOF || payload.size() >= 9) {
      *error_msg = "Expected intermediate EOF packet";
      return fail_after_response(MES_ERR_STREAM);
    }
  }

  // Read row data packets until end-of-rows marker.
  result->rows.clear();
  size_t empty_packets = 0;
  for (;;) {
    rc = ReadPacket(sock, &payload, &seq_id);
    if (rc != MES_OK) {
      *error_msg = "Failed to read row data";
      return fail_after_response(rc);
    }

    if (payload.empty()) {
      if (++empty_packets > kMaxEmptyRowPackets) {
        *error_msg = "Server sent only empty packets while reading result-set rows";
        return fail_after_response(MES_ERR_STREAM);
      }
      continue;
    }

    // End-of-rows detection depends on CLIENT_DEPRECATE_EOF negotiation
    if (payload[0] == kPacketEOF) {
      if (deprecate_eof) {
        // A text row can begin with 0xFE only when its first length-encoded
        // field is at least 16 MiB. MySQL reserves 0xFE packets below the
        // maximum packet payload length for the OK-replacing-EOF marker.
        if (payload.size() < 0xFFFFFFu) break;
      } else {
        // Traditional EOF: 0xFE with < 9 bytes (typically 5)
        if (payload.size() < 9) break;
      }
    }

    // ERR packet during row reading
    if (payload[0] == kPacketErr) {
      ParseErrPacket(payload, error_msg);
      return MES_ERR_VALIDATION;
    }

    // Parse row data. A truncated row (insufficient bytes for the declared
    // column count/length) is a parse failure, not silently-missing data.
    QueryResultRow row;
    if (!ParseTextResultRow(payload, static_cast<size_t>(column_count), &row)) {
      *error_msg = "Truncated result-set row";
      return fail_after_response(MES_ERR_STREAM);
    }
    if (result->rows.size() == kMaxResultRows) {
      *error_msg = "Result set row count exceeds maximum (" + std::to_string(kMaxResultRows) + ")";
      return fail_after_response(MES_ERR_QUEUE_FULL);
    }
    for (const auto& value : row.values) {
      if (value.size() > kMaxResultBytes - result_bytes) {
        *error_msg =
            "Result set byte size exceeds maximum (" + std::to_string(kMaxResultBytes) + ")";
        return fail_after_response(MES_ERR_QUEUE_FULL);
      }
      result_bytes += value.size();
    }
    result->rows.push_back(std::move(row));
  }

  return MES_OK;
}

}  // namespace mes::protocol
