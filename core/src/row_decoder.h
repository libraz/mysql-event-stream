// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @file row_decoder.h
 * @brief Row event decoding for WRITE/UPDATE/DELETE row events
 *
 * Decodes MySQL binlog ROWS_EVENT bodies into structured row data
 * using table metadata from TABLE_MAP events.
 */

#ifndef MES_ROW_DECODER_H_
#define MES_ROW_DECODER_H_

#include <cstddef>
#include <cstdint>
#include <vector>

#include "types.h"

namespace mes {

/// Result of decoding an UPDATE row event - before and after images.
struct UpdatePair {
  RowData before;
  RowData after;
};

/**
 * @brief Upper bound on the expansion of a single MariaDB compressed column.
 *
 * Column values are materialized in memory, so expansion is bounded
 * independently of the compact on-wire field.
 */
constexpr size_t kMaxDecompressedColumnBytes = 64U * 1024U * 1024U;

/**
 * @brief Heap budget for the decoded column payloads of one row event.
 *
 * @ref kMaxDecompressedColumnBytes caps a single compressed field but says
 * nothing about how many such fields an event may carry, and the on-wire size
 * of a row event bounds neither its column count nor its row count. A few
 * kilobytes of wire bytes can therefore buy one maximally expanded field per
 * column per row. The budget bounds the sum instead: every decoded column
 * charges its payload against it, across all rows and across both images of an
 * UPDATE, and the decode fails once the budget is exhausted. Compressed
 * columns clamp their expansion to what remains, so the peak — not merely the
 * final total — stays under @ref limit.
 *
 * @p used is also the accounting hook: callers can read back exactly how many
 * bytes an event bought.
 */
struct DecodeBudget {
  /// Maximum total decoded payload bytes one event may materialize.
  size_t limit = kMaxDecompressedColumnBytes;
  /// Bytes charged so far. Never exceeds @ref limit.
  size_t used = 0;

  /**
   * @brief Budget for an event body of @p body_bytes.
   *
   * The floor is the single-field expansion cap, so one legitimately huge
   * compressed column still decodes. Above that the budget follows the event
   * size, because an uncompressed value can never exceed the bytes it was
   * decoded from — an event that carries no compressed columns is thus never
   * rejected, whatever ceiling the caller configured for event size.
   */
  static DecodeBudget ForEventBody(size_t body_bytes) {
    DecodeBudget budget;
    if (body_bytes > budget.limit) budget.limit = body_bytes;
    return budget;
  }

  /// Bytes still available under the budget.
  size_t Remaining() const { return limit - used; }

  /// Charge @p bytes; returns false (and charges nothing) when over budget.
  bool Charge(size_t bytes) {
    if (bytes > Remaining()) return false;
    used += bytes;
    return true;
  }
};

/**
 * @brief Decode row data from WRITE_ROWS_EVENT body.
 * @param data Event body after header (starting at table_id).
 * @param len Length of event body (excluding checksum).
 * @param metadata Table metadata from TABLE_MAP event.
 * @param is_v2 true for V2 row events (type 30-32), false for V1 (type 23-25).
 * @param[out] rows Decoded rows output.
 * @param[in,out] budget Decode budget for this event; nullptr uses the default.
 * @return true on success, false on malformed data or budget exhaustion.
 */
bool DecodeWriteRows(const uint8_t* data, size_t len, const TableMetadata& metadata, bool is_v2,
                     std::vector<RowData>* rows, DecodeBudget* budget = nullptr);

/**
 * @brief Decode row data from UPDATE_ROWS_EVENT body.
 * @param data Event body after header.
 * @param len Length of event body.
 * @param metadata Table metadata from TABLE_MAP event.
 * @param is_v2 true for V2 events.
 * @param[out] pairs Before/after row pairs output.
 * @param[in,out] budget Decode budget for this event; nullptr uses the default.
 * @return true on success, false on malformed data or budget exhaustion.
 */
bool DecodeUpdateRows(const uint8_t* data, size_t len, const TableMetadata& metadata, bool is_v2,
                      std::vector<UpdatePair>* pairs, DecodeBudget* budget = nullptr);

/**
 * @brief Decode row data from DELETE_ROWS_EVENT body.
 * @param data Event body after header.
 * @param len Length of event body.
 * @param metadata Table metadata from TABLE_MAP event.
 * @param is_v2 true for V2 events.
 * @param[out] rows Decoded rows output.
 * @param[in,out] budget Decode budget for this event; nullptr uses the default.
 * @return true on success, false on malformed data or budget exhaustion.
 */
bool DecodeDeleteRows(const uint8_t* data, size_t len, const TableMetadata& metadata, bool is_v2,
                      std::vector<RowData>* rows, DecodeBudget* budget = nullptr);

/**
 * @brief Decode a single column value from raw binlog data.
 * @param type Column type.
 * @param meta Type-specific metadata.
 * @param is_unsigned Whether the column is unsigned.
 * @param data Pointer to the column data.
 * @param len Remaining data length.
 * @param[out] bytes_consumed Number of bytes consumed.
 * @param charset_known Whether TABLE_MAP carried a collation for the column.
 *        Character-family and BLOB-family columns are surfaced as bytes when it
 *        is false: the two members of each on-wire pair (VARCHAR/VARBINARY,
 *        CHAR/BINARY, TEXT/BLOB) share a binlog type byte, so without a
 *        collation they cannot be told apart, and bytes reproduce the payload
 *        exactly where text would corrupt a binary value.
 * @param binary_charset Whether that collation is the binary one.
 * @param max_value_bytes Ceiling on the decoded value; compressed columns
 *        refuse to expand beyond it. Callers decoding a whole event pass the
 *        remainder of their @ref DecodeBudget.
 * @return Decoded ColumnValue.
 */
ColumnValue DecodeColumnValue(ColumnType type, uint32_t meta, bool is_unsigned, const uint8_t* data,
                              size_t len, size_t* bytes_consumed, bool charset_known = false,
                              bool binary_charset = false,
                              size_t max_value_bytes = kMaxDecompressedColumnBytes);

}  // namespace mes

#endif  // MES_ROW_DECODER_H_
