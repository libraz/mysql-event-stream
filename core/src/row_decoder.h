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
 * @brief Decode row data from WRITE_ROWS_EVENT body.
 * @param data Event body after header (starting at table_id).
 * @param len Length of event body (excluding checksum).
 * @param metadata Table metadata from TABLE_MAP event.
 * @param is_v2 true for V2 row events (type 30-32), false for V1 (type 23-25).
 * @param[out] rows Decoded rows output.
 * @return true on success, false on malformed data.
 */
bool DecodeWriteRows(const uint8_t* data, size_t len,
                     const TableMetadata& metadata, bool is_v2,
                     std::vector<RowData>* rows);

/**
 * @brief Decode row data from UPDATE_ROWS_EVENT body.
 * @param data Event body after header.
 * @param len Length of event body.
 * @param metadata Table metadata from TABLE_MAP event.
 * @param is_v2 true for V2 events.
 * @param[out] pairs Before/after row pairs output.
 * @return true on success, false on malformed data.
 */
bool DecodeUpdateRows(const uint8_t* data, size_t len,
                      const TableMetadata& metadata, bool is_v2,
                      std::vector<UpdatePair>* pairs);

/**
 * @brief Decode row data from DELETE_ROWS_EVENT body.
 * @param data Event body after header.
 * @param len Length of event body.
 * @param metadata Table metadata from TABLE_MAP event.
 * @param is_v2 true for V2 events.
 * @param[out] rows Decoded rows output.
 * @return true on success, false on malformed data.
 */
bool DecodeDeleteRows(const uint8_t* data, size_t len,
                      const TableMetadata& metadata, bool is_v2,
                      std::vector<RowData>* rows);

/**
 * @brief Decode a single column value from raw binlog data.
 * @param type Column type.
 * @param meta Type-specific metadata.
 * @param is_unsigned Whether the column is unsigned.
 * @param data Pointer to the column data.
 * @param len Remaining data length.
 * @param[out] bytes_consumed Number of bytes consumed.
 * @return Decoded ColumnValue.
 */
ColumnValue DecodeColumnValue(ColumnType type, uint16_t meta, bool is_unsigned,
                              const uint8_t* data, size_t len,
                              size_t* bytes_consumed);

}  // namespace mes

#endif  // MES_ROW_DECODER_H_
