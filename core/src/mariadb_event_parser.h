// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @file mariadb_event_parser.h
 * @brief MariaDB-specific binlog event parsing
 *
 * Parses MariaDB-specific events that differ from MySQL:
 * - MARIADB_GTID_EVENT (162): Extract domain-server-seq GTID
 * - MARIADB_GTID_LIST_EVENT (163): Parse GTID list at binlog start
 * - MARIADB_ANNOTATE_ROWS_EVENT (160): Extract SQL text for debug
 */

#ifndef MES_MARIADB_EVENT_PARSER_H_
#define MES_MARIADB_EVENT_PARSER_H_

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "event_header.h"
#include "mariadb_gtid.h"
#include "mes.h"

namespace mes {

/**
 * @brief MariaDB-specific binlog event parser
 *
 * Handles events with type codes 160-164 that are unique to MariaDB.
 * Row events (TABLE_MAP, WRITE/UPDATE/DELETE_ROWS) use the same binary
 * format as MySQL and are parsed by the standard event parser.
 */
class MariaDBEventParser {
 public:
  /**
   * @brief Extract GTID from MARIADB_GTID_EVENT (type 162).
   *
   * Event layout after 19-byte header:
   *   seq_no    (8 bytes, little-endian uint64)
   *   domain_id (4 bytes, little-endian uint32)
   *   flags     (1 byte)
   *
   * The server_id comes from the standard event header (bytes 5-8).
   *
   * @param buffer Raw event data (including 19-byte header).
   * @param length Total event length.
   * @param[out] out GTID string in "domain-server-seq" format.
   * @return MES_OK on success, MES_ERR_PARSE on error.
   */
  static mes_error_t ExtractGtid(const uint8_t* buffer, size_t length, std::string* out);

  /**
   * @brief Parse MARIADB_GTID_LIST_EVENT (type 163).
   *
   * Event layout after 19-byte header:
   *   count_and_flags (4 bytes, little-endian) - lower 28 bits = count
   *   entries[count]:
   *     domain_id (4 bytes, little-endian uint32)
   *     server_id (4 bytes, little-endian uint32)
   *     seq_no    (8 bytes, little-endian uint64)
   *
   * @param buffer Raw event data (including 19-byte header).
   * @param length Total event length.
   * @param[out] out Vector of parsed GTIDs.
   * @return MES_OK on success, MES_ERR_PARSE on error.
   */
  static mes_error_t ParseGtidList(const uint8_t* buffer, size_t length,
                                   std::vector<MariaDBGtid>* out);

  /**
   * @brief Extract SQL text from MARIADB_ANNOTATE_ROWS_EVENT (type 160).
   *
   * The SQL text occupies the event body after the header,
   * optionally excluding the 4-byte CRC32 checksum at the end.
   *
   * @param buffer Raw event data (including 19-byte header).
   * @param length Total event length.
   * @param has_checksum Whether the event includes a trailing CRC32 checksum.
   * @param[out] out SQL text string.
   * @return MES_OK on success, MES_ERR_PARSE on error.
   */
  static mes_error_t ExtractAnnotateRows(const uint8_t* buffer, size_t length, bool has_checksum,
                                         std::string* out);
};

}  // namespace mes

#endif  // MES_MARIADB_EVENT_PARSER_H_
