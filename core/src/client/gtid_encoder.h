// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @file gtid_encoder.h
 * @brief GTID binary encoding for COM_BINLOG_DUMP_GTID protocol
 */

#ifndef MES_CLIENT_GTID_ENCODER_H_
#define MES_CLIENT_GTID_ENCODER_H_

#include <array>
#include <cstdint>
#include <string>
#include <vector>

#include "mes.h"

namespace mes {

/**
 * @brief Encodes MySQL GTID sets into binary format for binlog replication
 *
 * Binary format:
 *   8 bytes: number of SIDs (UUIDs)
 *   For each SID:
 *     16 bytes: UUID
 *     8 bytes: number of intervals
 *     For each interval:
 *       8 bytes: start transaction number
 *       8 bytes: end transaction number (exclusive)
 */
class GtidEncoder {
 public:
  /**
   * @brief Encode a GTID set string into binary format
   * @param gtid_set String like "uuid:1-3,5-7" or "uuid1:1-3,uuid2:5-7"
   * @param out Output: binary encoded GTID set
   * @return MES_OK on success, error code otherwise
   */
  static mes_error_t Encode(const char* gtid_set, std::vector<uint8_t>* out);

  /**
   * @brief Convert single GTID "uuid:N" to range "uuid:1-N"
   *
   * COM_BINLOG_DUMP_GTID semantics: "send events NOT in this set".
   * A single GTID "uuid:101" means interval [101,102), causing duplicates.
   * Converting to "uuid:1-101" excludes all transactions 1 through 101.
   *
   * @param gtid GTID string to convert
   * @return Converted string (unchanged if already a range or multi-UUID)
   */
  static std::string ConvertSingleGtidToRange(const std::string& gtid);

 private:
  struct Interval {
    int64_t start;
    int64_t end;  // exclusive
  };

  struct Sid {
    std::array<uint8_t, 16> uuid{};
    std::vector<Interval> intervals;
  };

  static mes_error_t ParseUuid(const char* str, uint8_t* out);
  static mes_error_t ParseInterval(const char* str, Interval* out);
  static void StoreInt64Le(std::vector<uint8_t>& buf, uint64_t val);
};

}  // namespace mes

#endif  // MES_CLIENT_GTID_ENCODER_H_
