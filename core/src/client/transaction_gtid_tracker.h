// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#ifndef MES_CLIENT_TRANSACTION_GTID_TRACKER_H_
#define MES_CLIENT_TRANSACTION_GTID_TRACKER_H_

#include <array>
#include <cstddef>
#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "mariadb_gtid.h"
#include "server_flavor.h"

namespace mes {

/**
 * Tracks the received transaction GTID separately from checkpoint candidates.
 * The candidate is a complete MySQL SID interval set or MariaDB domain
 * high-water set, rather than only the most recently received GTID.
 */
class TransactionGtidTracker {
 public:
  void Reset();

  /** Reset and seed the complete set advertised as the stream start position. */
  bool Reset(const std::string& initial_gtid_set, ServerFlavor flavor);

  /**
   * Observe one complete event and return a safe complete-set checkpoint.
   *
   * A checkpoint is returned at a transaction commit boundary, or when a
   * PREVIOUS_GTIDS/GTID_LIST baseline event has been completely received.
   * The caller is responsible for promoting it only after delivering the
   * corresponding event to the consumer.
   */
  std::string Observe(const uint8_t* data, size_t size, bool has_checksum);

  /** Last transaction GTID received from the wire (may be uncommitted). */
  const std::string& received_gtid() const { return received_gtid_; }

 private:
  struct Interval {
    uint64_t start = 0;
    uint64_t end = 0;  // Exclusive.
  };

  using Sid = std::array<uint8_t, 16>;
  using MySQLSet = std::map<Sid, std::vector<Interval>>;
  using MariaDBSet = std::map<uint32_t, MariaDBGtid>;

  struct PendingGtid {
    bool present = false;
    ServerFlavor flavor = ServerFlavor::kMySQL;
    Sid sid{};
    uint64_t sequence_no = 0;
    MariaDBGtid mariadb;
  };

  static bool DecodeMySQLSet(const uint8_t* data, size_t size, MySQLSet* out);
  static void MergeInterval(std::vector<Interval>* intervals, Interval interval);
  static void MergeMariaDBGtid(MariaDBSet* set, const MariaDBGtid& gtid);
  static std::string FormatMySQLSet(const MySQLSet& set);
  static std::string FormatMariaDBSet(const MariaDBSet& set);

  std::string CommitPending();
  std::string FormatCurrentSet() const;

  ServerFlavor flavor_ = ServerFlavor::kMySQL;
  MySQLSet mysql_set_;
  MariaDBSet mariadb_set_;
  std::string received_gtid_;
  PendingGtid pending_gtid_;
};

}  // namespace mes

#endif  // MES_CLIENT_TRANSACTION_GTID_TRACKER_H_
