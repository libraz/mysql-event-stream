// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @file test_e2e_tagged_gtid.cpp
 * @brief E2E test for resuming a client stream across tagged GTIDs
 *
 * A transaction committed under `gtid_next='AUTOMATIC:<tag>'` is written as a
 * GTID_TAGGED_LOG_EVENT, and the checkpoint the client publishes for it holds
 * a tagged TSID. Resuming from that checkpoint sends the set in the tagged
 * (format 1) COM_BINLOG_DUMP_GTID encoding. The test stops a stream after one
 * tagged transaction, resumes from the published checkpoint, and checks that
 * the transactions after it arrive exactly once and the one before it never.
 *
 * Tagged GTIDs exist on MySQL 8.4 and later only; the test skips elsewhere.
 * Requires a running server at localhost:13308.
 *
 * Start with: cd e2e/docker && docker compose up -d
 */

#include <gtest/gtest.h>

#include <cstdlib>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include "mes.h"
#include "test_e2e_helpers.h"

using namespace e2e;

namespace {

constexpr char kTag[] = "mes_e2e";
constexpr char kFirstRow[] = "tagged_gtid_first";
constexpr char kOfflineRow[] = "tagged_gtid_offline";
constexpr char kSecondRow[] = "tagged_gtid_second";
constexpr char kSentinelRow[] = "tagged_gtid_sentinel";

/** @brief Whether a MySQL version string is 8.4 or later. */
bool SupportsTaggedGtids(const std::string& version) {
  char* rest = nullptr;
  const long major = std::strtol(version.c_str(), &rest, 10);
  const long minor = (rest != nullptr && *rest == '.') ? std::strtol(rest + 1, nullptr, 10) : 0;
  return major > 8 || (major == 8 && minor >= 4);
}

/** @brief Insert one mes_test.items row as a transaction carrying kTag. */
mes_error_t InsertTaggedRow(const std::string& name) {
  // gtid_next is a session variable, so the SET and the INSERT share a session.
  mes::protocol::MysqlConnection conn;
  mes_error_t rc = conn.Connect(kHost, kPort, kRootUser, kRootPass, kTimeout, kTimeout,
                                DefaultSslMode(), DefaultCa(), "", "");
  if (rc != MES_OK) return rc;
  mes::protocol::QueryResult qr;
  std::string err;
  for (const std::string& sql :
       {std::string("SET gtid_next = 'AUTOMATIC:") + kTag + "'",
        "INSERT INTO mes_test.items (name, value) VALUES ('" + name + "', 1)"}) {
    rc = mes::protocol::ExecuteQuery(conn.Socket(), sql, &qr, &err);
    if (rc != MES_OK) {
      ADD_FAILURE() << sql << ": " << err;
      return rc;
    }
  }
  return MES_OK;
}

/** @brief A client stream started at a GTID set, feeding one engine. */
class TaggedStream {
 public:
  TaggedStream() = default;
  TaggedStream(const TaggedStream&) = delete;
  TaggedStream& operator=(const TaggedStream&) = delete;

  ~TaggedStream() {
    if (engine_ != nullptr) mes_destroy(engine_);
    if (client_ == nullptr) return;
    mes_client_stop(client_);
    mes_client_disconnect(client_);
    mes_client_destroy(client_);
  }

  /** @brief Connect and start at @p gtid_set; failures are reported on the test. */
  bool Start(const std::string& gtid_set, uint32_t server_id) {
    gtid_set_ = gtid_set;
    ca_ = DefaultCa();
    client_ = mes_client_create();
    engine_ = mes_create();
    if (client_ == nullptr || engine_ == nullptr) {
      ADD_FAILURE() << "mes_client_create or mes_create failed";
      return false;
    }

    mes_client_config_t config{};
    config.host = kHost;
    config.port = kPort;
    config.user = kReplUser;
    config.password = kReplPass;
    config.server_id = server_id;
    config.connect_timeout_s = kTimeout;
    config.read_timeout_s = 3;
    config.ssl_mode = static_cast<mes_ssl_mode_t>(DefaultSslMode());
    config.ssl_ca = ca_.empty() ? nullptr : ca_.c_str();
    config.start_position_mode = MES_START_AT_GTID;
    config.start_gtid = gtid_set_.c_str();

    if (mes_client_connect(client_, &config) != MES_OK) {
      ADD_FAILURE() << "mes_client_connect failed: " << mes_client_last_error(client_);
      return false;
    }
    if (mes_client_start(client_) != MES_OK) {
      ADD_FAILURE() << "mes_client_start failed: " << mes_client_last_error(client_);
      return false;
    }
    return true;
  }

  /**
   * @brief Poll until @p done holds, counting delivered items rows by name.
   * @return Whether @p done held before the poll budget ran out or a poll failed.
   */
  template <typename Done>
  bool PollUntil(Done&& done, int max_polls = 200) {
    for (int i = 0; i < max_polls; ++i) {
      const mes_poll_result_t result = mes_client_poll(client_);
      if (result.error != MES_OK) {
        ADD_FAILURE() << "mes_client_poll failed: " << mes_client_last_error(client_);
        return false;
      }
      if (!result.is_heartbeat && result.data != nullptr) {
        const auto drain = [this] {
          const mes_event_t* event = nullptr;
          while (mes_next_event(engine_, &event) == MES_OK) Record(CopyEvent(event));
        };
        if (!FeedAll(engine_, result, drain)) return false;
        drain();
      }
      if (done()) return true;
    }
    return false;
  }

  /** @brief Insert events delivered for the row named @p name. */
  int Count(const std::string& name) const {
    const auto it = rows_.find(name);
    return it == rows_.end() ? 0 : it->second;
  }

  /** @brief The checkpoint the client has published, or "" for none. */
  std::string Checkpoint() const {
    const char* checkpoint = mes_client_current_gtid(client_);
    return checkpoint != nullptr ? checkpoint : "";
  }

 private:
  void Record(const CapturedEvent& event) {
    if (event.type != MES_EVENT_INSERT || event.table != "items") return;
    for (const CapturedColumn& column : event.after) {
      if (column.str_data.rfind("tagged_gtid_", 0) == 0) ++rows_[column.str_data];
    }
  }

  mes_client_t* client_ = nullptr;
  mes_engine_t* engine_ = nullptr;
  std::string gtid_set_;
  std::string ca_;
  std::map<std::string, int> rows_;
};

TEST(E2ETaggedGtid, ResumingFromATaggedCheckpointDeliversEachLaterTransactionOnce) {
  if (IsMariaDB()) GTEST_SKIP() << "Tagged GTIDs are MySQL-only";
  const std::optional<std::string> version = QueryScalar("SELECT @@version");
  ASSERT_TRUE(version.has_value()) << "version probe failed";
  if (!SupportsTaggedGtids(*version)) {
    GTEST_SKIP() << "Tagged GTIDs need MySQL 8.4 or later, server is " << *version;
  }

  ScopedCleanup cleanup("DELETE FROM mes_test.items WHERE name LIKE 'tagged_gtid_%'");
  const std::string start = GetCurrentGtid();
  ASSERT_FALSE(start.empty());
  const std::string tagged_tsid = std::string(":") + kTag + ":";

  // First stream: one tagged transaction, then the checkpoint that covers it.
  std::string saved;
  {
    TaggedStream first;
    ASSERT_TRUE(first.Start(start, server_ids::kTaggedGtidFirstStream));
    ASSERT_EQ(InsertTaggedRow(kFirstRow), MES_OK);
    ASSERT_TRUE(first.PollUntil([&] {
      return first.Count(kFirstRow) == 1 &&
             first.Checkpoint().find(tagged_tsid) != std::string::npos;
    })) << "checkpoint: "
        << first.Checkpoint();
    EXPECT_EQ(first.Count(kFirstRow), 1);
    saved = first.Checkpoint();
  }

  // Committed while no stream is running: the resumed stream must pick it up.
  ASSERT_EQ(InsertTaggedRow(kOfflineRow), MES_OK);

  // Second stream: resume from the tagged checkpoint.
  TaggedStream resumed;
  ASSERT_TRUE(resumed.Start(saved, server_ids::kTaggedGtidResumedStream))
      << "resume from " << saved;
  ASSERT_EQ(InsertTaggedRow(kSecondRow), MES_OK);
  // Rows arrive in commit order, so once the sentinel is in, any duplicate of an
  // earlier transaction would already have been delivered.
  ASSERT_EQ(InsertTaggedRow(kSentinelRow), MES_OK);
  ASSERT_TRUE(resumed.PollUntil([&] { return resumed.Count(kSentinelRow) > 0; }))
      << "resumed from " << saved;

  EXPECT_EQ(resumed.Count(kFirstRow), 0) << "resumed from " << saved;
  EXPECT_EQ(resumed.Count(kOfflineRow), 1) << "resumed from " << saved;
  EXPECT_EQ(resumed.Count(kSecondRow), 1) << "resumed from " << saved;
  EXPECT_EQ(resumed.Count(kSentinelRow), 1) << "resumed from " << saved;
  EXPECT_NE(resumed.Checkpoint().find(tagged_tsid), std::string::npos) << resumed.Checkpoint();
}

}  // namespace
