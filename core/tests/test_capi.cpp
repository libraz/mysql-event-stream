// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>
#include <thread>
#include <vector>

#include "client/binlog_client.h"
#include "client/metadata_fetcher.h"
#include "event_header.h"
#include "mes.h"
#include "source_scan.h"
#include "test_helpers.h"

namespace mes {
// Defined in capi.cpp. Not part of the public C ABI: it is the only way to put
// an engine into the "metadata enabled" shape without a live MySQL server.
void CapiInstallUnconnectedMetadataFetcher(mes_engine_t* engine);

/** @brief Reads the credential a MetadataFetcher retains for reconnection. */
class MetadataFetcherTestAccess {
 public:
  static const std::string& RetainedPassword(const MetadataFetcher& fetcher) {
    return fetcher.password_;
  }
};
}  // namespace mes

namespace {

using mes::BinlogEventType;
using mes::test::BuildDeleteRowsBody;
using mes::test::BuildEvent;
using mes::test::BuildQueryEventBody;
using mes::test::BuildRotateBody;
using mes::test::BuildTableMapBody;
using mes::test::BuildUpdateRowsBody;
using mes::test::BuildWriteRowsBody;

// ---- ABI introspection ----

TEST(CApi, SizeofEventMatchesStruct) { EXPECT_EQ(mes_sizeof_event(), sizeof(mes_event_t)); }

TEST(CApi, SizeofColumnMatchesStruct) { EXPECT_EQ(mes_sizeof_column(), sizeof(mes_column_t)); }

// The literal version is not asserted here: it lives in the header macros, and
// CheckVersionConsistency.cmake fails configuration when those drift from the
// CMake project version or either binding manifest. What this asserts is that
// mes_version() reports exactly what a caller compiles against.
TEST(CApi, VersionAndAbiAreExposed) {
  const std::string expected = std::to_string(MES_VERSION_MAJOR) + "." +
                               std::to_string(MES_VERSION_MINOR) + "." +
                               std::to_string(MES_VERSION_PATCH);
  EXPECT_EQ(mes_version(), expected);
  EXPECT_EQ(mes_abi_version(), MES_ABI_VERSION);
}

// ---- Client configuration defaults ----

// A zero timeout field means "unset", so the bound a zero-initialized C config
// ends up with must be the one a direct C++ caller of BinlogClient gets. The
// two values are declared separately -- the header names them for C callers,
// BinlogClientConfig applies them -- so nothing but this assertion keeps one
// from being raised without the other.
TEST(CApi, DocumentedTimeoutDefaultsMatchTheClientDefaults) {
  const mes::BinlogClientConfig defaults;
  EXPECT_EQ(MES_DEFAULT_CONNECT_TIMEOUT_S, defaults.connect_timeout_s);
  EXPECT_EQ(MES_DEFAULT_READ_TIMEOUT_S, defaults.read_timeout_s);
}

// Both C ABI entry points that accept mes_client_config_t must resolve a zero
// timeout to that default before the value reaches the transport: an unresolved
// zero disables the bound, and resolving it at one entry point only would give
// one struct field two meanings. Whether the bound is in force is observable
// only by waiting it out, so the property is asserted on the sources: the field
// is read exactly twice per entry point, both times inside the fallback.
TEST(CApi, NeitherEntryPointPassesAnUnresolvedTimeoutToTheTransport) {
  // Assembled from fragments so this test is not a hit in its own scan.
  const std::string field = std::string("config-") + ">";
  const std::string macro = std::string("MES_DEFAULT_") + "CONNECT_TIMEOUT_S";
  const std::string read_macro = std::string("MES_DEFAULT_") + "READ_TIMEOUT_S";
  const struct {
    std::string field;
    std::string fallback;
  } resolutions[] = {
      {field + "connect_timeout_s", macro},
      {field + "read_timeout_s", read_macro},
  };

  for (const char* relative : {"core/src/capi.cpp", "core/src/client/capi_client.cpp"}) {
    const std::filesystem::path source = mes::source_scan::RepoRoot() / relative;
    const std::string text = mes::source_scan::ReadCollapsed(source);
    ASSERT_FALSE(text.empty()) << "unreadable: " << source;

    for (const auto& resolution : resolutions) {
      const std::string expression =
          resolution.field + " != 0 ? " + resolution.field + " : " + resolution.fallback;
      EXPECT_EQ(mes::source_scan::CountOccurrences(text, expression), 1)
          << relative << " must resolve " << resolution.field << " through " << resolution.fallback;
      EXPECT_EQ(mes::source_scan::CountOccurrences(text, resolution.field), 2)
          << relative << " reads " << resolution.field << " outside its fallback, so a zero "
          << "reaches the transport unresolved";
    }
  }
}

// ---- Engine lifecycle ----

TEST(CApi, CreateAndDestroy) {
  mes_engine_t* engine = mes_create();
  ASSERT_NE(engine, nullptr);
  mes_destroy(engine);
}

TEST(CApi, DestroyNull) {
  mes_destroy(nullptr);  // Should not crash
}

TEST(CApi, DestroyWithMetadataFetcherKeepsFetcherAliveForTheEngine) {
  mes_engine_t* engine = mes_create();
  ASSERT_NE(engine, nullptr);
  mes::CapiInstallUnconnectedMetadataFetcher(engine);

  // A DDL QUERY_EVENT makes the engine dereference the fetcher it does not own.
  auto ddl = BuildEvent(static_cast<uint8_t>(BinlogEventType::kQueryEvent), 1000, 100,
                        BuildQueryEventBody("testdb", "ALTER TABLE users ADD COLUMN x INT"));
  size_t consumed = 0;
  ASSERT_EQ(mes_feed(engine, ddl.data(), ddl.size(), &consumed), MES_OK);
  EXPECT_EQ(consumed, ddl.size());

  // The fetcher outlives the CdcEngine that points at it; under ASan/LSan this
  // also asserts it is released exactly once.
  mes_destroy(engine);
}

TEST(CApi, MetadataConnectionRejectsInvalidSslModeBeforeConnecting) {
  mes_engine_t* engine = mes_create();
  ASSERT_NE(engine, nullptr);
  mes_client_config_t config{};
  config.ssl_mode = static_cast<mes_ssl_mode_t>(99);
  EXPECT_EQ(mes_engine_set_metadata_conn(engine, &config), MES_ERR_INVALID_ARG);
  mes_destroy(engine);
}

TEST(CApi, MetadataFetcherReleasesItsRetainedPasswordOnDisconnect) {
  mes::MetadataFetcher fetcher;
  // Nothing listens on loopback port 1, so Connect() fails, but only after
  // retaining the parameters its single reconnect attempt needs.
  EXPECT_NE(fetcher.Connect("127.0.0.1", 1, "repl", "cleartext-secret", 1, 1), MES_OK);
  EXPECT_EQ(mes::MetadataFetcherTestAccess::RetainedPassword(fetcher), "cleartext-secret");

  // Disconnect is the end of the reconnect window, so nothing may keep holding
  // the plaintext credential afterwards.
  fetcher.Disconnect();
  EXPECT_TRUE(mes::MetadataFetcherTestAccess::RetainedPassword(fetcher).empty());
}

TEST(CApi, ClientRejectsZeroServerIdBeforeConnecting) {
  mes_client_t* client = mes_client_create();
  ASSERT_NE(client, nullptr);
  mes_client_config_t config{};
  config.server_id = 0;
  EXPECT_EQ(mes_client_connect(client, &config), MES_ERR_INVALID_ARG);
  EXPECT_STREQ(mes_client_last_error(client), "server_id must be non-zero");
  mes_client_destroy(client);
}

// ---- Null argument handling ----

TEST(CApi, FeedNullEngine) {
  uint8_t data[] = {0};
  size_t consumed;
  EXPECT_EQ(mes_feed(nullptr, data, 1, &consumed), MES_ERR_NULL_ARG);
}

TEST(CApi, FeedNullConsumed) {
  auto* engine = mes_create();
  uint8_t data[] = {0};
  EXPECT_EQ(mes_feed(engine, data, 1, nullptr), MES_ERR_NULL_ARG);
  mes_destroy(engine);
}

TEST(CApi, FeedNullDataWithLen) {
  auto* engine = mes_create();
  size_t consumed;
  EXPECT_EQ(mes_feed(engine, nullptr, 10, &consumed), MES_ERR_NULL_ARG);
  mes_destroy(engine);
}

TEST(CApi, FeedNullDataZeroLen) {
  auto* engine = mes_create();
  size_t consumed;
  EXPECT_EQ(mes_feed(engine, nullptr, 0, &consumed), MES_OK);
  EXPECT_EQ(consumed, 0u);
  mes_destroy(engine);
}

TEST(CApi, NextEventNullEngine) {
  const mes_event_t* event;
  EXPECT_EQ(mes_next_event(nullptr, &event), MES_ERR_NULL_ARG);
}

TEST(CApi, NextEventNullOutput) {
  auto* engine = mes_create();
  EXPECT_EQ(mes_next_event(engine, nullptr), MES_ERR_NULL_ARG);
  mes_destroy(engine);
}

TEST(CApi, NextEventNoEvent) {
  auto* engine = mes_create();
  const mes_event_t* event;
  EXPECT_EQ(mes_next_event(engine, &event), MES_ERR_NO_EVENT);
  mes_destroy(engine);
}

TEST(CApi, HasEventsNull) { EXPECT_EQ(mes_has_events(nullptr), 0); }

TEST(CApi, HasEventsEmpty) {
  auto* engine = mes_create();
  EXPECT_EQ(mes_has_events(engine), 0);
  mes_destroy(engine);
}

TEST(CApi, GetPositionNullEngine) {
  const char* file;
  uint64_t offset;
  EXPECT_EQ(mes_get_position(nullptr, &file, &offset), MES_ERR_NULL_ARG);
}

TEST(CApi, GetPositionNullOutputs) {
  auto* engine = mes_create();
  // Both file and offset are null -- should still succeed
  EXPECT_EQ(mes_get_position(engine, nullptr, nullptr), MES_OK);
  mes_destroy(engine);
}

TEST(CApi, ResetNullEngine) { EXPECT_EQ(mes_reset(nullptr), MES_ERR_NULL_ARG); }

// ---- INSERT flow ----

TEST(CApi, InsertEvent) {
  auto* engine = mes_create();

  auto tm_body = BuildTableMapBody(1, "testdb", "users");
  auto tm_event =
      BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000, 100, tm_body);

  auto wr_body = BuildWriteRowsBody(1, 42);
  auto wr_event =
      BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1000, 200, wr_body);

  std::vector<uint8_t> stream;
  stream.insert(stream.end(), tm_event.begin(), tm_event.end());
  stream.insert(stream.end(), wr_event.begin(), wr_event.end());

  size_t consumed;
  ASSERT_EQ(mes_feed(engine, stream.data(), stream.size(), &consumed), MES_OK);
  EXPECT_EQ(consumed, stream.size());

  EXPECT_EQ(mes_has_events(engine), 1);

  const mes_event_t* event;
  ASSERT_EQ(mes_next_event(engine, &event), MES_OK);

  EXPECT_EQ(event->type, MES_EVENT_INSERT);
  EXPECT_STREQ(event->database, "testdb");
  EXPECT_STREQ(event->table, "users");
  EXPECT_EQ(event->before_count, 0u);
  EXPECT_EQ(event->before_columns, nullptr);
  ASSERT_EQ(event->after_count, 1u);
  EXPECT_EQ(event->after_columns[0].type, MES_COL_INT);
  EXPECT_EQ(event->after_columns[0].int_val, 42);
  EXPECT_STREQ(event->after_columns[0].col_name, "");
  EXPECT_EQ(event->timestamp, 1000u);

  // No more events
  EXPECT_EQ(mes_next_event(engine, &event), MES_ERR_NO_EVENT);
  mes_destroy(engine);
}

// mes_event_t.source_sql is a NUL-terminated pointer valid until the next
// mes_feed/mes_next_event/mes_reset, whether or not the row was annotated.
TEST(CApi, SourceSqlIsNulTerminatedForAnnotatedAndPlainEvents) {
  auto* engine = mes_create();

  const std::string sql = "INSERT INTO users VALUES (42)";
  auto annotate = BuildEvent(static_cast<uint8_t>(BinlogEventType::kMariaDBAnnotateRowsEvent), 1000,
                             50, std::vector<uint8_t>(sql.begin(), sql.end()));
  auto tm_event = BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000, 100,
                             BuildTableMapBody(1, "testdb", "users"));
  auto wr_event = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1000, 200,
                             BuildWriteRowsBody(1, 42));

  std::vector<uint8_t> stream;
  stream.insert(stream.end(), annotate.begin(), annotate.end());
  stream.insert(stream.end(), tm_event.begin(), tm_event.end());
  stream.insert(stream.end(), wr_event.begin(), wr_event.end());

  size_t consumed;
  ASSERT_EQ(mes_feed(engine, stream.data(), stream.size(), &consumed), MES_OK);

  const mes_event_t* event;
  ASSERT_EQ(mes_next_event(engine, &event), MES_OK);
  ASSERT_NE(event->source_sql, nullptr);
  EXPECT_STREQ(event->source_sql, sql.c_str());

  // A XID ends the annotated statement, so the next row carries an empty
  // (but still dereferenceable) source_sql.
  auto xid = BuildEvent(static_cast<uint8_t>(BinlogEventType::kXidEvent), 1000, 250, {});
  auto next_wr = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1001, 300,
                            BuildWriteRowsBody(1, 43));
  std::vector<uint8_t> tail;
  tail.insert(tail.end(), xid.begin(), xid.end());
  tail.insert(tail.end(), next_wr.begin(), next_wr.end());
  ASSERT_EQ(mes_feed(engine, tail.data(), tail.size(), &consumed), MES_OK);

  ASSERT_EQ(mes_next_event(engine, &event), MES_OK);
  ASSERT_NE(event->source_sql, nullptr);
  EXPECT_STREQ(event->source_sql, "");

  mes_destroy(engine);
}

// mes_event_t.database, .table and .binlog_file are never-NULL, NUL-terminated
// pointers into engine-owned storage, valid until the next
// mes_feed/mes_next_event/mes_reset. They borrow the TABLE_MAP registration and
// the active binlog filename instead of copying them, so that lifetime has to
// hold after the stream has moved past both.
TEST(CApi, EventStringsStayNulTerminatedAfterTheStreamDropsWhatTheyName) {
  auto* engine = mes_create();
  ASSERT_NE(engine, nullptr);

  auto tm_event = BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000, 100,
                             BuildTableMapBody(1, "testdb", "users"));
  auto wr_event = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1001, 200,
                             BuildWriteRowsBody(1, 42));

  size_t consumed = 0;
  ASSERT_EQ(mes_feed(engine, tm_event.data(), tm_event.size(), &consumed), MES_OK);
  ASSERT_EQ(mes_feed(engine, wr_event.data(), wr_event.size(), &consumed), MES_OK);

  const mes_event_t* event = nullptr;
  ASSERT_EQ(mes_next_event(engine, &event), MES_OK);
  ASSERT_NE(event->database, nullptr);
  ASSERT_NE(event->table, nullptr);
  EXPECT_STREQ(event->database, "testdb");
  EXPECT_STREQ(event->table, "users");
  // No ROTATE has been seen, so the filename is the documented empty string --
  // still a pointer that can be read rather than NULL.
  ASSERT_NE(event->binlog_file, nullptr);
  EXPECT_STREQ(event->binlog_file, "");

  // Queue a second row, then rotate: the ROTATE replaces the filename and
  // clears the registry that owns the names, both before the row is fetched.
  auto wr2_event = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1002, 300,
                              BuildWriteRowsBody(1, 43));
  auto rotate_event = BuildEvent(static_cast<uint8_t>(BinlogEventType::kRotateEvent), 1003, 400,
                                 BuildRotateBody(4, "mysql-bin.000009"));
  ASSERT_EQ(mes_feed(engine, wr2_event.data(), wr2_event.size(), &consumed), MES_OK);
  ASSERT_EQ(mes_feed(engine, rotate_event.data(), rotate_event.size(), &consumed), MES_OK);

  ASSERT_EQ(mes_next_event(engine, &event), MES_OK);
  ASSERT_NE(event->database, nullptr);
  ASSERT_NE(event->table, nullptr);
  EXPECT_STREQ(event->database, "testdb");
  EXPECT_STREQ(event->table, "users");
  // No length travels with these pointers, so only the terminator bounds them.
  EXPECT_EQ(std::strlen(event->database), std::strlen("testdb"));
  EXPECT_EQ(std::strlen(event->table), std::strlen("users"));
  // The row was decoded before the rotation, so it still resumes in the file
  // that applied then -- which for this row is still no file at all.
  EXPECT_STREQ(event->binlog_file, "");
  EXPECT_EQ(event->binlog_offset, 300u);

  mes_destroy(engine);
}

// ---- UPDATE flow ----

TEST(CApi, UpdateEvent) {
  auto* engine = mes_create();

  auto tm_body = BuildTableMapBody(10, "db", "t");
  auto tm_event =
      BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000, 100, tm_body);

  auto upd_body = BuildUpdateRowsBody(10, 100, 200);
  auto upd_event =
      BuildEvent(static_cast<uint8_t>(BinlogEventType::kUpdateRowsEvent), 1002, 200, upd_body);

  std::vector<uint8_t> stream;
  stream.insert(stream.end(), tm_event.begin(), tm_event.end());
  stream.insert(stream.end(), upd_event.begin(), upd_event.end());

  size_t consumed;
  ASSERT_EQ(mes_feed(engine, stream.data(), stream.size(), &consumed), MES_OK);

  const mes_event_t* event;
  ASSERT_EQ(mes_next_event(engine, &event), MES_OK);

  EXPECT_EQ(event->type, MES_EVENT_UPDATE);
  EXPECT_STREQ(event->database, "db");
  EXPECT_STREQ(event->table, "t");
  ASSERT_EQ(event->before_count, 1u);
  EXPECT_EQ(event->before_columns[0].type, MES_COL_INT);
  EXPECT_EQ(event->before_columns[0].int_val, 100);
  ASSERT_EQ(event->after_count, 1u);
  EXPECT_EQ(event->after_columns[0].type, MES_COL_INT);
  EXPECT_EQ(event->after_columns[0].int_val, 200);
  EXPECT_STREQ(event->before_columns[0].col_name, "");
  EXPECT_STREQ(event->after_columns[0].col_name, "");

  mes_destroy(engine);
}

// ---- DELETE flow ----

TEST(CApi, DeleteEvent) {
  auto* engine = mes_create();

  auto tm_body = BuildTableMapBody(10, "db", "t");
  auto tm_event =
      BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000, 100, tm_body);

  auto del_body = BuildDeleteRowsBody(10, 999);
  auto del_event =
      BuildEvent(static_cast<uint8_t>(BinlogEventType::kDeleteRowsEvent), 1003, 200, del_body);

  std::vector<uint8_t> stream;
  stream.insert(stream.end(), tm_event.begin(), tm_event.end());
  stream.insert(stream.end(), del_event.begin(), del_event.end());

  size_t consumed;
  ASSERT_EQ(mes_feed(engine, stream.data(), stream.size(), &consumed), MES_OK);

  const mes_event_t* event;
  ASSERT_EQ(mes_next_event(engine, &event), MES_OK);

  EXPECT_EQ(event->type, MES_EVENT_DELETE);
  ASSERT_EQ(event->before_count, 1u);
  EXPECT_EQ(event->before_columns[0].type, MES_COL_INT);
  EXPECT_EQ(event->before_columns[0].int_val, 999);
  EXPECT_STREQ(event->before_columns[0].col_name, "");
  EXPECT_EQ(event->after_count, 0u);
  EXPECT_EQ(event->after_columns, nullptr);

  mes_destroy(engine);
}

// ---- ROTATE event ----

TEST(CApi, RotateEvent) {
  auto* engine = mes_create();

  auto rot_body = BuildRotateBody(4, "binlog.000002");
  auto rot_event = BuildEvent(static_cast<uint8_t>(BinlogEventType::kRotateEvent), 0, 0, rot_body);

  size_t consumed;
  mes_feed(engine, rot_event.data(), rot_event.size(), &consumed);

  const char* file;
  uint64_t offset;
  ASSERT_EQ(mes_get_position(engine, &file, &offset), MES_OK);
  EXPECT_STREQ(file, "binlog.000002");
  EXPECT_EQ(offset, 4u);

  mes_destroy(engine);
}

// ---- Reset ----

TEST(CApi, Reset) {
  auto* engine = mes_create();

  auto tm_body = BuildTableMapBody(1, "db", "t");
  auto tm_event =
      BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000, 100, tm_body);
  auto wr_body = BuildWriteRowsBody(1, 42);
  auto wr_event =
      BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1001, 200, wr_body);

  size_t consumed;
  mes_feed(engine, tm_event.data(), tm_event.size(), &consumed);
  mes_feed(engine, wr_event.data(), wr_event.size(), &consumed);
  EXPECT_EQ(mes_has_events(engine), 1);

  EXPECT_EQ(mes_reset(engine), MES_OK);
  EXPECT_EQ(mes_has_events(engine), 1);
  const mes_event_t* preserved = nullptr;
  ASSERT_EQ(mes_next_event(engine, &preserved), MES_OK);
  ASSERT_NE(preserved, nullptr);
  EXPECT_EQ(preserved->type, MES_EVENT_INSERT);

  mes_destroy(engine);
}

// ---- Multiple events ----

TEST(CApi, MultipleEvents) {
  auto* engine = mes_create();

  auto tm_body = BuildTableMapBody(1, "db", "t");
  auto tm_event =
      BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000, 100, tm_body);

  auto wr1 = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1001, 200,
                        BuildWriteRowsBody(1, 10));
  auto wr2 = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1002, 300,
                        BuildWriteRowsBody(1, 20));

  std::vector<uint8_t> stream;
  stream.insert(stream.end(), tm_event.begin(), tm_event.end());
  stream.insert(stream.end(), wr1.begin(), wr1.end());
  stream.insert(stream.end(), wr2.begin(), wr2.end());

  size_t consumed;
  ASSERT_EQ(mes_feed(engine, stream.data(), stream.size(), &consumed), MES_OK);

  const mes_event_t* event;

  ASSERT_EQ(mes_next_event(engine, &event), MES_OK);
  EXPECT_EQ(event->type, MES_EVENT_INSERT);
  EXPECT_EQ(event->after_columns[0].int_val, 10);

  ASSERT_EQ(mes_next_event(engine, &event), MES_OK);
  EXPECT_EQ(event->type, MES_EVENT_INSERT);
  EXPECT_EQ(event->after_columns[0].int_val, 20);

  EXPECT_EQ(mes_next_event(engine, &event), MES_ERR_NO_EVENT);

  mes_destroy(engine);
}

// ---- Event storage reuse ----

// A TABLE_MAP body for one nullable BLOB column with a 1-byte length prefix.
// A blob payload reaches the C ABI as a pointer into engine-owned heap storage,
// which an inline scalar column would not exercise.
std::vector<uint8_t> BuildBlobTableMapBody(uint64_t table_id, const std::string& db,
                                           const std::string& table) {
  mes::test::EventBuilder b;
  b.WriteU48Le(table_id);
  b.WriteU16Le(0);  // flags
  b.WriteU8(static_cast<uint8_t>(db.size()));
  b.WriteString(db);
  b.WriteU8(0);
  b.WriteU8(static_cast<uint8_t>(table.size()));
  b.WriteString(table);
  b.WriteU8(0);
  b.WriteU8(1);     // column_count
  b.WriteU8(0xFC);  // BLOB
  b.WriteU8(1);     // metadata length
  b.WriteU8(1);     // pack_length
  b.WriteU8(0x01);  // null bitmap: nullable
  return b.Data();
}

// A WRITE_ROWS_EVENT V2 body for the single-BLOB schema above.
std::vector<uint8_t> BuildBlobWriteRowsBody(uint64_t table_id,
                                            const std::vector<uint8_t>& payload) {
  mes::test::EventBuilder b;
  b.WriteU48Le(table_id);
  b.WriteU16Le(0);  // flags
  b.WriteU16Le(2);  // V2 var_header_len
  b.WriteU8(1);     // column_count
  b.WriteU8(0x01);  // columns_present
  b.WriteU8(0x00);  // null bitmap: value present
  b.WriteU8(static_cast<uint8_t>(payload.size()));
  b.WriteBytes(payload);
  return b.Data();
}

// mes.h ends the lifetime of a returned event at the next
// mes_feed()/mes_next_event()/mes_reset() call, and a binding is required to
// copy out of it before then. That rule is a permission to invalidate, so
// neither "the event still reads correctly afterwards" nor "the memory is gone"
// is a property this ABI promises. What it does state is ownership: the header
// calls this struct a read-only view into engine internals, and declares no
// function that frees one, so an event cannot be a per-call allocation without
// leaking. One view per engine is therefore the design and not an accident of
// it, and that is what makes the copy obligation real rather than
// precautionary -- a caller that retains the pointer silently reads whatever
// event was decoded most recently instead of the one it fetched.
TEST(CApi, RetainedEventPointerFollowsTheStreamInsteadOfKeepingItsOwnEvent) {
  auto* engine = mes_create();
  ASSERT_NE(engine, nullptr);

  // Two rows of the same schema and payload length, differing only in their
  // bytes, so what a retained pointer shows can be attributed to the refill.
  const std::vector<uint8_t> first_payload(48, 0xA1);
  const std::vector<uint8_t> second_payload(48, 0xB2);

  auto tm = BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000, 100,
                       BuildBlobTableMapBody(7, "heldb", "heldt"));
  auto wr1 = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1001, 200,
                        BuildBlobWriteRowsBody(7, first_payload));
  auto wr2 = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1002, 300,
                        BuildBlobWriteRowsBody(7, second_payload));

  size_t consumed = 0;
  ASSERT_EQ(mes_feed(engine, tm.data(), tm.size(), &consumed), MES_OK);
  ASSERT_EQ(mes_feed(engine, wr1.data(), wr1.size(), &consumed), MES_OK);

  const mes_event_t* retained = nullptr;
  ASSERT_EQ(mes_next_event(engine, &retained), MES_OK);
  ASSERT_NE(retained, nullptr);
  ASSERT_EQ(retained->after_count, 1u);
  ASSERT_EQ(retained->after_columns[0].type, MES_COL_BYTES);
  ASSERT_EQ(retained->after_columns[0].str_len, first_payload.size());
  EXPECT_EQ(
      std::memcmp(retained->after_columns[0].str_data, first_payload.data(), first_payload.size()),
      0);
  EXPECT_EQ(retained->timestamp, 1001u);

  ASSERT_EQ(mes_feed(engine, wr2.data(), wr2.size(), &consumed), MES_OK);

  const mes_event_t* second = nullptr;
  ASSERT_EQ(mes_next_event(engine, &second), MES_OK);

  // One slot: the second fetch hands back the same struct the first one did.
  EXPECT_EQ(second, retained);

  // Read through the retained pointer only, as a binding that skipped the copy
  // would. Every field it offers now describes the second row.
  ASSERT_EQ(retained->after_count, 1u);
  ASSERT_EQ(retained->after_columns[0].str_len, second_payload.size());
  EXPECT_EQ(std::memcmp(retained->after_columns[0].str_data, second_payload.data(),
                        second_payload.size()),
            0);
  EXPECT_EQ(retained->timestamp, 1002u);

  mes_destroy(engine);
}

// --- ConvertColumn coverage tests ---

TEST(CapiTest, FloatColumn) {
  mes_engine_t* engine = mes_create();
  ASSERT_NE(engine, nullptr);

  // Build TABLE_MAP with FLOAT column
  mes::test::EventBuilder tb;
  tb.WriteU48Le(42);
  tb.WriteU16Le(0);
  tb.WriteU8(2);
  tb.WriteString("db");
  tb.WriteU8(0);
  tb.WriteU8(1);
  tb.WriteString("t");
  tb.WriteU8(0);
  tb.WriteU8(1);     // 1 column
  tb.WriteU8(0x04);  // FLOAT
  tb.WriteU8(1);     // metadata length
  tb.WriteU8(4);     // float size
  tb.WriteU8(0x01);  // null bitmap
  auto tm_event =
      BuildEvent(static_cast<uint8_t>(mes::BinlogEventType::kTableMapEvent), 1000, 100, tb.Data());

  // Build WRITE_ROWS with float value
  mes::test::EventBuilder rb;
  rb.WriteU48Le(42);
  rb.WriteU16Le(0);
  rb.WriteU16Le(2);
  rb.WriteU8(1);
  rb.WriteU8(0x01);
  rb.WriteU8(0x00);
  float fval = 3.14f;
  uint8_t fbytes[4];
  memcpy(fbytes, &fval, 4);
  rb.WriteBytes(std::vector<uint8_t>(fbytes, fbytes + 4));
  auto wr_event =
      BuildEvent(static_cast<uint8_t>(mes::BinlogEventType::kWriteRowsEvent), 1000, 200, rb.Data());

  std::vector<uint8_t> stream;
  stream.insert(stream.end(), tm_event.begin(), tm_event.end());
  stream.insert(stream.end(), wr_event.begin(), wr_event.end());

  size_t consumed = 0;
  mes_feed(engine, stream.data(), stream.size(), &consumed);

  const mes_event_t* event = nullptr;
  ASSERT_EQ(mes_next_event(engine, &event), MES_OK);
  ASSERT_NE(event, nullptr);
  ASSERT_GE(event->after_count, 1u);
  EXPECT_EQ(event->after_columns[0].type, MES_COL_DOUBLE);
  EXPECT_NEAR(event->after_columns[0].double_val, 3.14, 0.01);

  mes_destroy(engine);
}

TEST(CapiTest, BlobColumn) {
  mes_engine_t* engine = mes_create();
  ASSERT_NE(engine, nullptr);

  mes::test::EventBuilder tb;
  tb.WriteU48Le(42);
  tb.WriteU16Le(0);
  tb.WriteU8(2);
  tb.WriteString("db");
  tb.WriteU8(0);
  tb.WriteU8(1);
  tb.WriteString("t");
  tb.WriteU8(0);
  tb.WriteU8(1);
  tb.WriteU8(0xFC);  // BLOB
  tb.WriteU8(1);     // metadata length
  tb.WriteU8(1);     // pack_length = 1
  tb.WriteU8(0x01);
  auto tm_event =
      BuildEvent(static_cast<uint8_t>(mes::BinlogEventType::kTableMapEvent), 1000, 100, tb.Data());

  mes::test::EventBuilder rb;
  rb.WriteU48Le(42);
  rb.WriteU16Le(0);
  rb.WriteU16Le(2);
  rb.WriteU8(1);
  rb.WriteU8(0x01);
  rb.WriteU8(0x00);
  rb.WriteU8(3);  // blob length
  rb.WriteBytes(std::vector<uint8_t>({0xDE, 0xAD, 0xBE}));
  auto wr_event =
      BuildEvent(static_cast<uint8_t>(mes::BinlogEventType::kWriteRowsEvent), 1000, 200, rb.Data());

  std::vector<uint8_t> stream;
  stream.insert(stream.end(), tm_event.begin(), tm_event.end());
  stream.insert(stream.end(), wr_event.begin(), wr_event.end());

  size_t consumed = 0;
  mes_feed(engine, stream.data(), stream.size(), &consumed);

  const mes_event_t* event = nullptr;
  ASSERT_EQ(mes_next_event(engine, &event), MES_OK);
  ASSERT_GE(event->after_count, 1u);
  EXPECT_EQ(event->after_columns[0].type, MES_COL_BYTES);
  EXPECT_EQ(event->after_columns[0].str_len, 3u);

  mes_destroy(engine);
}

// ---- JSON column (bytes, not string) ----

TEST(CapiTest, JsonColumnReturnsBytesNotString) {
  mes_engine_t* engine = mes_create();
  ASSERT_NE(engine, nullptr);

  // Build TABLE_MAP with a JSON column (type 0xF5, metadata byte = 4)
  mes::test::EventBuilder tb;
  tb.WriteU48Le(42);  // table_id
  tb.WriteU16Le(0);   // flags
  tb.WriteU8(2);      // db name length
  tb.WriteString("db");
  tb.WriteU8(0);  // null terminator
  tb.WriteU8(1);  // table name length
  tb.WriteString("t");
  tb.WriteU8(0);     // null terminator
  tb.WriteU8(1);     // 1 column
  tb.WriteU8(0xF5);  // JSON
  tb.WriteU8(1);     // metadata length
  tb.WriteU8(4);     // pack_length = 4
  tb.WriteU8(0x01);  // null bitmap
  auto tm_event =
      BuildEvent(static_cast<uint8_t>(mes::BinlogEventType::kTableMapEvent), 1000, 100, tb.Data());

  // Build WRITE_ROWS with a JSON value: 4-byte length prefix + payload
  mes::test::EventBuilder rb;
  rb.WriteU48Le(42);  // table_id
  rb.WriteU16Le(0);   // flags
  rb.WriteU16Le(2);   // var_header_len
  rb.WriteU8(1);      // column_count
  rb.WriteU8(0x01);   // columns_present
  rb.WriteU8(0x00);   // null_bitmap (not null)
  // JSON data: 4-byte LE length prefix followed by payload bytes
  std::vector<uint8_t> json_payload = {0x00, 0x01, 0x00, 0x0C, 0x00, 0x0B, 0x00, 0x01};
  rb.WriteU32Le(static_cast<uint32_t>(json_payload.size()));
  rb.WriteBytes(json_payload);
  auto wr_event =
      BuildEvent(static_cast<uint8_t>(mes::BinlogEventType::kWriteRowsEvent), 1000, 200, rb.Data());

  std::vector<uint8_t> stream;
  stream.insert(stream.end(), tm_event.begin(), tm_event.end());
  stream.insert(stream.end(), wr_event.begin(), wr_event.end());

  size_t consumed = 0;
  ASSERT_EQ(mes_feed(engine, stream.data(), stream.size(), &consumed), MES_OK);
  EXPECT_EQ(consumed, stream.size());

  const mes_event_t* event = nullptr;
  ASSERT_EQ(mes_next_event(engine, &event), MES_OK);
  ASSERT_NE(event, nullptr);
  ASSERT_GE(event->after_count, 1u);
  EXPECT_EQ(event->after_columns[0].type, MES_COL_BYTES);
  EXPECT_EQ(event->after_columns[0].str_len, json_payload.size());
  EXPECT_NE(event->after_columns[0].str_data, nullptr);
  // Verify the actual bytes match
  EXPECT_EQ(memcmp(event->after_columns[0].str_data, json_payload.data(), json_payload.size()), 0);

  mes_destroy(engine);
}

// ---- Parse error propagation ----

TEST(CApi, FeedReturnsParseErrorOnInvalidData) {
  auto* engine = mes_create();

  // Build a 19-byte header with event_length too small (< 23) to trigger kError
  mes::test::EventBuilder b;
  b.WriteU32Le(1000);  // timestamp
  b.WriteU8(0x21);     // type_code (arbitrary)
  b.WriteU32Le(1);     // server_id
  b.WriteU32Le(10);    // event_length = 10 (invalid: less than header + checksum)
  b.WriteU32Le(0);     // next_position
  b.WriteU16Le(0);     // flags
  auto bad_header = b.Data();

  size_t consumed = 0;
  EXPECT_EQ(mes_feed(engine, bad_header.data(), bad_header.size(), &consumed), MES_ERR_PARSE);

  // After reset, engine should recover
  EXPECT_EQ(mes_reset(engine), MES_OK);

  // Verify engine works normally after reset
  auto tm_body = BuildTableMapBody(1, "db", "t");
  auto tm_event =
      BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000, 100, tm_body);
  auto wr_body = BuildWriteRowsBody(1, 42);
  auto wr_event =
      BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1001, 200, wr_body);

  std::vector<uint8_t> stream;
  stream.insert(stream.end(), tm_event.begin(), tm_event.end());
  stream.insert(stream.end(), wr_event.begin(), wr_event.end());

  ASSERT_EQ(mes_feed(engine, stream.data(), stream.size(), &consumed), MES_OK);
  EXPECT_EQ(consumed, stream.size());
  EXPECT_EQ(mes_has_events(engine), 1);

  const mes_event_t* event;
  ASSERT_EQ(mes_next_event(engine, &event), MES_OK);
  EXPECT_EQ(event->type, MES_EVENT_INSERT);
  EXPECT_EQ(event->after_columns[0].int_val, 42);

  mes_destroy(engine);
}

TEST(CApi, FeedReturnsChecksumErrorOnCorruptedEvent) {
  auto* engine = mes_create();
  ASSERT_NE(engine, nullptr);
  auto event = BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000, 100,
                          BuildTableMapBody(1, "db", "t"));
  event[mes::kEventHeaderSize + 1] ^= 0x40;

  size_t consumed = 123;
  EXPECT_EQ(mes_feed(engine, event.data(), event.size(), &consumed), MES_ERR_CHECKSUM);
  EXPECT_EQ(consumed, 0u);
  EXPECT_EQ(mes_has_events(engine), 0);

  mes_destroy(engine);
}

TEST(CApi, FeedProcessingErrorsReturnWithoutReprocessingParsedEvent) {
  // Each invalid event is followed by a normal event in the same buffer.  The
  // C API must return the processing error rather than repeatedly processing
  // the already-parsed invalid event and spinning forever.
  const auto normal_event =
      BuildEvent(static_cast<uint8_t>(BinlogEventType::kQueryEvent), 1001, 200, {});

  struct Case {
    const char* name;
    std::vector<uint8_t> invalid_event;
    mes_error_t expected;
  };

  const auto short_table_map = BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent),
                                          1000, 100, std::vector<uint8_t>(5, 0));
  const auto malformed_table_map = BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent),
                                              1000, 100, std::vector<uint8_t>(6, 0));
  const auto malformed_rotate =
      BuildEvent(static_cast<uint8_t>(BinlogEventType::kRotateEvent), 1000, 100, {});
  const auto short_row = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1000,
                                    100, std::vector<uint8_t>(5, 0));
  const auto row_without_table_map = BuildEvent(
      static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1000, 100, BuildWriteRowsBody(1, 42));
  auto truncated_row_body = BuildWriteRowsBody(1, 42);
  truncated_row_body.pop_back();
  const auto truncated_row = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent),
                                        1000, 100, truncated_row_body);

  const std::vector<Case> cases = {
      {"short table map", short_table_map, MES_ERR_PARSE},
      {"malformed table map", malformed_table_map, MES_ERR_PARSE},
      {"malformed rotate", malformed_rotate, MES_ERR_PARSE},
      {"short row event", short_row, MES_ERR_DECODE_ROW},
      {"row without table map", row_without_table_map, MES_ERR_DECODE_ROW},
      {"truncated row", truncated_row, MES_ERR_DECODE_ROW},
  };

  for (const auto& test_case : cases) {
    SCOPED_TRACE(test_case.name);
    auto* engine = mes_create();
    ASSERT_NE(engine, nullptr);

    std::vector<uint8_t> stream = test_case.invalid_event;
    stream.insert(stream.end(), normal_event.begin(), normal_event.end());
    size_t consumed = 123;
    EXPECT_EQ(mes_feed(engine, stream.data(), stream.size(), &consumed), test_case.expected);
    EXPECT_EQ(consumed, 0u);
    EXPECT_EQ(mes_has_events(engine), 0);

    mes_destroy(engine);
  }
}

TEST(CApi, ChecksumOverrideSupportsNoChecksumStream) {
  EXPECT_EQ(mes_set_checksum_enabled(nullptr, 0), MES_ERR_NULL_ARG);
  auto* engine = mes_create();
  ASSERT_NE(engine, nullptr);
  ASSERT_EQ(mes_set_checksum_enabled(engine, 0), MES_OK);

  auto table_map =
      mes::test::BuildEventNoChecksum(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000,
                                      100, BuildTableMapBody(1, "db", "t"));
  auto write = mes::test::BuildEventNoChecksum(
      static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1001, 200, BuildWriteRowsBody(1, 42));
  std::vector<uint8_t> stream = table_map;
  stream.insert(stream.end(), write.begin(), write.end());

  size_t consumed = 0;
  EXPECT_EQ(mes_feed(engine, stream.data(), stream.size(), &consumed), MES_OK);
  EXPECT_EQ(consumed, stream.size());
  EXPECT_EQ(mes_has_events(engine), 1);
  mes_destroy(engine);
}

// ---- Reset after error recovers ----

TEST(CApi, ResetAfterErrorRecovers) {
  auto* engine = mes_create();

  // Feed invalid data to trigger MES_ERR_PARSE
  mes::test::EventBuilder b;
  b.WriteU32Le(1000);  // timestamp
  b.WriteU8(0x21);     // type_code
  b.WriteU32Le(1);     // server_id
  b.WriteU32Le(10);    // event_length = 10 (invalid)
  b.WriteU32Le(0);     // next_position
  b.WriteU16Le(0);     // flags
  auto bad_header = b.Data();

  size_t consumed = 0;
  EXPECT_EQ(mes_feed(engine, bad_header.data(), bad_header.size(), &consumed), MES_ERR_PARSE);

  // Reset
  EXPECT_EQ(mes_reset(engine), MES_OK);

  // Feed valid data
  auto tm_body = BuildTableMapBody(1, "testdb", "users");
  auto tm_event =
      BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 2000, 100, tm_body);

  auto wr_body = BuildWriteRowsBody(1, 99);
  auto wr_event =
      BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 2001, 200, wr_body);

  std::vector<uint8_t> stream;
  stream.insert(stream.end(), tm_event.begin(), tm_event.end());
  stream.insert(stream.end(), wr_event.begin(), wr_event.end());

  consumed = 0;
  ASSERT_EQ(mes_feed(engine, stream.data(), stream.size(), &consumed), MES_OK);
  EXPECT_EQ(consumed, stream.size());

  EXPECT_EQ(mes_has_events(engine), 1);

  const mes_event_t* event;
  ASSERT_EQ(mes_next_event(engine, &event), MES_OK);
  EXPECT_EQ(event->type, MES_EVENT_INSERT);
  EXPECT_STREQ(event->database, "testdb");
  EXPECT_STREQ(event->table, "users");
  ASSERT_EQ(event->after_count, 1u);
  EXPECT_EQ(event->after_columns[0].int_val, 99);

  mes_destroy(engine);
}

TEST(CApi, FeedReturnsDecodeErrorForTruncatedRowEvent) {
  auto* engine = mes_create();

  auto tm_body = BuildTableMapBody(1, "db", "t");
  auto tm_event =
      BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000, 100, tm_body);

  auto wr_body = BuildWriteRowsBody(1, 42);
  wr_body.pop_back();
  auto wr_event =
      BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1001, 200, wr_body);

  std::vector<uint8_t> stream;
  stream.insert(stream.end(), tm_event.begin(), tm_event.end());
  stream.insert(stream.end(), wr_event.begin(), wr_event.end());

  size_t consumed = 123;
  EXPECT_EQ(mes_feed(engine, stream.data(), stream.size(), &consumed), MES_ERR_DECODE_ROW);
  EXPECT_EQ(consumed, 0u);
  EXPECT_EQ(mes_has_events(engine), 0);

  EXPECT_EQ(mes_reset(engine), MES_OK);

  auto valid_wr_event = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1002,
                                   300, BuildWriteRowsBody(1, 99));
  stream.clear();
  stream.insert(stream.end(), tm_event.begin(), tm_event.end());
  stream.insert(stream.end(), valid_wr_event.begin(), valid_wr_event.end());

  ASSERT_EQ(mes_feed(engine, stream.data(), stream.size(), &consumed), MES_OK);
  EXPECT_EQ(consumed, stream.size());

  const mes_event_t* event = nullptr;
  ASSERT_EQ(mes_next_event(engine, &event), MES_OK);
  EXPECT_EQ(event->after_columns[0].int_val, 99);

  mes_destroy(engine);
}

// ---- mes_set_max_queue_size backpressure ----

TEST(CApi, SetMaxQueueSizeBackpressure) {
  auto* engine = mes_create();

  // Set max queue size to 1
  EXPECT_EQ(mes_set_max_queue_size(engine, 1), MES_OK);

  // Build TABLE_MAP + 2 WRITE_ROWS events
  auto tm_body = BuildTableMapBody(1, "db", "t");
  auto tm_event =
      BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000, 100, tm_body);

  auto wr1 = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1001, 200,
                        BuildWriteRowsBody(1, 10));
  auto wr2 = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1002, 300,
                        BuildWriteRowsBody(1, 20));

  std::vector<uint8_t> stream;
  stream.insert(stream.end(), tm_event.begin(), tm_event.end());
  stream.insert(stream.end(), wr1.begin(), wr1.end());
  stream.insert(stream.end(), wr2.begin(), wr2.end());

  // Feed the entire stream; with max_queue_size=1, the engine should stop
  // after producing the first event (backpressure)
  size_t consumed = 0;
  ASSERT_EQ(mes_feed(engine, stream.data(), stream.size(), &consumed), MES_OK);
  // Should have consumed TABLE_MAP + first WRITE_ROWS but not the second
  EXPECT_LT(consumed, stream.size());
  EXPECT_EQ(mes_has_events(engine), 1);

  // Drain the first event
  const mes_event_t* event;
  ASSERT_EQ(mes_next_event(engine, &event), MES_OK);
  EXPECT_EQ(event->type, MES_EVENT_INSERT);
  EXPECT_EQ(event->after_columns[0].int_val, 10);

  // Feed the remaining data
  size_t remaining = stream.size() - consumed;
  size_t consumed2 = 0;
  ASSERT_EQ(mes_feed(engine, stream.data() + consumed, remaining, &consumed2), MES_OK);
  EXPECT_GT(consumed2, 0u);

  // Should now have the second event
  ASSERT_EQ(mes_next_event(engine, &event), MES_OK);
  EXPECT_EQ(event->type, MES_EVENT_INSERT);
  EXPECT_EQ(event->after_columns[0].int_val, 20);

  mes_destroy(engine);
}

TEST(CApi, SetMaxQueueSizeNullEngine) {
  EXPECT_EQ(mes_set_max_queue_size(nullptr, 10), MES_ERR_NULL_ARG);
}

TEST(CApi, SetMaxQueueSizeZeroRestoresBoundedDefault) {
  auto* engine = mes_create();

  // Setting max_queue_size to 0 restores the bounded default.
  EXPECT_EQ(mes_set_max_queue_size(engine, 0), MES_OK);

  // Build TABLE_MAP + 2 WRITE_ROWS events
  auto tm_body = BuildTableMapBody(1, "db", "t");
  auto tm_event =
      BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000, 100, tm_body);

  auto wr1 = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1001, 200,
                        BuildWriteRowsBody(1, 10));
  auto wr2 = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1002, 300,
                        BuildWriteRowsBody(1, 20));

  std::vector<uint8_t> stream;
  stream.insert(stream.end(), tm_event.begin(), tm_event.end());
  stream.insert(stream.end(), wr1.begin(), wr1.end());
  stream.insert(stream.end(), wr2.begin(), wr2.end());

  size_t consumed = 0;
  ASSERT_EQ(mes_feed(engine, stream.data(), stream.size(), &consumed), MES_OK);
  // The default is much larger than this fixture, so all data is consumed.
  EXPECT_EQ(consumed, stream.size());

  // Both events should be available
  const mes_event_t* event;
  ASSERT_EQ(mes_next_event(engine, &event), MES_OK);
  EXPECT_EQ(event->after_columns[0].int_val, 10);
  ASSERT_EQ(mes_next_event(engine, &event), MES_OK);
  EXPECT_EQ(event->after_columns[0].int_val, 20);

  mes_destroy(engine);
}

// ---- mes_set_max_event_size / mes_get_max_event_size ----

TEST(CApi, MaxEventSizeDefault) {
  auto* engine = mes_create();
  // Default is 64 MiB per the header documentation.
  EXPECT_EQ(mes_get_max_event_size(engine), 64u * 1024u * 1024u);
  mes_destroy(engine);
}

TEST(CApi, MaxEventSizeGetNull) { EXPECT_EQ(mes_get_max_event_size(nullptr), 0u); }

TEST(CApi, MaxEventSizeSetNull) {
  EXPECT_EQ(mes_set_max_event_size(nullptr, 1024), MES_ERR_NULL_ARG);
}

TEST(CApi, MaxEventSizeRoundTrip) {
  auto* engine = mes_create();
  EXPECT_EQ(mes_set_max_event_size(engine, 2u * 1024u * 1024u), MES_OK);
  EXPECT_EQ(mes_get_max_event_size(engine), 2u * 1024u * 1024u);
  mes_destroy(engine);
}

TEST(CApi, MaxEventSizeClampsToAbsoluteMax) {
  auto* engine = mes_create();
  // Any value above 1 GiB is clamped to 1 GiB.
  EXPECT_EQ(mes_set_max_event_size(engine, UINT32_MAX), MES_OK);
  EXPECT_EQ(mes_get_max_event_size(engine), 1024u * 1024u * 1024u);
  mes_destroy(engine);
}

TEST(CApi, MaxEventSizeZeroMeansNoLimit) {
  auto* engine = mes_create();
  // 0 means "no limit": resolves to the 1 GiB hard cap rather than
  // rejecting all events.
  EXPECT_EQ(mes_set_max_event_size(engine, 0), MES_OK);
  EXPECT_EQ(mes_get_max_event_size(engine), 1024u * 1024u * 1024u);
  mes_destroy(engine);
}

TEST(CApi, ClientMaxEventSizeDefaultAndRoundTrip) {
  mes_client_t* client = mes_client_create();
  ASSERT_NE(client, nullptr);
  EXPECT_EQ(mes_client_is_connected(client), 0);
  EXPECT_EQ(mes_client_is_streaming(client), 0);
  EXPECT_EQ(mes_client_get_max_event_size(client), 32u * 1024u * 1024u);
  EXPECT_EQ(mes_client_set_max_event_size(client, 128u * 1024u * 1024u), MES_OK);
  EXPECT_EQ(mes_client_get_max_event_size(client), 128u * 1024u * 1024u);
  EXPECT_EQ(mes_client_set_max_event_size(client, 0), MES_OK);
  EXPECT_EQ(mes_client_get_max_event_size(client), 1024u * 1024u * 1024u);
  mes_client_destroy(client);
}

TEST(CApi, ClientMaxQueueBytesDefaultAndRoundTrip) {
  mes_client_t* client = mes_client_create();
  ASSERT_NE(client, nullptr);
  EXPECT_EQ(mes_client_get_max_queue_bytes(client), 48u * 1024u * 1024u);
  EXPECT_EQ(mes_client_queued_bytes(client), 0u);
  EXPECT_EQ(mes_client_set_max_queue_bytes(client, 512u * 1024u * 1024u), MES_OK);
  EXPECT_EQ(mes_client_get_max_queue_bytes(client), 512u * 1024u * 1024u);
  EXPECT_EQ(mes_client_set_max_queue_bytes(client, 0), MES_OK);
  EXPECT_EQ(mes_client_get_max_queue_bytes(client), 48u * 1024u * 1024u);
  mes_client_destroy(client);
}

// A caller holding a valid handle is told why the call was refused: the error
// code alone does not distinguish a missing output buffer from a zero capacity.
TEST(CApi, ClientPollBatchValidatesOutputArguments) {
  mes_poll_result_t results[2]{};
  size_t count = 0;
  mes_client_t* client = mes_client_create();
  ASSERT_NE(client, nullptr);
  EXPECT_EQ(mes_client_poll_batch(client, nullptr, 2, &count), MES_ERR_NULL_ARG);
  EXPECT_STREQ(mes_client_last_error(client), "results and result_count must not be NULL");
  EXPECT_EQ(mes_client_poll_batch(client, results, 2, nullptr), MES_ERR_NULL_ARG);
  EXPECT_STREQ(mes_client_last_error(client), "results and result_count must not be NULL");
  EXPECT_EQ(mes_client_poll_batch(client, results, 0, &count), MES_ERR_INVALID_ARG);
  EXPECT_STREQ(mes_client_last_error(client), "capacity must be at least 1");
  mes_client_destroy(client);
}

// Each sub-case below is a configuration valid in every respect other than the
// condition it tests, so the rejection cannot be satisfied by an unrelated
// defect in the fixture: a zero server_id, for instance, is refused earlier and
// would mask whether the start position is checked at all. Every message is
// asserted for the same reason -- a bare MES_ERR_INVALID_ARG does not say which
// branch fired.
TEST(CApi, ClientRejectsInvalidStartPositionConfigurationBeforeConnecting) {
  mes_client_t* client = mes_client_create();
  ASSERT_NE(client, nullptr);

  mes_client_config_t config{};
  config.server_id = 1;
  config.start_position_mode = static_cast<mes_start_position_mode_t>(99);
  EXPECT_EQ(mes_client_connect(client, &config), MES_ERR_INVALID_ARG);
  EXPECT_STREQ(mes_client_last_error(client),
               "start_position_mode must be current, gtid or position");

  const char* const kPositionRequired =
      "binlog_file and binlog_position (4 through UINT32_MAX) are required";

  config = {};
  config.server_id = 1;
  config.start_position_mode = MES_START_AT_POSITION;
  config.binlog_file = "binlog.000001";
  config.binlog_position = 3;
  EXPECT_EQ(mes_client_connect(client, &config), MES_ERR_INVALID_ARG);
  EXPECT_STREQ(mes_client_last_error(client), kPositionRequired);

  config.binlog_position = 0x100000000ull;
  EXPECT_EQ(mes_client_connect(client, &config), MES_ERR_INVALID_ARG);
  EXPECT_STREQ(mes_client_last_error(client), kPositionRequired);

  config.binlog_file = nullptr;
  config.binlog_position = 4;
  EXPECT_EQ(mes_client_connect(client, &config), MES_ERR_INVALID_ARG);
  EXPECT_STREQ(mes_client_last_error(client), kPositionRequired);
  mes_client_destroy(client);
}

TEST(CApi, ClientLastErrorDescribesTheCallThatFailedMostRecently) {
  mes_client_t* client = mes_client_create();
  ASSERT_NE(client, nullptr);

  mes_client_config_t config{};
  config.server_id = 1;
  config.start_position_mode = static_cast<mes_start_position_mode_t>(99);
  EXPECT_EQ(mes_client_connect(client, &config), MES_ERR_INVALID_ARG);
  EXPECT_STREQ(mes_client_last_error(client),
               "start_position_mode must be current, gtid or position");

  // A rejection decided at the C ABI boundary must not outlive the call it
  // describes, or the next failure is reported with the wrong reason.
  EXPECT_EQ(mes_client_start(client), MES_ERR_DISCONNECTED);
  EXPECT_STREQ(mes_client_last_error(client), "Not connected");
  mes_client_destroy(client);
}

// ---- Client entry-point inventory ----

/**
 * @brief One mes_client_* entry point and the contracts a caller may rely on.
 *
 * Every entry point declared in mes.h appears exactly once in
 * kClientEntryPoints below, and both the NULL-handle sweep and the state matrix
 * are generated from that one list. An entry point added to the header without
 * being listed therefore fails
 * ClientEntryPointInventoryCoversEveryHeaderDeclaration instead of shipping
 * with no coverage at all.
 */
struct ClientEntryPoint {
  const char* name;
  /** Asserts the documented behavior for a NULL client handle. */
  void (*reject_null_handle)();
  /**
   * Asserts the documented behavior on a client that is neither connected nor
   * streaming. Probes run against a shared client, so a probe must leave the
   * client in the state it received: nothing here may make it connected, and
   * anything it configures it restores.
   */
  void (*probe_unconnected)(mes_client_t* client);
};

/** @brief A config rejected at the C ABI boundary, before any socket is used. */
mes_client_config_t RejectedBeforeAnyIo() {
  mes_client_config_t config{};
  config.server_id = 1;
  config.start_position_mode = static_cast<mes_start_position_mode_t>(99);
  return config;
}

const ClientEntryPoint kClientEntryPoints[] = {
    {"mes_client_create",
     [] {
       // Creation takes no handle. Its contract is that a successful create
       // yields an independent, destroyable instance.
       mes_client_t* client = mes_client_create();
       ASSERT_NE(client, nullptr);
       mes_client_destroy(client);
     },
     [](mes_client_t* client) {
       mes_client_t* other = mes_client_create();
       ASSERT_NE(other, nullptr);
       EXPECT_NE(other, client);
       mes_client_destroy(other);
     }},
    {"mes_client_destroy", [] { mes_client_destroy(nullptr); },
     [](mes_client_t* client) {
       // Destroying an unrelated instance leaves this one usable.
       mes_client_destroy(mes_client_create());
       EXPECT_EQ(mes_client_is_connected(client), 0);
     }},
    {"mes_client_connect",
     [] {
       mes_client_config_t config = RejectedBeforeAnyIo();
       EXPECT_EQ(mes_client_connect(nullptr, &config), MES_ERR_NULL_ARG);
     },
     [](mes_client_t* client) {
       EXPECT_EQ(mes_client_connect(client, nullptr), MES_ERR_NULL_ARG);
       EXPECT_STRNE(mes_client_last_error(client), "");
       mes_client_config_t config = RejectedBeforeAnyIo();
       EXPECT_EQ(mes_client_connect(client, &config), MES_ERR_INVALID_ARG);
       EXPECT_STRNE(mes_client_last_error(client), "");
     }},
    {"mes_client_start", [] { EXPECT_EQ(mes_client_start(nullptr), MES_ERR_NULL_ARG); },
     [](mes_client_t* client) {
       EXPECT_EQ(mes_client_start(client), MES_ERR_DISCONNECTED);
       EXPECT_EQ(mes_client_is_streaming(client), 0);
     }},
    {"mes_client_poll",
     [] {
       const mes_poll_result_t result = mes_client_poll(nullptr);
       EXPECT_EQ(result.error, MES_ERR_NULL_ARG);
       EXPECT_EQ(result.data, nullptr);
       EXPECT_EQ(result.size, 0u);
       EXPECT_EQ(result.is_heartbeat, 0);
     },
     [](mes_client_t* client) {
       const mes_poll_result_t result = mes_client_poll(client);
       EXPECT_EQ(result.error, MES_ERR_DISCONNECTED);
       EXPECT_EQ(result.data, nullptr);
       EXPECT_EQ(result.is_heartbeat, 0);
     }},
    {"mes_client_poll_batch",
     [] {
       mes_poll_result_t results[2]{};
       size_t count = 0;
       EXPECT_EQ(mes_client_poll_batch(nullptr, results, 2, &count), MES_ERR_NULL_ARG);
     },
     [](mes_client_t* client) {
       mes_poll_result_t results[2]{};
       size_t count = 0;
       // A well formed batch call reports the terminal state in the result, not
       // in its return value.
       EXPECT_EQ(mes_client_poll_batch(client, results, 2, &count), MES_OK);
       ASSERT_EQ(count, 1u);
       EXPECT_EQ(results[0].error, MES_ERR_DISCONNECTED);
     }},
    {"mes_client_stop", [] { mes_client_stop(nullptr); },
     [](mes_client_t* client) {
       mes_client_stop(client);
       mes_client_stop(client);
       EXPECT_EQ(mes_client_is_streaming(client), 0);
       EXPECT_EQ(mes_client_is_connected(client), 0);
     }},
    {"mes_client_disconnect", [] { mes_client_disconnect(nullptr); },
     [](mes_client_t* client) {
       mes_client_disconnect(client);
       mes_client_disconnect(client);
       EXPECT_EQ(mes_client_is_connected(client), 0);
     }},
    {"mes_client_is_connected", [] { EXPECT_EQ(mes_client_is_connected(nullptr), 0); },
     [](mes_client_t* client) { EXPECT_EQ(mes_client_is_connected(client), 0); }},
    {"mes_client_is_streaming", [] { EXPECT_EQ(mes_client_is_streaming(nullptr), 0); },
     [](mes_client_t* client) { EXPECT_EQ(mes_client_is_streaming(client), 0); }},
    {"mes_client_flavor", [] { EXPECT_EQ(mes_client_flavor(nullptr), MES_SERVER_FLAVOR_MYSQL); },
     [](mes_client_t* client) {
       // No handshake has completed, so the flavor is still the default.
       EXPECT_EQ(mes_client_flavor(client), MES_SERVER_FLAVOR_MYSQL);
     }},
    {"mes_client_last_error", [] { EXPECT_STREQ(mes_client_last_error(nullptr), ""); },
     [](mes_client_t* client) {
       // The message depends on what this client has already been asked to do;
       // what every state owes the caller is a readable string.
       EXPECT_NE(mes_client_last_error(client), nullptr);
     }},
    {"mes_client_current_gtid", [] { EXPECT_STREQ(mes_client_current_gtid(nullptr), ""); },
     [](mes_client_t* client) { EXPECT_STREQ(mes_client_current_gtid(client), ""); }},
    {"mes_client_checksum_enabled", [] { EXPECT_EQ(mes_client_checksum_enabled(nullptr), 0); },
     [](mes_client_t* client) {
       // CRC32 is assumed until the server reports otherwise during start.
       EXPECT_EQ(mes_client_checksum_enabled(client), 1);
     }},
    {"mes_client_set_max_event_size",
     [] { EXPECT_EQ(mes_client_set_max_event_size(nullptr, 1024), MES_ERR_NULL_ARG); },
     [](mes_client_t* client) {
       const uint32_t previous = mes_client_get_max_event_size(client);
       EXPECT_EQ(mes_client_set_max_event_size(client, 128u * 1024u * 1024u), MES_OK);
       EXPECT_EQ(mes_client_get_max_event_size(client), 128u * 1024u * 1024u);
       EXPECT_EQ(mes_client_set_max_event_size(client, previous), MES_OK);
     }},
    {"mes_client_get_max_event_size", [] { EXPECT_EQ(mes_client_get_max_event_size(nullptr), 0u); },
     [](mes_client_t* client) {
       EXPECT_EQ(mes_client_get_max_event_size(client), 32u * 1024u * 1024u);
     }},
    {"mes_client_set_max_queue_bytes",
     [] { EXPECT_EQ(mes_client_set_max_queue_bytes(nullptr, 1024), MES_ERR_NULL_ARG); },
     [](mes_client_t* client) {
       const size_t previous = mes_client_get_max_queue_bytes(client);
       EXPECT_EQ(mes_client_set_max_queue_bytes(client, 512u * 1024u * 1024u), MES_OK);
       EXPECT_EQ(mes_client_get_max_queue_bytes(client), 512u * 1024u * 1024u);
       EXPECT_EQ(mes_client_set_max_queue_bytes(client, previous), MES_OK);
     }},
    {"mes_client_get_max_queue_bytes",
     [] { EXPECT_EQ(mes_client_get_max_queue_bytes(nullptr), 0u); },
     [](mes_client_t* client) {
       EXPECT_EQ(mes_client_get_max_queue_bytes(client), MES_DEFAULT_QUEUE_BYTES);
     }},
    {"mes_client_queued_bytes", [] { EXPECT_EQ(mes_client_queued_bytes(nullptr), 0u); },
     [](mes_client_t* client) { EXPECT_EQ(mes_client_queued_bytes(client), 0u); }},
    {"mes_client_crc_errors", [] { EXPECT_EQ(mes_client_crc_errors(nullptr), 0u); },
     [](mes_client_t* client) { EXPECT_EQ(mes_client_crc_errors(client), 0u); }},
};

/** @brief Client states reachable from the C ABI without a live server. */
enum class ClientState {
  kFresh,
  kConnectFailed,
  kStopped,
  kStoppedTwice,
  kDisconnected,
};

const char* ClientStateName(ClientState state) {
  switch (state) {
    case ClientState::kFresh:
      return "never connected";
    case ClientState::kConnectFailed:
      return "failed to connect";
    case ClientState::kStopped:
      return "stopped";
    case ClientState::kStoppedTwice:
      return "stopped twice";
    case ClientState::kDisconnected:
      return "disconnected";
  }
  return "unknown";
}

/**
 * @brief Drive a freshly created client into @p state.
 *
 * Every state here is "not connected, not streaming": the connected and
 * streaming states need a real handshake and are exercised in the E2E tier.
 */
void DriveClientToState(mes_client_t* client, ClientState state) {
  switch (state) {
    case ClientState::kFresh:
      break;
    case ClientState::kConnectFailed: {
      mes_client_config_t config{};
      // Nothing listens on loopback port 1, so the attempt fails on the socket
      // without needing a server, and the timeout bounds the wait.
      config.host = "127.0.0.1";
      config.port = 1;
      config.user = "repl";
      config.server_id = 1;
      config.connect_timeout_s = 1;
      config.read_timeout_s = 1;
      EXPECT_NE(mes_client_connect(client, &config), MES_OK);
      break;
    }
    case ClientState::kStopped:
      mes_client_stop(client);
      break;
    case ClientState::kStoppedTwice:
      mes_client_stop(client);
      mes_client_stop(client);
      break;
    case ClientState::kDisconnected:
      mes_client_disconnect(client);
      break;
  }
}

/** @brief Name declared by a `MES_API <return type> name(` line, else empty. */
std::string DeclaredFunctionName(const std::string& line) {
  if (line.rfind("MES_API ", 0) != 0) return "";
  const size_t paren = line.find('(');
  if (paren == std::string::npos) return "";
  size_t begin = paren;
  while (begin > 0) {
    const auto ch = static_cast<unsigned char>(line[begin - 1]);
    if (std::isalnum(ch) == 0 && ch != '_') break;
    --begin;
  }
  return line.substr(begin, paren - begin);
}

/** @brief The public header, located relative to this test's own source file. */
std::string PublicHeaderPath() {
  const std::string self = __FILE__;
  const size_t slash = self.find_last_of("/\\");
  const std::string dir = slash == std::string::npos ? std::string(".") : self.substr(0, slash);
  return dir + "/../include/mes.h";
}

TEST(CApi, ClientEntryPointInventoryCoversEveryHeaderDeclaration) {
  const std::string header_path = PublicHeaderPath();
  std::ifstream header(header_path);
  ASSERT_TRUE(header.is_open()) << "cannot read the public header at " << header_path;

  std::vector<std::string> declared;
  std::string line;
  while (std::getline(header, line)) {
    const std::string name = DeclaredFunctionName(line);
    if (name.rfind("mes_client_", 0) == 0) declared.push_back(name);
  }
  ASSERT_FALSE(declared.empty()) << "no mes_client_* declarations found in " << header_path;

  std::vector<std::string> listed;
  for (const ClientEntryPoint& entry : kClientEntryPoints) listed.emplace_back(entry.name);
  std::sort(declared.begin(), declared.end());
  std::sort(listed.begin(), listed.end());
  // Equality covers both directions: an unlisted entry point and a listed name
  // the header no longer declares.
  EXPECT_EQ(listed, declared);
  EXPECT_EQ(std::size(kClientEntryPoints), declared.size());
}

TEST(CApi, ClientEntryPointsRejectNullHandle) {
  for (const ClientEntryPoint& entry : kClientEntryPoints) {
    SCOPED_TRACE(entry.name);
    entry.reject_null_handle();
  }
}

TEST(CApi, ClientEntryPointsHonorTheirContractInEveryUnconnectedState) {
  const ClientState states[] = {
      ClientState::kFresh,        ClientState::kConnectFailed, ClientState::kStopped,
      ClientState::kStoppedTwice, ClientState::kDisconnected,
  };
  for (ClientState state : states) {
    mes_client_t* client = mes_client_create();
    ASSERT_NE(client, nullptr);
    DriveClientToState(client, state);
    for (const ClientEntryPoint& entry : kClientEntryPoints) {
      SCOPED_TRACE(std::string(entry.name) + " on a client that " + ClientStateName(state));
      entry.probe_unconnected(client);
    }
    mes_client_destroy(client);
  }
}

TEST(CApi, ClientStopIsCallableFromOtherThreadsAndIsIdempotent) {
  mes_client_t* client = mes_client_create();
  ASSERT_NE(client, nullptr);

  // Stop is the one entry point the C ABI allows on a thread other than the
  // owner. Several threads request it, twice each, while the owner keeps polling
  // the same client, so losing the internal serialization shows up here rather
  // than in a binding's finalizer.
  constexpr int kStoppers = 4;
  std::atomic<int> ready{0};
  std::vector<std::thread> stoppers;
  stoppers.reserve(kStoppers);
  for (int i = 0; i < kStoppers; ++i) {
    stoppers.emplace_back([client, &ready] {
      ready.fetch_add(1, std::memory_order_release);
      mes_client_stop(client);
      mes_client_stop(client);
    });
  }
  while (ready.load(std::memory_order_acquire) < kStoppers) std::this_thread::yield();

  // A client that was never started has nothing to drain, so each poll returns
  // the terminal state instead of blocking on the queue.
  for (int i = 0; i < 100; ++i) {
    EXPECT_EQ(mes_client_poll(client).error, MES_ERR_DISCONNECTED);
  }
  for (std::thread& stopper : stoppers) stopper.join();

  EXPECT_EQ(mes_client_is_streaming(client), 0);
  EXPECT_EQ(mes_client_is_connected(client), 0);
  EXPECT_STRNE(mes_client_last_error(client), "");
  mes_client_destroy(client);
}

// ---- Filter setters ----

// Feed a TABLE_MAP + WRITE_ROWS pair for one table and report whether the
// engine surfaced the resulting INSERT. Each call needs its own table_id so
// the registry entry of a previous call is not reused.
bool FeedInsertFor(mes_engine_t* engine, uint64_t table_id, const char* database,
                   const char* table) {
  auto tm_event = BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000, 100,
                             BuildTableMapBody(table_id, database, table));
  auto wr_event = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1001, 200,
                             BuildWriteRowsBody(table_id, 1));
  std::vector<uint8_t> stream;
  stream.insert(stream.end(), tm_event.begin(), tm_event.end());
  stream.insert(stream.end(), wr_event.begin(), wr_event.end());

  size_t consumed = 0;
  EXPECT_EQ(mes_feed(engine, stream.data(), stream.size(), &consumed), MES_OK);
  EXPECT_EQ(consumed, stream.size());
  const mes_event_t* event = nullptr;
  return mes_next_event(engine, &event) == MES_OK;
}

TEST(CApi, SetIncludeDatabasesRejectsNullElementAndKeepsFilter) {
  auto* engine = mes_create();
  const char* keep[] = {"keepdb"};
  ASSERT_EQ(mes_set_include_databases(engine, keep, 1), MES_OK);

  const char* all_null[] = {nullptr};
  EXPECT_EQ(mes_set_include_databases(engine, all_null, 1), MES_ERR_NULL_ARG);
  const char* partial[] = {"otherdb", nullptr};
  EXPECT_EQ(mes_set_include_databases(engine, partial, 2), MES_ERR_NULL_ARG);

  // Neither rejected call may have cleared or widened the installed filter.
  EXPECT_FALSE(FeedInsertFor(engine, 1, "otherdb", "t"));
  EXPECT_TRUE(FeedInsertFor(engine, 2, "keepdb", "t"));
  mes_destroy(engine);
}

TEST(CApi, SetIncludeTablesRejectsNullElementAndKeepsFilter) {
  auto* engine = mes_create();
  const char* keep[] = {"keepdb.keep"};
  ASSERT_EQ(mes_set_include_tables(engine, keep, 1), MES_OK);

  const char* all_null[] = {nullptr};
  EXPECT_EQ(mes_set_include_tables(engine, all_null, 1), MES_ERR_NULL_ARG);
  const char* partial[] = {"keepdb.other", nullptr};
  EXPECT_EQ(mes_set_include_tables(engine, partial, 2), MES_ERR_NULL_ARG);

  EXPECT_FALSE(FeedInsertFor(engine, 1, "keepdb", "other"));
  EXPECT_TRUE(FeedInsertFor(engine, 2, "keepdb", "keep"));
  mes_destroy(engine);
}

TEST(CApi, SetExcludeTablesRejectsNullElementAndKeepsFilter) {
  auto* engine = mes_create();
  const char* drop[] = {"skipdb.skip"};
  ASSERT_EQ(mes_set_exclude_tables(engine, drop, 1), MES_OK);

  const char* all_null[] = {nullptr};
  EXPECT_EQ(mes_set_exclude_tables(engine, all_null, 1), MES_ERR_NULL_ARG);
  const char* partial[] = {"skipdb.other", nullptr};
  EXPECT_EQ(mes_set_exclude_tables(engine, partial, 2), MES_ERR_NULL_ARG);

  EXPECT_FALSE(FeedInsertFor(engine, 1, "skipdb", "skip"));
  EXPECT_TRUE(FeedInsertFor(engine, 2, "skipdb", "other"));
  mes_destroy(engine);
}

TEST(CApi, FilterSettersAcceptEmptyArrays) {
  auto* engine = mes_create();
  EXPECT_EQ(mes_set_include_databases(engine, nullptr, 0), MES_OK);
  EXPECT_EQ(mes_set_include_tables(engine, nullptr, 0), MES_OK);
  EXPECT_EQ(mes_set_exclude_tables(engine, nullptr, 0), MES_OK);
  EXPECT_TRUE(FeedInsertFor(engine, 1, "anydb", "t"));
  mes_destroy(engine);
}

TEST(CApi, FilterSettersRejectNullArrayWithNonZeroCount) {
  auto* engine = mes_create();
  EXPECT_EQ(mes_set_include_databases(engine, nullptr, 1), MES_ERR_NULL_ARG);
  EXPECT_EQ(mes_set_include_tables(engine, nullptr, 1), MES_ERR_NULL_ARG);
  EXPECT_EQ(mes_set_exclude_tables(engine, nullptr, 1), MES_ERR_NULL_ARG);
  EXPECT_EQ(mes_set_include_databases(nullptr, nullptr, 0), MES_ERR_NULL_ARG);
  EXPECT_EQ(mes_set_include_tables(nullptr, nullptr, 0), MES_ERR_NULL_ARG);
  EXPECT_EQ(mes_set_exclude_tables(nullptr, nullptr, 0), MES_ERR_NULL_ARG);
  mes_destroy(engine);
}

TEST(CApi, ErrorStringCoversEveryEnumerator) {
  // Every mes_error_t declared in mes.h, in declaration order. A new enumerator
  // without a case in mes_error_string() silently degrades to "unknown error",
  // which callers cannot distinguish from a corrupt code.
  struct ErrorStringCase {
    mes_error_t code;
    const char* text;
  };
  static constexpr ErrorStringCase kCases[] = {
      {MES_OK, "success"},
      {MES_ERR_NULL_ARG, "null argument"},
      {MES_ERR_INVALID_ARG, "invalid argument"},
      {MES_ERR_INTERNAL, "internal error"},
      {MES_ERR_PARSE, "parse error"},
      {MES_ERR_CHECKSUM, "checksum mismatch"},
      {MES_ERR_DECODE, "decode error"},
      {MES_ERR_DECODE_COLUMN, "column decode error"},
      {MES_ERR_DECODE_ROW, "row decode error"},
      {MES_ERR_NO_EVENT, "no event available"},
      {MES_ERR_QUEUE_FULL, "queue full"},
      {MES_ERR_CONNECT, "connection error"},
      {MES_ERR_AUTH, "authentication error"},
      {MES_ERR_VALIDATION, "validation error"},
      {MES_ERR_STREAM, "stream error"},
      {MES_ERR_DISCONNECTED, "disconnected"},
      {MES_ERR_GTID_PURGED, "requested GTID position has been purged"},
      {MES_ERR_GTID_TAGGED_UNSUPPORTED, "legacy tagged GTID error"},
  };
  ASSERT_EQ(std::size(kCases), 18u);

  for (const auto& c : kCases) {
    const char* text = mes_error_string(c.code);
    ASSERT_NE(text, nullptr) << "code " << static_cast<int>(c.code);
    EXPECT_STREQ(text, c.text) << "code " << static_cast<int>(c.code);
    EXPECT_STRNE(text, "unknown error") << "code " << static_cast<int>(c.code);
    // The returned pointer is static storage, so it stays valid and stable
    // across calls; bindings copy it lazily.
    EXPECT_EQ(text, mes_error_string(c.code)) << "code " << static_cast<int>(c.code);
  }

  // Distinctness: no two enumerators share a message, so an error string
  // identifies its code.
  for (size_t i = 0; i < std::size(kCases); ++i) {
    for (size_t j = i + 1; j < std::size(kCases); ++j) {
      EXPECT_STRNE(kCases[i].text, kCases[j].text) << "codes " << static_cast<int>(kCases[i].code)
                                                   << " and " << static_cast<int>(kCases[j].code);
    }
  }

  EXPECT_STREQ(mes_error_string(static_cast<mes_error_t>(999)), "unknown error");
}

}  // namespace
