// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>

#include <cstring>
#include <string>
#include <string_view>
#include <vector>

#include "binary_util.h"
#include "cdc_engine.h"
#include "client/metadata_fetcher.h"
#include "crc32.h"
#include "event_header.h"
#include "logger.h"
#include "test_helpers.h"

namespace mes {
namespace {

using test::BuildDeleteRowsBody;
using test::BuildEvent;
using test::BuildEventNoChecksum;
using test::BuildQueryEventBody;
using test::BuildRotateBody;
using test::BuildTableMapBody;
using test::BuildUpdateRowsBody;
using test::BuildWriteRowsBody;

int g_include_filter_warning_count = 0;
std::string g_include_filter_warning_message;

void CaptureIncludeFilterWarning(mes_log_level_t level, const char* message, void*) {
  if (level != MES_LOG_WARN || message == nullptr) return;
  const std::string_view text(message);
  if (text.find("event=include_filter_matched_nothing") == std::string_view::npos) return;
  ++g_include_filter_warning_count;
  g_include_filter_warning_message = text;
}

std::vector<uint8_t> DecodeHexFixture(const char* hex) {
  std::vector<uint8_t> bytes;
  int high_nibble = -1;
  for (const char* p = hex; *p != '\0'; ++p) {
    const char ch = *p;
    if (ch == ' ' || ch == '\n') continue;
    const int value = ch >= '0' && ch <= '9'   ? ch - '0'
                      : ch >= 'a' && ch <= 'f' ? ch - 'a' + 10
                      : ch >= 'A' && ch <= 'F' ? ch - 'A' + 10
                                               : -1;
    EXPECT_NE(value, -1) << "invalid fixture hex";
    if (value < 0) return {};
    if (high_nibble < 0) {
      high_nibble = value;
    } else {
      bytes.push_back(static_cast<uint8_t>((high_nibble << 4) | value));
      high_nibble = -1;
    }
  }
  EXPECT_EQ(high_nibble, -1) << "odd fixture hex length";
  return bytes;
}

// Extracted byte-for-byte from MySQL 8.4.10's mysql-bin.000011 on
// 2026-07-29. The captured transaction inserts every DATETIME2, TIMESTAMP2,
// and negative TIME2 precision (0 through 6); it is deliberately not built
// with test::BuildEvent or the temporal test encoder.
constexpr char kMySql84TemporalGolden[] = R"hex(
66a5696a1302000000730000009a0500000000a000000000000100086d65735f74657374001561756469745f74656d706f72616c5f676f6c64656e00160312121212121212111111111111111313131313131315000102030405060001020304050600010203040506feff3f010100
d077b03166a5696a1e02000000a40000003e0600000000a000000000000100020016ffffff0000000100000099b382000099b38200003299b38200006399b382000004ce99b3820000162e99b382000001e23a99b38200000f423f665a6480665a648032665a648063665a648004ce665a6480162e665a648001e23a665a64800f423f7f37487f3747ce7f37479d7f3747fb327f3747e9d27f3747fe1dc67f3747f0bdc1e04f567c
)hex";

TEST(CdcEngineGoldenFixtureTest, MySql84TemporalFspAndNegativeTime2) {
  const std::vector<uint8_t> fixture = DecodeHexFixture(kMySql84TemporalGolden);
  ASSERT_EQ(fixture.size(), 279u);
  ASSERT_EQ(binary::ReadU32Le(fixture.data() + 9), 115u);
  ASSERT_EQ(binary::ReadU32Le(fixture.data() + 115 + 9), 164u);
  ASSERT_EQ(ComputeCRC32(fixture.data(), 111), binary::ReadU32Le(fixture.data() + 111));
  ASSERT_EQ(ComputeCRC32(fixture.data() + 115, 160), binary::ReadU32Le(fixture.data() + 115 + 160));

  CdcEngine engine;
  ASSERT_EQ(engine.Feed(fixture.data(), fixture.size()), fixture.size());
  ASSERT_FALSE(engine.IsError()) << "error=" << engine.ErrorCode();

  ChangeEvent event;
  ASSERT_TRUE(engine.NextEvent(&event));
  ASSERT_EQ(event.after.columns.size(), 22u);
  EXPECT_EQ(event.after.columns[0].int_val, 1);

  const std::vector<std::string> expected_datetime = {
      "2024-06-01 00:00:00",       "2024-06-01 00:00:00.5",    "2024-06-01 00:00:00.99",
      "2024-06-01 00:00:00.123",   "2024-06-01 00:00:00.5678", "2024-06-01 00:00:00.12345",
      "2024-06-01 00:00:00.999999"};
  for (size_t i = 0; i < expected_datetime.size(); ++i) {
    EXPECT_EQ(event.after.columns[1 + i].string_val, expected_datetime[i]);
  }

  const std::vector<std::string> expected_timestamp = {
      "1717200000",      "1717200000.5",     "1717200000.99",    "1717200000.123",
      "1717200000.5678", "1717200000.12345", "1717200000.999999"};
  for (size_t i = 0; i < expected_timestamp.size(); ++i) {
    EXPECT_EQ(event.after.columns[8 + i].string_val, expected_timestamp[i]);
  }

  const std::vector<std::string> expected_time = {
      "-12:34:56",      "-12:34:56.5",     "-12:34:56.99",    "-12:34:56.123",
      "-12:34:56.5678", "-12:34:56.12345", "-12:34:56.999999"};
  for (size_t i = 0; i < expected_time.size(); ++i) {
    EXPECT_EQ(event.after.columns[15 + i].string_val, expected_time[i]);
  }
}

// --- binlog_checksum=NONE handling ---

TEST(CdcEngineChecksumTest, DecodesStreamWithoutChecksum) {
  // With checksum disabled, events carry no trailing CRC32; the engine must
  // not strip 4 bytes (which would corrupt the last column / metadata).
  CdcEngine engine;
  engine.SetChecksumEnabled(false);

  auto table_map = BuildEventNoChecksum(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000,
                                        100, BuildTableMapBody(42, "testdb", "users"));
  auto write = BuildEventNoChecksum(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1000,
                                    150, BuildWriteRowsBody(42, 7777));
  engine.Feed(table_map.data(), table_map.size());
  engine.Feed(write.data(), write.size());

  ChangeEvent event;
  ASSERT_TRUE(engine.NextEvent(&event));
  EXPECT_FALSE(engine.IsError());
  EXPECT_EQ(event.type, EventType::kInsert);
  ASSERT_EQ(event.after.columns.size(), 1u);
  EXPECT_EQ(event.after.columns[0].int_val, 7777);
}

TEST(CdcEngineChecksumTest, DefaultStripsChecksum) {
  // Default path: events include a 4-byte checksum (BuildEvent appends one).
  CdcEngine engine;  // checksum enabled by default
  auto table_map = BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000, 100,
                              BuildTableMapBody(42, "testdb", "users"));
  auto write = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1000, 150,
                          BuildWriteRowsBody(42, 4242));
  engine.Feed(table_map.data(), table_map.size());
  engine.Feed(write.data(), write.size());

  ChangeEvent event;
  ASSERT_TRUE(engine.NextEvent(&event));
  ASSERT_EQ(event.after.columns.size(), 1u);
  EXPECT_EQ(event.after.columns[0].int_val, 4242);
}

TEST(CdcEngineMariaDBTest, AnnotateRowsSqlIsAttachedUntilTransactionEnd) {
  CdcEngine engine;
  const std::string sql = "INSERT INTO users VALUES (42)";
  std::vector<uint8_t> annotation(sql.begin(), sql.end());
  auto annotate = BuildEvent(static_cast<uint8_t>(BinlogEventType::kMariaDBAnnotateRowsEvent), 1000,
                             50, annotation);
  auto table_map = BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000, 100,
                              BuildTableMapBody(42, "testdb", "users"));
  auto write = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1000, 150,
                          BuildWriteRowsBody(42, 42));
  ASSERT_EQ(engine.Feed(annotate.data(), annotate.size()), annotate.size());
  ASSERT_EQ(engine.Feed(table_map.data(), table_map.size()), table_map.size());
  ASSERT_EQ(engine.Feed(write.data(), write.size()), write.size());

  ChangeEvent event;
  ASSERT_TRUE(engine.NextEvent(&event));
  EXPECT_EQ(event.source_sql, sql);

  auto xid = BuildEvent(static_cast<uint8_t>(BinlogEventType::kXidEvent), 1000, 175, {});
  auto next_write = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1001, 200,
                               BuildWriteRowsBody(42, 43));
  ASSERT_EQ(engine.Feed(xid.data(), xid.size()), xid.size());
  ASSERT_EQ(engine.Feed(next_write.data(), next_write.size()), next_write.size());
  ASSERT_TRUE(engine.NextEvent(&event));
  EXPECT_TRUE(event.source_sql.empty());
}

TEST(CdcEngineChecksumTest, CorruptedCrcReturnsChecksumErrorWithoutAdvancingPosition) {
  CdcEngine engine;
  auto table_map = BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000, 100,
                              BuildTableMapBody(42, "testdb", "users"));
  ASSERT_EQ(engine.Feed(table_map.data(), table_map.size()), table_map.size());
  ASSERT_FALSE(engine.IsError());
  ASSERT_EQ(engine.CurrentPosition().offset, 100u);

  auto write = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1000, 150,
                          BuildWriteRowsBody(42, 4242));
  write[kEventHeaderSize + 8] ^= 0x80;
  EXPECT_EQ(engine.Feed(write.data(), write.size()), write.size());
  EXPECT_TRUE(engine.IsError());
  EXPECT_EQ(engine.ErrorCode(), MES_ERR_CHECKSUM);
  EXPECT_EQ(engine.CurrentPosition().offset, 100u);
  EXPECT_FALSE(engine.HasEvents());
}

TEST(CdcEngineChecksumTest, ChecksumNoneArtificialRotateDoesNotPolluteFilename) {
  CdcEngine engine;
  engine.SetChecksumEnabled(false);
  auto rotate = BuildEvent(static_cast<uint8_t>(BinlogEventType::kRotateEvent), 0, 0,
                           BuildRotateBody(4, "mysql-bin.000010"));
  rotate[17] = static_cast<uint8_t>(kLogEventArtificialFlag);
  const uint32_t crc = ComputeCRC32(rotate.data(), rotate.size() - kChecksumSize);
  std::memcpy(rotate.data() + rotate.size() - kChecksumSize, &crc, sizeof(crc));

  EXPECT_EQ(engine.Feed(rotate.data(), rotate.size()), rotate.size());
  EXPECT_FALSE(engine.IsError());
  EXPECT_EQ(engine.CurrentPosition().binlog_file, "mysql-bin.000010");
  EXPECT_EQ(engine.CurrentPosition().offset, 4u);
}

// --- DDL detection drives metadata cache invalidation ---

TEST(CdcEngineDdlTest, DetectsDdlStatements) {
  auto alter = BuildQueryEventBody("testdb", "ALTER TABLE users ADD COLUMN x INT");
  EXPECT_TRUE(IsDdlQueryEvent(alter.data(), alter.size()));

  auto rename = BuildQueryEventBody("testdb", "RENAME TABLE a TO b");
  EXPECT_TRUE(IsDdlQueryEvent(rename.data(), rename.size()));

  auto lowercase = BuildQueryEventBody("testdb", "  create table t (id int)");
  EXPECT_TRUE(IsDdlQueryEvent(lowercase.data(), lowercase.size()));

  auto with_status = BuildQueryEventBody("testdb", "DROP TABLE t", /*status_vars_len=*/7);
  EXPECT_TRUE(IsDdlQueryEvent(with_status.data(), with_status.size()));
}

TEST(CdcEngineDdlTest, DetectsDdlAfterEveryMySqlLeadingCommentForm) {
  const std::array<std::string, 8> statements = {
      "/* audit */ ALTER TABLE t ADD COLUMN c INT",
      "/* first */ /* second */ RENAME TABLE a TO b",
      "-- audit\nDROP TABLE t",
      "# audit\r\nCREATE TABLE t (id INT)",
      "/*+ optimizer hint */ TRUNCATE TABLE t",
      "/*!50100 ALTER TABLE t ADD COLUMN c INT */",
      "/*! ALTER TABLE t ADD COLUMN c INT */",
      "/*M!100100 RENAME TABLE a TO b */",
  };
  for (const auto& statement : statements) {
    auto body = BuildQueryEventBody("testdb", statement);
    EXPECT_TRUE(IsDdlQueryEvent(body.data(), body.size())) << statement;
  }
}

TEST(CdcEngineDdlTest, RejectsCommentOnlyMalformedAndKeywordPrefixes) {
  const std::array<std::string, 6> statements = {"/* unterminated", "/* comment only */",
                                                 "-- comment only", "# comment only",
                                                 "ALTERED TABLE t", "CREATE2 TABLE t"};
  for (const auto& statement : statements) {
    auto body = BuildQueryEventBody("testdb", statement);
    EXPECT_FALSE(IsDdlQueryEvent(body.data(), body.size())) << statement;
  }
}

// --- names_resolved surfaces metadata-fetch failures ---

TEST(CdcEngineNamesResolvedTest, FalseInStandaloneMode) {
  // No metadata fetcher is attached and the TABLE_MAP has no names, so
  // consumers must not mistake positional column keys for resolved names.
  CdcEngine engine;
  auto table_map = BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000, 100,
                              BuildTableMapBody(42, "testdb", "users"));
  auto write = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1000, 150,
                          BuildWriteRowsBody(42, 1));
  engine.Feed(table_map.data(), table_map.size());
  engine.Feed(write.data(), write.size());

  ChangeEvent event;
  ASSERT_TRUE(engine.NextEvent(&event));
  EXPECT_FALSE(event.names_resolved);
}

TEST(CdcEngineNamesResolvedTest, FalseWhenFetcherCannotResolve) {
  // A metadata fetcher is attached but never connected, so FetchColumnInfo
  // fails. The binlog carries no column names, so resolution is required and
  // fails: the event must report names_resolved == false.
  CdcEngine engine;
  MetadataFetcher fetcher;  // intentionally not Connect()ed
  engine.SetMetadataFetcher(&fetcher);

  auto table_map = BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000, 100,
                              BuildTableMapBody(42, "testdb", "users"));
  auto write = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1000, 150,
                          BuildWriteRowsBody(42, 1));
  engine.Feed(table_map.data(), table_map.size());
  engine.Feed(write.data(), write.size());

  ChangeEvent event;
  ASSERT_TRUE(engine.NextEvent(&event));
  EXPECT_FALSE(event.names_resolved);
}

TEST(CdcEngineUnknownEventTest, UnsupportedEventsFailWithoutAdvancingCheckpoint) {
  const std::vector<uint8_t> unsupported_types = {
      static_cast<uint8_t>(BinlogEventType::kPartialUpdateRowsEvent),
      static_cast<uint8_t>(BinlogEventType::kMariaDBWriteRowsCompressedEvent),
      0xFE,
  };

  for (uint8_t type : unsupported_types) {
    CdcEngine engine;
    auto event = BuildEvent(type, 1000, 1234, {});

    EXPECT_EQ(engine.Feed(event.data(), event.size()), event.size());
    EXPECT_TRUE(engine.IsError()) << "event type " << static_cast<unsigned>(type);
    EXPECT_EQ(engine.ErrorCode(), MES_ERR_PARSE);
    EXPECT_EQ(engine.CurrentPosition().offset, 0u);
  }
}

TEST(CdcEngineUnknownEventTest, TaggedGtidIsRecognizedAsControlEvent) {
  CdcEngine engine;
  auto event =
      BuildEvent(static_cast<uint8_t>(BinlogEventType::kGtidTaggedLogEvent), 1000, 1234, {});

  EXPECT_EQ(engine.Feed(event.data(), event.size()), event.size());
  EXPECT_FALSE(engine.IsError());
  EXPECT_EQ(engine.CurrentPosition().offset, 1234u);
}

TEST(CdcEngineDdlTest, IgnoresNonDdlStatements) {
  auto begin = BuildQueryEventBody("testdb", "BEGIN");
  EXPECT_FALSE(IsDdlQueryEvent(begin.data(), begin.size()));

  auto commit = BuildQueryEventBody("testdb", "COMMIT");
  EXPECT_FALSE(IsDdlQueryEvent(commit.data(), commit.size()));

  auto insert = BuildQueryEventBody("testdb", "INSERT INTO t VALUES (1)");
  EXPECT_FALSE(IsDdlQueryEvent(insert.data(), insert.size()));
}

TEST(CdcEngineDdlTest, HandlesMalformedQueryEvent) {
  std::vector<uint8_t> too_short = {0x01, 0x02, 0x03};
  EXPECT_FALSE(IsDdlQueryEvent(too_short.data(), too_short.size()));
  EXPECT_FALSE(IsDdlQueryEvent(nullptr, 0));
}

TEST(CdcEngineDdlTest, QueryEventFedThroughEngineIsSafe) {
  // A QUERY_EVENT (including DDL) must not break the parser; subsequent row
  // events must still decode.
  CdcEngine engine;
  auto ddl_body = BuildQueryEventBody("testdb", "ALTER TABLE users ADD COLUMN x INT");
  auto ddl_event =
      BuildEvent(static_cast<uint8_t>(BinlogEventType::kQueryEvent), 1000, 50, ddl_body);
  size_t consumed = engine.Feed(ddl_event.data(), ddl_event.size());
  EXPECT_EQ(consumed, ddl_event.size());
  EXPECT_FALSE(engine.IsError());

  auto table_map_body = BuildTableMapBody(42, "testdb", "users");
  auto table_map_event =
      BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000, 100, table_map_body);
  engine.Feed(table_map_event.data(), table_map_event.size());
  auto write_event = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1000, 150,
                                BuildWriteRowsBody(42, 123));
  engine.Feed(write_event.data(), write_event.size());
  EXPECT_TRUE(engine.HasEvents());
  EXPECT_FALSE(engine.IsError());
}

TEST(CdcEngineTest, InsertEvent) {
  CdcEngine engine;

  auto table_map_body = BuildTableMapBody(42, "testdb", "users");
  auto table_map_event =
      BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000, 100, table_map_body);

  auto write_body = BuildWriteRowsBody(42, 123);
  auto write_event =
      BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1001, 200, write_body);

  // Feed table map
  size_t consumed = engine.Feed(table_map_event.data(), table_map_event.size());
  EXPECT_EQ(consumed, table_map_event.size());
  EXPECT_FALSE(engine.HasEvents());

  // Feed write rows
  consumed = engine.Feed(write_event.data(), write_event.size());
  EXPECT_EQ(consumed, write_event.size());
  ASSERT_TRUE(engine.HasEvents());
  EXPECT_EQ(engine.PendingEventCount(), 1u);

  ChangeEvent event;
  ASSERT_TRUE(engine.NextEvent(&event));
  EXPECT_EQ(event.type, EventType::kInsert);
  EXPECT_EQ(event.database, "testdb");
  EXPECT_EQ(event.table, "users");
  EXPECT_EQ(event.timestamp, 1001u);
  ASSERT_EQ(event.after.columns.size(), 1u);
  EXPECT_EQ(event.after.columns[0].int_val, 123);
  EXPECT_TRUE(event.after.columns[0].name.empty());
  EXPECT_FALSE(engine.HasEvents());
}

TEST(CdcEngineTest, QueuedEventRetainsSharedTableMetadataAcrossRotate) {
  CdcEngine engine;
  auto table_map_body = BuildTableMapBody(43, "db", "named_table");
  // COLUMN_NAME optional metadata: one length-encoded name, "id".
  table_map_body.insert(table_map_body.end(), {0x04, 0x03, 0x02, 'i', 'd'});
  const auto table_map =
      BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000, 100, table_map_body);
  const auto write = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1001, 200,
                                BuildWriteRowsBody(43, 7));
  const auto rotate = BuildEvent(static_cast<uint8_t>(BinlogEventType::kRotateEvent), 1002, 300,
                                 BuildRotateBody(4, "mysql-bin.000002"));

  engine.Feed(table_map.data(), table_map.size());
  engine.Feed(write.data(), write.size());
  // ROTATE clears the registry before the queued row event is consumed.
  engine.Feed(rotate.data(), rotate.size());

  ChangeEvent event;
  ASSERT_TRUE(engine.NextEvent(&event));
  ASSERT_TRUE(event.table_metadata);
  ASSERT_EQ(event.after.columns.size(), 1u);
  EXPECT_EQ(event.after.columns[0].name, "id");
  EXPECT_EQ(event.after.columns[0].name.data(), event.table_metadata->columns[0].name.data());
}

TEST(CdcEngineTest, UpdateEvent) {
  CdcEngine engine;

  auto table_map_body = BuildTableMapBody(10, "db", "t");
  auto table_map_event =
      BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000, 100, table_map_body);

  auto update_body = BuildUpdateRowsBody(10, 100, 200);
  auto update_event =
      BuildEvent(static_cast<uint8_t>(BinlogEventType::kUpdateRowsEvent), 1002, 200, update_body);

  engine.Feed(table_map_event.data(), table_map_event.size());
  engine.Feed(update_event.data(), update_event.size());

  ASSERT_TRUE(engine.HasEvents());
  ChangeEvent event;
  ASSERT_TRUE(engine.NextEvent(&event));
  EXPECT_EQ(event.type, EventType::kUpdate);
  EXPECT_EQ(event.database, "db");
  EXPECT_EQ(event.table, "t");
  ASSERT_EQ(event.before.columns.size(), 1u);
  EXPECT_EQ(event.before.columns[0].int_val, 100);
  ASSERT_EQ(event.after.columns.size(), 1u);
  EXPECT_EQ(event.after.columns[0].int_val, 200);
  EXPECT_TRUE(event.before.columns[0].name.empty());
  EXPECT_TRUE(event.after.columns[0].name.empty());
}

TEST(CdcEngineTest, DeleteEvent) {
  CdcEngine engine;

  auto table_map_body = BuildTableMapBody(10, "db", "t");
  auto table_map_event =
      BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000, 100, table_map_body);

  auto delete_body = BuildDeleteRowsBody(10, 999);
  auto delete_event =
      BuildEvent(static_cast<uint8_t>(BinlogEventType::kDeleteRowsEvent), 1003, 200, delete_body);

  engine.Feed(table_map_event.data(), table_map_event.size());
  engine.Feed(delete_event.data(), delete_event.size());

  ASSERT_TRUE(engine.HasEvents());
  ChangeEvent event;
  ASSERT_TRUE(engine.NextEvent(&event));
  EXPECT_EQ(event.type, EventType::kDelete);
  ASSERT_EQ(event.before.columns.size(), 1u);
  EXPECT_EQ(event.before.columns[0].int_val, 999);
  EXPECT_TRUE(event.before.columns[0].name.empty());
}

TEST(CdcEngineTest, RotateEvent) {
  CdcEngine engine;

  auto rotate_body = BuildRotateBody(4, "binlog.000002");
  auto rotate_event =
      BuildEvent(static_cast<uint8_t>(BinlogEventType::kRotateEvent), 0, 0, rotate_body);

  engine.Feed(rotate_event.data(), rotate_event.size());
  EXPECT_FALSE(engine.HasEvents());
  EXPECT_EQ(engine.CurrentPosition().binlog_file, "binlog.000002");
  EXPECT_EQ(engine.CurrentPosition().offset, 4u);
}

TEST(CdcEngineTest, RotateClearsTableMapRegistry) {
  CdcEngine engine;

  // Register table_id 1, then rotate to a new binlog file.
  auto table_map_event = BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000,
                                    100, BuildTableMapBody(1, "db", "t"));
  auto rotate_event = BuildEvent(static_cast<uint8_t>(BinlogEventType::kRotateEvent), 0, 0,
                                 BuildRotateBody(4, "binlog.000002"));
  // A row event for table_id 1 *after* the rotate, without a fresh TABLE_MAP.
  auto write_event = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1001, 200,
                                BuildWriteRowsBody(1, 42));

  std::vector<uint8_t> stream;
  stream.insert(stream.end(), table_map_event.begin(), table_map_event.end());
  stream.insert(stream.end(), rotate_event.begin(), rotate_event.end());
  stream.insert(stream.end(), write_event.begin(), write_event.end());

  engine.Feed(stream.data(), stream.size());

  // The registry was cleared on rotate, so the stale table_id no longer
  // resolves and the row event produces nothing (rather than decoding against
  // stale metadata). The missing map is explicit so the resume position cannot
  // silently advance past the row event.
  EXPECT_FALSE(engine.HasEvents());
  EXPECT_TRUE(engine.IsError());
  EXPECT_EQ(engine.ErrorCode(), MES_ERR_DECODE_ROW);
  EXPECT_EQ(engine.CurrentPosition().offset, 4u);
}

TEST(CdcEngineTest, DecodeFailureDoesNotAdvancePosition) {
  CdcEngine engine;

  // TABLE_MAP advances the position to its next_position (100).
  auto table_map_event = BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000,
                                    100, BuildTableMapBody(1, "db", "t"));
  // Truncated row event with next_position 200; its decode will fail.
  auto write_body = BuildWriteRowsBody(1, 42);
  write_body.pop_back();
  auto write_event =
      BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1001, 200, write_body);

  std::vector<uint8_t> stream;
  stream.insert(stream.end(), table_map_event.begin(), table_map_event.end());
  stream.insert(stream.end(), write_event.begin(), write_event.end());

  engine.Feed(stream.data(), stream.size());

  EXPECT_TRUE(engine.IsError());
  // The resume offset must stay at the last good event (100), not advance to
  // the failed event's next_position (200), so a reconnect re-reads it.
  EXPECT_EQ(engine.CurrentPosition().offset, 100u);
}

TEST(CdcEngineTest, UnknownEventSkipped) {
  CdcEngine engine;

  // Event type 99 is unknown
  std::vector<uint8_t> body = {0x01, 0x02, 0x03, 0x04};
  auto event = BuildEvent(99, 1000, 100, body);

  size_t consumed = engine.Feed(event.data(), event.size());
  EXPECT_EQ(consumed, event.size());
  EXPECT_FALSE(engine.HasEvents());
}

TEST(CdcEngineTest, MultipleEvents) {
  CdcEngine engine;

  auto table_map_body = BuildTableMapBody(1, "db", "t");
  auto table_map_event =
      BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000, 100, table_map_body);

  auto write1 = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1001, 200,
                           BuildWriteRowsBody(1, 10));
  auto write2 = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1002, 300,
                           BuildWriteRowsBody(1, 20));

  // Feed all events in one buffer
  std::vector<uint8_t> combined;
  combined.insert(combined.end(), table_map_event.begin(), table_map_event.end());
  combined.insert(combined.end(), write1.begin(), write1.end());
  combined.insert(combined.end(), write2.begin(), write2.end());

  engine.Feed(combined.data(), combined.size());
  EXPECT_EQ(engine.PendingEventCount(), 2u);

  ChangeEvent event1, event2;
  ASSERT_TRUE(engine.NextEvent(&event1));
  EXPECT_EQ(event1.after.columns[0].int_val, 10);
  ASSERT_TRUE(engine.NextEvent(&event2));
  EXPECT_EQ(event2.after.columns[0].int_val, 20);
  EXPECT_FALSE(engine.HasEvents());
}

TEST(CdcEngineTest, ByteByByteFeeding) {
  CdcEngine engine;

  auto table_map_body = BuildTableMapBody(1, "db", "t");
  auto table_map_event =
      BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000, 100, table_map_body);

  auto write_body = BuildWriteRowsBody(1, 42);
  auto write_event =
      BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1001, 200, write_body);

  // Combine and feed byte by byte
  std::vector<uint8_t> combined;
  combined.insert(combined.end(), table_map_event.begin(), table_map_event.end());
  combined.insert(combined.end(), write_event.begin(), write_event.end());

  for (size_t i = 0; i < combined.size(); i++) {
    engine.Feed(&combined[i], 1);
  }

  ASSERT_TRUE(engine.HasEvents());
  ChangeEvent event;
  ASSERT_TRUE(engine.NextEvent(&event));
  EXPECT_EQ(event.type, EventType::kInsert);
  EXPECT_EQ(event.after.columns[0].int_val, 42);
}

TEST(CdcEngineTest, Reset) {
  CdcEngine engine;

  auto table_map_body = BuildTableMapBody(1, "db", "t");
  auto table_map_event =
      BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000, 100, table_map_body);

  auto write_body = BuildWriteRowsBody(1, 42);
  auto write_event =
      BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1001, 200, write_body);

  engine.Feed(table_map_event.data(), table_map_event.size());
  engine.Feed(write_event.data(), write_event.size());
  ASSERT_TRUE(engine.HasEvents());

  engine.Reset();
  EXPECT_TRUE(engine.HasEvents());
  EXPECT_EQ(engine.PendingEventCount(), 1u);
  EXPECT_EQ(engine.CurrentPosition().offset, 0u);
  EXPECT_TRUE(engine.CurrentPosition().binlog_file.empty());

  ChangeEvent preserved;
  ASSERT_TRUE(engine.NextEvent(&preserved));
  EXPECT_EQ(preserved.type, EventType::kInsert);

  // After reset, row event without table map is an explicit state/decode error.
  engine.Feed(write_event.data(), write_event.size());
  EXPECT_FALSE(engine.HasEvents());
  EXPECT_EQ(engine.ErrorCode(), MES_ERR_DECODE_ROW);
}

TEST(CdcEngineTest, EmptyRotateFilenamePreservesResumeFilename) {
  CdcEngine engine;
  const auto named = BuildEvent(static_cast<uint8_t>(BinlogEventType::kRotateEvent), 0, 100,
                                BuildRotateBody(4, "mysql-bin.000007"));
  engine.Feed(named.data(), named.size());
  ASSERT_FALSE(engine.IsError());
  EXPECT_EQ(engine.CurrentPosition().binlog_file, "mysql-bin.000007");

  const auto empty = BuildEvent(static_cast<uint8_t>(BinlogEventType::kRotateEvent), 0, 200,
                                BuildRotateBody(123, ""));
  engine.Feed(empty.data(), empty.size());
  ASSERT_FALSE(engine.IsError());
  EXPECT_EQ(engine.CurrentPosition().binlog_file, "mysql-bin.000007");
  EXPECT_EQ(engine.CurrentPosition().offset, 123u);
}

TEST(CdcEngineTest, NextEventNullOutput) {
  CdcEngine engine;
  EXPECT_FALSE(engine.NextEvent(nullptr));
}

TEST(CdcEngineTest, NextEventEmptyQueue) {
  CdcEngine engine;
  ChangeEvent event;
  EXPECT_FALSE(engine.NextEvent(&event));
}

TEST(CdcEngineTest, RowEventWithoutTableMap) {
  CdcEngine engine;

  // Feed a WRITE_ROWS_EVENT without a preceding TABLE_MAP_EVENT
  auto write_body = BuildWriteRowsBody(999, 42);
  auto write_event =
      BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1001, 200, write_body);

  // The malformed event is fully accepted by the parser before row decoding
  // fails. Feed must return at that point rather than spinning on the parser's
  // still-ready event state.
  EXPECT_EQ(engine.Feed(write_event.data(), write_event.size()), write_event.size());
  EXPECT_FALSE(engine.HasEvents());
  EXPECT_TRUE(engine.IsError());
  EXPECT_EQ(engine.ErrorCode(), MES_ERR_DECODE_ROW);
  EXPECT_EQ(engine.CurrentPosition().offset, 0u);
}

TEST(CdcEngineTest, TruncatedRowEventSetsDecodeError) {
  CdcEngine engine;

  auto table_map_body = BuildTableMapBody(1, "db", "t");
  auto table_map_event =
      BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000, 100, table_map_body);

  auto write_body = BuildWriteRowsBody(1, 42);
  write_body.pop_back();
  auto write_event =
      BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1001, 200, write_body);

  std::vector<uint8_t> stream;
  stream.insert(stream.end(), table_map_event.begin(), table_map_event.end());
  stream.insert(stream.end(), write_event.begin(), write_event.end());

  size_t consumed = engine.Feed(stream.data(), stream.size());
  EXPECT_EQ(consumed, stream.size());
  EXPECT_TRUE(engine.IsError());
  EXPECT_EQ(engine.ErrorCode(), MES_ERR_DECODE_ROW);
  EXPECT_FALSE(engine.HasEvents());
  EXPECT_EQ(engine.Feed(stream.data(), stream.size()), 0u);

  engine.Reset();
  EXPECT_FALSE(engine.IsError());
  EXPECT_EQ(engine.ErrorCode(), MES_OK);
}

TEST(CdcEngineTest, BackpressureStopsFeedingWhenQueueFull) {
  CdcEngine engine;
  engine.SetMaxQueueSize(2);

  // Register table map
  auto table_map_body = BuildTableMapBody(1, "db", "t");
  auto table_map_event =
      BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000, 100, table_map_body);

  // Build 3 write events
  auto write1 = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1001, 200,
                           BuildWriteRowsBody(1, 10));
  auto write2 = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1002, 300,
                           BuildWriteRowsBody(1, 20));
  auto write3 = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1003, 400,
                           BuildWriteRowsBody(1, 30));

  // Combine all events into one buffer
  std::vector<uint8_t> combined;
  combined.insert(combined.end(), table_map_event.begin(), table_map_event.end());
  combined.insert(combined.end(), write1.begin(), write1.end());
  combined.insert(combined.end(), write2.begin(), write2.end());
  combined.insert(combined.end(), write3.begin(), write3.end());

  // Feed should stop after queue reaches 2
  size_t consumed = engine.Feed(combined.data(), combined.size());
  EXPECT_LT(consumed, combined.size());
  EXPECT_EQ(engine.PendingEventCount(), 2u);

  // Drain one event
  ChangeEvent event;
  ASSERT_TRUE(engine.NextEvent(&event));
  EXPECT_EQ(event.after.columns[0].int_val, 10);
  EXPECT_EQ(engine.PendingEventCount(), 1u);

  // Re-feed the remaining bytes
  size_t consumed2 = engine.Feed(combined.data() + consumed, combined.size() - consumed);
  EXPECT_GT(consumed2, 0u);

  // Should now have the second event still queued plus at least one more
  EXPECT_GE(engine.PendingEventCount(), 2u);

  // Drain all remaining events
  ASSERT_TRUE(engine.NextEvent(&event));
  EXPECT_EQ(event.after.columns[0].int_val, 20);
  ASSERT_TRUE(engine.NextEvent(&event));
  EXPECT_EQ(event.after.columns[0].int_val, 30);
  EXPECT_FALSE(engine.HasEvents());
}

TEST(CdcEngineTest, BackpressureInnerLoopStopsWhenQueueFull) {
  CdcEngine engine;
  engine.SetMaxQueueSize(1);

  // Register table map
  auto table_map_body = BuildTableMapBody(1, "db", "t");
  auto table_map_event =
      BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000, 100, table_map_body);

  // Build 3 write events and combine them into one buffer.
  // When the stream parser processes this buffer in a single Feed call,
  // the inner loop should stop after 1 event due to backpressure.
  auto write1 = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1001, 200,
                           BuildWriteRowsBody(1, 10));
  auto write2 = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1002, 300,
                           BuildWriteRowsBody(1, 20));
  auto write3 = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1003, 400,
                           BuildWriteRowsBody(1, 30));

  std::vector<uint8_t> combined;
  combined.insert(combined.end(), table_map_event.begin(), table_map_event.end());
  combined.insert(combined.end(), write1.begin(), write1.end());
  combined.insert(combined.end(), write2.begin(), write2.end());
  combined.insert(combined.end(), write3.begin(), write3.end());

  // Feed entire buffer; inner loop should stop after 1 event
  size_t consumed = engine.Feed(combined.data(), combined.size());
  EXPECT_LT(consumed, combined.size());
  EXPECT_EQ(engine.PendingEventCount(), 1u);

  // Drain and re-feed iteratively until all events consumed
  size_t total_consumed = consumed;
  size_t events_seen = 0;
  int iterations = 0;
  constexpr int kMaxIterations = 10000;
  while ((total_consumed < combined.size() || engine.HasEvents()) && iterations < kMaxIterations) {
    ++iterations;
    ChangeEvent event;
    while (engine.NextEvent(&event)) {
      events_seen++;
    }
    if (total_consumed < combined.size()) {
      size_t c = engine.Feed(combined.data() + total_consumed, combined.size() - total_consumed);
      total_consumed += c;
    }
  }
  ASSERT_LT(iterations, kMaxIterations) << "Feed/drain loop did not terminate";
  // Drain any remaining
  ChangeEvent event;
  while (engine.NextEvent(&event)) {
    events_seen++;
  }
  EXPECT_EQ(events_seen, 3u);
}

TEST(CdcEngineTest, BackpressureIsBoundedByDefault) {
  CdcEngine engine;
  EXPECT_EQ(engine.MaxQueueSize(), MES_DEFAULT_QUEUE_SIZE);

  auto table_map_body = BuildTableMapBody(1, "db", "t");
  auto table_map_event =
      BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000, 100, table_map_body);

  auto write1 = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1001, 200,
                           BuildWriteRowsBody(1, 10));
  auto write2 = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1002, 300,
                           BuildWriteRowsBody(1, 20));
  auto write3 = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1003, 400,
                           BuildWriteRowsBody(1, 30));

  std::vector<uint8_t> combined;
  combined.insert(combined.end(), table_map_event.begin(), table_map_event.end());
  combined.insert(combined.end(), write1.begin(), write1.end());
  combined.insert(combined.end(), write2.begin(), write2.end());
  combined.insert(combined.end(), write3.begin(), write3.end());

  // The default has room for this small fixture while preventing unbounded
  // growth for a producer that outruns the consumer.
  size_t consumed = engine.Feed(combined.data(), combined.size());
  EXPECT_EQ(consumed, combined.size());
  EXPECT_EQ(engine.PendingEventCount(), 3u);
}

TEST(CdcEngineTest, IncludeDatabasesFilter) {
  CdcEngine engine;
  engine.SetIncludeDatabases({"mydb"});

  // Register table in allowed database
  auto tm1 = BuildTableMapBody(1, "mydb", "users");
  auto ev1 = BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000, 100, tm1);
  engine.Feed(ev1.data(), ev1.size());

  auto w1 = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1001, 200,
                       BuildWriteRowsBody(1, 10));
  engine.Feed(w1.data(), w1.size());
  EXPECT_EQ(engine.PendingEventCount(), 1u);

  // Register table in blocked database
  auto tm2 = BuildTableMapBody(2, "otherdb", "logs");
  auto ev2 = BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1002, 300, tm2);
  engine.Feed(ev2.data(), ev2.size());

  auto w2 = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1003, 400,
                       BuildWriteRowsBody(2, 20));
  engine.Feed(w2.data(), w2.size());

  // Should still have only 1 event (from mydb.users)
  EXPECT_EQ(engine.PendingEventCount(), 1u);

  ChangeEvent event;
  ASSERT_TRUE(engine.NextEvent(&event));
  EXPECT_EQ(event.database, "mydb");
  EXPECT_EQ(event.table, "users");
  EXPECT_EQ(event.after.columns[0].int_val, 10);
  EXPECT_FALSE(engine.HasEvents());
}

TEST(CdcEngineTest, ExcludeTablesFilter) {
  CdcEngine engine;
  engine.SetExcludeTables({"mydb.logs"});

  // Register allowed table
  auto tm1 = BuildTableMapBody(1, "mydb", "users");
  auto ev1 = BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000, 100, tm1);
  engine.Feed(ev1.data(), ev1.size());

  auto w1 = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1001, 200,
                       BuildWriteRowsBody(1, 10));
  engine.Feed(w1.data(), w1.size());
  EXPECT_EQ(engine.PendingEventCount(), 1u);

  // Register excluded table
  auto tm2 = BuildTableMapBody(2, "mydb", "logs");
  auto ev2 = BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1002, 300, tm2);
  engine.Feed(ev2.data(), ev2.size());

  auto w2 = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1003, 400,
                       BuildWriteRowsBody(2, 20));
  engine.Feed(w2.data(), w2.size());

  // Should still have only 1 event
  EXPECT_EQ(engine.PendingEventCount(), 1u);

  ChangeEvent event;
  ASSERT_TRUE(engine.NextEvent(&event));
  EXPECT_EQ(event.database, "mydb");
  EXPECT_EQ(event.table, "users");
  EXPECT_FALSE(engine.HasEvents());
}

TEST(CdcEngineTest, IncludeTablesFilter) {
  CdcEngine engine;
  engine.SetIncludeTables({"mydb.users"});

  // Register included table
  auto tm1 = BuildTableMapBody(1, "mydb", "users");
  auto ev1 = BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000, 100, tm1);
  engine.Feed(ev1.data(), ev1.size());

  auto w1 = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1001, 200,
                       BuildWriteRowsBody(1, 10));
  engine.Feed(w1.data(), w1.size());
  EXPECT_EQ(engine.PendingEventCount(), 1u);

  // Register non-included table
  auto tm2 = BuildTableMapBody(2, "mydb", "logs");
  auto ev2 = BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1002, 300, tm2);
  engine.Feed(ev2.data(), ev2.size());

  auto w2 = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1003, 400,
                       BuildWriteRowsBody(2, 20));
  engine.Feed(w2.data(), w2.size());

  EXPECT_EQ(engine.PendingEventCount(), 1u);

  ChangeEvent event;
  ASSERT_TRUE(engine.NextEvent(&event));
  EXPECT_EQ(event.table, "users");
  EXPECT_FALSE(engine.HasEvents());
}

TEST(CdcEngineTest, TableFilterTrailingWildcardMatchesQualifiedAndBareNames) {
  CdcEngine engine;
  engine.SetIncludeTables({"mydb.audit_*", "users*"});
  engine.SetExcludeTables({"mydb.audit_private*"});

  const auto audit_map = BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000,
                                    100, BuildTableMapBody(1, "mydb", "audit_log"));
  const auto private_map = BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1001,
                                      200, BuildTableMapBody(2, "mydb", "audit_private_log"));
  const auto users_map = BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1002,
                                    300, BuildTableMapBody(3, "otherdb", "users_archive"));
  ASSERT_EQ(engine.Feed(audit_map.data(), audit_map.size()), audit_map.size());
  ASSERT_EQ(engine.Feed(private_map.data(), private_map.size()), private_map.size());
  ASSERT_EQ(engine.Feed(users_map.data(), users_map.size()), users_map.size());

  const auto audit_row = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1003,
                                    400, BuildWriteRowsBody(1, 10));
  const auto private_row = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1004,
                                      500, BuildWriteRowsBody(2, 20));
  const auto users_row = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1005,
                                    600, BuildWriteRowsBody(3, 30));
  ASSERT_EQ(engine.Feed(audit_row.data(), audit_row.size()), audit_row.size());
  ASSERT_EQ(engine.Feed(private_row.data(), private_row.size()), private_row.size());
  ASSERT_EQ(engine.Feed(users_row.data(), users_row.size()), users_row.size());

  ChangeEvent event;
  ASSERT_TRUE(engine.NextEvent(&event));
  EXPECT_EQ(event.table, "audit_log");
  ASSERT_TRUE(engine.NextEvent(&event));
  EXPECT_EQ(event.table, "users_archive");
  EXPECT_FALSE(engine.HasEvents());
}

TEST(CdcEngineTest, WarnsOnceWhenIncludeFiltersMatchNoTableMaps) {
  g_include_filter_warning_count = 0;
  g_include_filter_warning_message.clear();
  LogConfig::SetCallback(CaptureIncludeFilterWarning, MES_LOG_DEBUG, nullptr);
  {
    CdcEngine engine;
    engine.SetIncludeTables({"mydb.missing*"});
    const auto table_map = BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000,
                                      100, BuildTableMapBody(1, "mydb", "users"));
    ASSERT_EQ(engine.Feed(table_map.data(), table_map.size()), table_map.size());
    engine.Reset();
  }
  LogConfig::SetCallback(nullptr, MES_LOG_ERROR, nullptr);

  EXPECT_EQ(g_include_filter_warning_count, 1);
  EXPECT_NE(g_include_filter_warning_message.find("include_table_count=1"), std::string::npos);
}

TEST(CdcEngineTest, WarnsOnDestructionOnlyWhenNoIncludeFilterMatches) {
  g_include_filter_warning_count = 0;
  g_include_filter_warning_message.clear();
  LogConfig::SetCallback(CaptureIncludeFilterWarning, MES_LOG_DEBUG, nullptr);
  const auto table_map = BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000,
                                    100, BuildTableMapBody(1, "mydb", "users"));
  {
    CdcEngine engine;
    engine.SetIncludeTables({"mydb.missing"});
    ASSERT_EQ(engine.Feed(table_map.data(), table_map.size()), table_map.size());
  }
  EXPECT_EQ(g_include_filter_warning_count, 1);

  {
    CdcEngine engine;
    engine.SetIncludeTables({"mydb.users"});
    ASSERT_EQ(engine.Feed(table_map.data(), table_map.size()), table_map.size());
  }
  LogConfig::SetCallback(nullptr, MES_LOG_ERROR, nullptr);

  EXPECT_EQ(g_include_filter_warning_count, 1);
}

TEST(CdcEngineTest, ExcludeTableUnqualifiedName) {
  CdcEngine engine;
  // Exclude by unqualified name - should match any database
  engine.SetExcludeTables({"logs"});

  auto tm1 = BuildTableMapBody(1, "db1", "logs");
  auto ev1 = BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000, 100, tm1);
  engine.Feed(ev1.data(), ev1.size());

  auto w1 = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1001, 200,
                       BuildWriteRowsBody(1, 10));
  engine.Feed(w1.data(), w1.size());

  auto tm2 = BuildTableMapBody(2, "db2", "logs");
  auto ev2 = BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1002, 300, tm2);
  engine.Feed(ev2.data(), ev2.size());

  auto w2 = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1003, 400,
                       BuildWriteRowsBody(2, 20));
  engine.Feed(w2.data(), w2.size());

  // Both should be blocked
  EXPECT_FALSE(engine.HasEvents());
}

TEST(CdcEngineTest, FilterResetClearsBlockedIds) {
  CdcEngine engine;
  engine.SetIncludeDatabases({"mydb"});

  // Block a table
  auto tm1 = BuildTableMapBody(1, "otherdb", "t");
  auto ev1 = BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000, 100, tm1);
  engine.Feed(ev1.data(), ev1.size());

  auto w1 = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1001, 200,
                       BuildWriteRowsBody(1, 10));
  engine.Feed(w1.data(), w1.size());
  EXPECT_FALSE(engine.HasEvents());

  // Reset clears blocked set and filters remain
  engine.Reset();

  // After reset, the table map is also cleared, so this is a state/decode error.
  engine.Feed(w1.data(), w1.size());
  EXPECT_FALSE(engine.HasEvents());
  EXPECT_EQ(engine.ErrorCode(), MES_ERR_DECODE_ROW);
}

TEST(CdcEngineTest, RuntimeFilterChangesReevaluateExistingTableMap) {
  CdcEngine engine;

  // Register the table exactly once. Runtime filter changes must update the
  // derived table-id cache without waiting for another TABLE_MAP event.
  auto table_map = BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000, 100,
                              BuildTableMapBody(1, "mydb", "users"));
  ASSERT_EQ(engine.Feed(table_map.data(), table_map.size()), table_map.size());

  engine.SetIncludeTables({"mydb.other"});
  auto blocked = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1001, 200,
                            BuildWriteRowsBody(1, 10));
  EXPECT_EQ(engine.Feed(blocked.data(), blocked.size()), blocked.size());
  EXPECT_FALSE(engine.HasEvents());
  EXPECT_FALSE(engine.IsError());

  engine.SetIncludeTables({"mydb.users"});
  auto allowed = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1002, 300,
                            BuildWriteRowsBody(1, 20));
  EXPECT_EQ(engine.Feed(allowed.data(), allowed.size()), allowed.size());

  ChangeEvent event;
  ASSERT_TRUE(engine.NextEvent(&event));
  EXPECT_EQ(event.database, "mydb");
  EXPECT_EQ(event.table, "users");
  ASSERT_EQ(event.after.columns.size(), 1u);
  EXPECT_EQ(event.after.columns[0].int_val, 20);
  EXPECT_FALSE(engine.HasEvents());
  EXPECT_FALSE(engine.IsError());
}

TEST(CdcEngineTest, TableMapTooShortBodyIsParseError) {
  CdcEngine engine;

  // Build a TABLE_MAP event with a body that is too short (< 8 bytes)
  std::vector<uint8_t> short_body = {0x01, 0x02, 0x03};
  auto event =
      BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000, 100, short_body);

  size_t consumed = engine.Feed(event.data(), event.size());
  EXPECT_EQ(consumed, event.size());
  EXPECT_FALSE(engine.HasEvents());
  EXPECT_TRUE(engine.IsError());
  EXPECT_EQ(engine.ErrorCode(), MES_ERR_PARSE);
  EXPECT_EQ(engine.CurrentPosition().offset, 0u);
}

TEST(CdcEngineTest, RowBodyShorterThanTableIdIsDecodeError) {
  CdcEngine engine;
  auto event = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1000, 100,
                          {0x01, 0x02, 0x03});
  EXPECT_EQ(engine.Feed(event.data(), event.size()), event.size());
  EXPECT_TRUE(engine.IsError());
  EXPECT_EQ(engine.ErrorCode(), MES_ERR_DECODE_ROW);
  EXPECT_EQ(engine.CurrentPosition().offset, 0u);
}

}  // namespace
}  // namespace mes
