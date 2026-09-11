// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>
#include <zlib.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "binary_util.h"
#include "cdc_engine.h"
#include "client/metadata_fetcher.h"
#include "crc32.h"
#include "event_header.h"
#include "logger.h"
#include "test_helpers.h"

namespace mes {

/** @brief Clears the backoff a failed metadata reconnect installed, so a test
 *  can model the retry window having elapsed between two events. */
class MetadataFetcherTestAccess {
 public:
  static void ClearReconnectBackoff(MetadataFetcher* fetcher) {
    fetcher->next_reconnect_attempt_ = {};
  }
};

namespace {

using test::BuildDeleteRowsBody;
using test::BuildEvent;
using test::BuildEventNoChecksum;
using test::BuildQueryEventBody;
using test::BuildRotateBody;
using test::BuildTableMapBody;
using test::BuildUpdateRowsBody;
using test::BuildWriteRowsBody;
using test::BuildWriteRowsBodyMultiRow;

int g_include_filter_warning_count = 0;
std::string g_include_filter_warning_message;

void CaptureIncludeFilterWarning(mes_log_level_t level, const char* message, void*) {
  if (level != MES_LOG_WARN || message == nullptr) return;
  const std::string_view text(message);
  if (text.find("event=include_filter_matched_nothing") == std::string_view::npos) return;
  ++g_include_filter_warning_count;
  g_include_filter_warning_message = text;
}

std::string g_last_error_log;

void CaptureErrorLog(mes_log_level_t level, const char* message, void*) {
  if (level != MES_LOG_ERROR || message == nullptr) return;
  g_last_error_log = message;
}

// Routes error-level logs into g_last_error_log for the duration of a scope,
// so a test can tell apart error paths that share the same error code.
class ScopedErrorLogCapture {
 public:
  ScopedErrorLogCapture() {
    g_last_error_log.clear();
    LogConfig::SetCallback(CaptureErrorLog, MES_LOG_ERROR, nullptr);
  }
  ~ScopedErrorLogCapture() { LogConfig::SetCallback(nullptr, MES_LOG_ERROR, nullptr); }

  ScopedErrorLogCapture(const ScopedErrorLogCapture&) = delete;
  ScopedErrorLogCapture& operator=(const ScopedErrorLogCapture&) = delete;
};

const char* g_captured_event_name = nullptr;
int g_captured_event_count = 0;
std::string g_captured_event_message;

void CaptureEventByName(mes_log_level_t, const char* message, void*) {
  if (message == nullptr || g_captured_event_name == nullptr) return;
  const std::string_view text(message);
  if (text.find(g_captured_event_name) == std::string_view::npos) return;
  ++g_captured_event_count;
  g_captured_event_message = text;
}

// Counts the structured log records carrying one event name for the duration of
// a scope, so a test can assert how often a condition was reported.
class ScopedEventLogCapture {
 public:
  explicit ScopedEventLogCapture(const char* event_field) {
    g_captured_event_name = event_field;
    g_captured_event_count = 0;
    g_captured_event_message.clear();
    LogConfig::SetCallback(CaptureEventByName, MES_LOG_DEBUG, nullptr);
  }
  ~ScopedEventLogCapture() {
    LogConfig::SetCallback(nullptr, MES_LOG_ERROR, nullptr);
    g_captured_event_name = nullptr;
  }

  ScopedEventLogCapture(const ScopedEventLogCapture&) = delete;
  ScopedEventLogCapture& operator=(const ScopedEventLogCapture&) = delete;

  int Count() const { return g_captured_event_count; }
  const std::string& LastMessage() const { return g_captured_event_message; }
};

// The TABLE_MAP body of BuildTableMapBody followed by the SIGNEDNESS optional
// metadata field (type 1) for its single numeric column.
std::vector<uint8_t> BuildTableMapBodyWithSignedness(uint64_t table_id, const std::string& db,
                                                     const std::string& table, bool is_unsigned) {
  std::vector<uint8_t> body = BuildTableMapBody(table_id, db, table);
  // Field type, payload length, then the MSB-first bitmap over numeric columns.
  const std::vector<uint8_t> signedness = {0x01, 0x01, is_unsigned ? uint8_t{0x80} : uint8_t{0x00}};
  body.insert(body.end(), signedness.begin(), signedness.end());
  return body;
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
  EXPECT_EQ(event.SourceSql(), sql);

  auto xid = BuildEvent(static_cast<uint8_t>(BinlogEventType::kXidEvent), 1000, 175, {});
  auto next_write = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1001, 200,
                               BuildWriteRowsBody(42, 43));
  ASSERT_EQ(engine.Feed(xid.data(), xid.size()), xid.size());
  ASSERT_EQ(engine.Feed(next_write.data(), next_write.size()), next_write.size());
  ASSERT_TRUE(engine.NextEvent(&event));
  EXPECT_TRUE(event.SourceSql().empty());
}

// One ANNOTATE_ROWS annotates every row of the ROWS event that follows it.
// Copying the statement into each row would charge its length once per row,
// which for a large statement dominates a queued event; the rows must share a
// single copy instead.
TEST(CdcEngineMariaDBTest, AnnotateRowsSqlIsSharedAcrossTheRowsOfOneEvent) {
  CdcEngine engine;
  const std::string sql(4096, 'x');
  auto annotate = BuildEvent(static_cast<uint8_t>(BinlogEventType::kMariaDBAnnotateRowsEvent), 1000,
                             50, std::vector<uint8_t>(sql.begin(), sql.end()));
  auto table_map = BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000, 100,
                              BuildTableMapBody(42, "testdb", "users"));
  // Two INT rows in one WRITE_ROWS event: the single-row body plus a second
  // (null bitmap, value) pair in the same layout.
  std::vector<uint8_t> two_rows = BuildWriteRowsBody(42, 1);
  const std::vector<uint8_t> second_row = {0x00, 0x02, 0x00, 0x00, 0x00};
  two_rows.insert(two_rows.end(), second_row.begin(), second_row.end());
  auto write =
      BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1000, 150, two_rows);
  ASSERT_EQ(engine.Feed(annotate.data(), annotate.size()), annotate.size());
  ASSERT_EQ(engine.Feed(table_map.data(), table_map.size()), table_map.size());
  ASSERT_EQ(engine.Feed(write.data(), write.size()), write.size());

  ChangeEvent first;
  ChangeEvent second;
  ASSERT_TRUE(engine.NextEvent(&first));
  ASSERT_TRUE(engine.NextEvent(&second));
  ASSERT_EQ(first.after.columns.size(), 1u);
  ASSERT_EQ(second.after.columns.size(), 1u);
  EXPECT_EQ(first.after.columns[0].int_val, 1);
  EXPECT_EQ(second.after.columns[0].int_val, 2);
  EXPECT_EQ(first.SourceSql(), sql);
  EXPECT_EQ(second.SourceSql(), sql);
  EXPECT_EQ(first.source_sql.get(), second.source_sql.get());
}

// An event with no annotation must not hold an allocation for the empty case.
TEST(CdcEngineMariaDBTest, EventsWithoutAnnotateRowsHoldNoStatement) {
  CdcEngine engine;
  auto table_map = BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000, 100,
                              BuildTableMapBody(42, "testdb", "users"));
  auto write = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1000, 150,
                          BuildWriteRowsBody(42, 7));
  ASSERT_EQ(engine.Feed(table_map.data(), table_map.size()), table_map.size());
  ASSERT_EQ(engine.Feed(write.data(), write.size()), write.size());

  ChangeEvent event;
  ASSERT_TRUE(engine.NextEvent(&event));
  EXPECT_EQ(event.source_sql, nullptr);
  EXPECT_TRUE(event.SourceSql().empty());
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

TEST(CdcEngineChecksumTest, PreVerifiedTrailerIsFramedWithoutBeingRevalidated) {
  // Fed from a producer that verified the trailer itself -- BinlogClient's
  // reader thread does -- the engine still frames the trailer out of the body
  // but no longer recomputes the CRC32, so it cannot fail an event on a
  // trailer it did not check.
  CdcEngine engine;
  engine.SetTrailerPreVerified(true);
  auto table_map = BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000, 100,
                              BuildTableMapBody(42, "testdb", "users"));
  auto write = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1000, 150,
                          BuildWriteRowsBody(42, 4242));
  write[write.size() - kChecksumSize] ^= 0xFF;

  ASSERT_EQ(engine.Feed(table_map.data(), table_map.size()), table_map.size());
  ASSERT_EQ(engine.Feed(write.data(), write.size()), write.size());
  EXPECT_FALSE(engine.IsError());
  EXPECT_EQ(engine.ErrorCode(), MES_OK);

  ChangeEvent event;
  ASSERT_TRUE(engine.NextEvent(&event));
  EXPECT_EQ(event.type, EventType::kInsert);
  ASSERT_EQ(event.after.columns.size(), 1u);
  EXPECT_EQ(event.after.columns[0].int_val, 4242);
  EXPECT_EQ(engine.CurrentPosition().offset, 150u);
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

TEST(CdcEngineNamesResolvedTest, FailedResolutionIsRetriedOnAByteIdenticalTableMap) {
  // The same TABLE_MAP precedes every ROWS event, so a resolution that failed
  // once must be attempted again when it arrives byte for byte again -- a
  // momentary metadata-connection failure must not leave the table reporting
  // positional column names for the rest of the binlog file.
  CdcEngine engine;
  MetadataFetcher fetcher;  // intentionally not Connect()ed
  engine.SetMetadataFetcher(&fetcher);
  // Each resolution attempt reaches the metadata connection, which fails and
  // reports it, so the number of reports is the number of attempts.
  ScopedEventLogCapture capture("event=metadata_reconnect_failed");

  auto table_map = BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000, 100,
                              BuildTableMapBody(42, "testdb", "users"));
  engine.Feed(table_map.data(), table_map.size());
  ASSERT_EQ(capture.Count(), 1) << capture.LastMessage();

  // The fetcher rate-limits reconnect attempts, so within the backoff window a
  // second attempt would not reach the metadata connection at all.
  MetadataFetcherTestAccess::ClearReconnectBackoff(&fetcher);
  engine.Feed(table_map.data(), table_map.size());
  EXPECT_EQ(capture.Count(), 2) << capture.LastMessage();
}

TEST(CdcEngineNamesResolvedTest, RetriedResolutionDoesNotWriteIntoMetadataQueuedEventsBorrow) {
  // A queued ChangeEvent reads its column names out of the TableMetadata it was
  // decoded against, so the retry above has to install a fresh object instead
  // of writing into that one. An in-place write would change what the earlier
  // event reports and, once a name outgrows its allocation, leave the views it
  // holds pointing at freed storage.
  CdcEngine engine;
  MetadataFetcher fetcher;  // intentionally not Connect()ed
  engine.SetMetadataFetcher(&fetcher);

  const auto table_map = BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000,
                                    100, BuildTableMapBody(42, "testdb", "users"));
  const auto first_write = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1001,
                                      200, BuildWriteRowsBody(42, 1));
  const auto second_write = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1002,
                                       300, BuildWriteRowsBody(42, 2));

  ASSERT_EQ(engine.Feed(table_map.data(), table_map.size()), table_map.size());
  ASSERT_EQ(engine.Feed(first_write.data(), first_write.size()), first_write.size());
  // The retry lands while the first event is still queued.
  MetadataFetcherTestAccess::ClearReconnectBackoff(&fetcher);
  ASSERT_EQ(engine.Feed(table_map.data(), table_map.size()), table_map.size());
  ASSERT_EQ(engine.Feed(second_write.data(), second_write.size()), second_write.size());
  ASSERT_EQ(engine.PendingEventCount(), 2u);

  std::vector<ChangeEvent> events;
  ChangeEvent event;
  while (engine.NextEvent(&event)) events.push_back(std::move(event));
  ASSERT_EQ(events.size(), 2u);
  ASSERT_TRUE(events[0].table_metadata);
  ASSERT_TRUE(events[1].table_metadata);
  EXPECT_NE(events[0].table_metadata.get(), events[1].table_metadata.get());
  // The first event still reads the storage of the registration it was decoded
  // against, which its own shared_ptr is what keeps alive.
  EXPECT_EQ(events[0].database.data(), events[0].table_metadata->database_name.data());
  EXPECT_EQ(events[0].table.data(), events[0].table_metadata->table_name.data());
  EXPECT_EQ(events[0].database, "testdb");
  EXPECT_EQ(events[0].table, "users");
}

TEST(CdcEngineDdlTest, TableMetadataCachedBeforeADdlStatementIsNotReusedAfterIt) {
  // A DDL statement can rename a column while leaving both the column count and
  // the TABLE_MAP body unchanged, so a registry entry that outlived it would
  // describe the pre-DDL schema. Nothing may be decoded from such an entry: the
  // server re-sends a TABLE_MAP before every ROWS event, which is what
  // repopulates the registry with the post-DDL schema.
  CdcEngine engine;
  auto table_map = BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000, 100,
                              BuildTableMapBody(42, "testdb", "users"));
  auto write = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1000, 150,
                          BuildWriteRowsBody(42, 1));
  engine.Feed(table_map.data(), table_map.size());
  engine.Feed(write.data(), write.size());

  ChangeEvent event;
  ASSERT_TRUE(engine.NextEvent(&event));
  ASSERT_FALSE(engine.IsError());

  auto ddl = BuildEvent(static_cast<uint8_t>(BinlogEventType::kQueryEvent), 1000, 200,
                        BuildQueryEventBody("testdb", "ALTER TABLE users RENAME COLUMN a TO b"));
  engine.Feed(ddl.data(), ddl.size());

  auto write_after = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1000, 250,
                                BuildWriteRowsBody(42, 2));
  engine.Feed(write_after.data(), write_after.size());

  EXPECT_FALSE(engine.NextEvent(&event));
  EXPECT_TRUE(engine.IsError());
  EXPECT_EQ(engine.ErrorCode(), MES_ERR_DECODE_ROW);
}

TEST(CdcEngineSignednessTest, UnavailableSignednessIsReportedOncePerTable) {
  // Without SIGNEDNESS in the TABLE_MAP and without a metadata side-connection,
  // every numeric column is decoded as signed, so an UNSIGNED value above the
  // signed range of its width reads as negative. That has to be observable, and
  // it has to be reported per table rather than per row event.
  ScopedEventLogCapture capture("event=table_map_missing_signedness");
  CdcEngine engine;
  auto table_map = BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000, 100,
                              BuildTableMapBody(42, "testdb", "users"));
  auto write = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1000, 150,
                          BuildWriteRowsBody(42, 1));
  for (int i = 0; i < 3; ++i) {
    engine.Feed(table_map.data(), table_map.size());
    engine.Feed(write.data(), write.size());
  }
  ASSERT_FALSE(engine.IsError());

  EXPECT_EQ(capture.Count(), 1);
  EXPECT_NE(capture.LastMessage().find("db=testdb"), std::string::npos) << capture.LastMessage();
  EXPECT_NE(capture.LastMessage().find("table=users"), std::string::npos) << capture.LastMessage();
}

TEST(CdcEngineSignednessTest, ResolvedSignednessIsNotReported) {
  // The TABLE_MAP carries the SIGNEDNESS optional metadata field, so signedness
  // is known and there is nothing to report.
  ScopedEventLogCapture capture("event=table_map_missing_signedness");
  CdcEngine engine;
  auto table_map =
      BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000, 100,
                 BuildTableMapBodyWithSignedness(42, "testdb", "users", /*is_unsigned=*/true));
  engine.Feed(table_map.data(), table_map.size());
  ASSERT_FALSE(engine.IsError());

  EXPECT_EQ(capture.Count(), 0) << capture.LastMessage();
}

TEST(CdcEngineChecksumTest, StreamWithChecksumsDisabledIsDecodedFromItsFormatDescription) {
  // The FDE of a binlog_checksum=NONE stream ends with the algorithm byte and
  // carries no trailer, and the post-header-length entry four bytes earlier can
  // equal the CRC32 algorithm code for an ordinary event-type table. The engine
  // must reframe the stream as unchecksummed instead of failing the first event
  // against a checksum that is not there.
  std::vector<uint8_t> fde_body(57 + 41, 0);
  fde_body[fde_body.size() - kChecksumSize - 1] = kBinlogChecksumAlgCrc32;
  fde_body[fde_body.size() - 1] = kBinlogChecksumAlgOff;

  CdcEngine engine;
  auto fde = BuildEventNoChecksum(static_cast<uint8_t>(BinlogEventType::kFormatDescriptionEvent),
                                  1000, 60, fde_body);
  auto table_map = BuildEventNoChecksum(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000,
                                        100, BuildTableMapBody(42, "testdb", "users"));
  auto write = BuildEventNoChecksum(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1000,
                                    150, BuildWriteRowsBody(42, 7));
  engine.Feed(fde.data(), fde.size());
  engine.Feed(table_map.data(), table_map.size());
  engine.Feed(write.data(), write.size());
  ASSERT_FALSE(engine.IsError()) << "error=" << engine.ErrorCode();

  ChangeEvent event;
  ASSERT_TRUE(engine.NextEvent(&event));
  ASSERT_EQ(event.after.columns.size(), 1u);
  EXPECT_EQ(event.after.columns[0].int_val, 7);
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

// The database and table names a ChangeEvent exposes are the TABLE_MAP's own
// storage, kept alive through table_metadata exactly as the column names in its
// rows are, so no row of the event holds a copy of them. Each view spans a whole
// owning string, which is what lets the C ABI hand out its data() as a
// NUL-terminated pointer.
TEST(CdcEngineTest, EventNamesBorrowTheTableMetadataStorageForEveryRow) {
  CdcEngine engine;
  const auto table_map = BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000,
                                    100, BuildTableMapBody(42, "testdb", "users"));
  const auto write = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1001, 200,
                                BuildWriteRowsBodyMultiRow(42, {1, 2, 3}));

  ASSERT_EQ(engine.Feed(table_map.data(), table_map.size()), table_map.size());
  ASSERT_EQ(engine.Feed(write.data(), write.size()), write.size());
  ASSERT_EQ(engine.PendingEventCount(), 3u);

  std::vector<ChangeEvent> events;
  ChangeEvent event;
  while (engine.NextEvent(&event)) events.push_back(std::move(event));
  ASSERT_EQ(events.size(), 3u);

  for (const ChangeEvent& row_event : events) {
    ASSERT_TRUE(row_event.table_metadata);
    EXPECT_EQ(row_event.database, "testdb");
    EXPECT_EQ(row_event.table, "users");
    // The metadata the event itself holds owns the bytes the views read.
    EXPECT_EQ(row_event.database.data(), row_event.table_metadata->database_name.data());
    EXPECT_EQ(row_event.table.data(), row_event.table_metadata->table_name.data());
    // A whole-string view, so the byte one past its end is the owner's
    // terminator and data() is a valid const char*.
    ASSERT_EQ(row_event.database.size(), row_event.table_metadata->database_name.size());
    ASSERT_EQ(row_event.table.size(), row_event.table_metadata->table_name.size());
    EXPECT_EQ(row_event.database.data()[row_event.database.size()], '\0');
    EXPECT_EQ(row_event.table.data()[row_event.table.size()], '\0');
    // One registration behind every row of the event, not one per row.
    EXPECT_EQ(row_event.database.data(), events.front().database.data());
    EXPECT_EQ(row_event.table.data(), events.front().table.data());
  }
}

// A queued event's names stay readable for the whole documented event lifetime,
// which outlasts the registry entry they came from: a ROTATE clears the registry
// and a re-registered table_id gets fresh storage, so each event has to hold the
// registration it was decoded against rather than whatever the registry holds
// when it is finally drained.
TEST(CdcEngineTest, QueuedEventNamesOutliveTheRegistryEntryTheyBorrow) {
  CdcEngine engine;
  const auto first_map = BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000,
                                    100, BuildTableMapBody(42, "testdb", "users"));
  const auto first_write = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1001,
                                      200, BuildWriteRowsBody(42, 1));
  const auto rotate = BuildEvent(static_cast<uint8_t>(BinlogEventType::kRotateEvent), 1002, 300,
                                 BuildRotateBody(4, "mysql-bin.000002"));
  // The same table_id after the rotation, naming a different table.
  const auto second_map = BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1003,
                                     400, BuildTableMapBody(42, "otherdb", "other_table"));
  const auto second_write = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1004,
                                       500, BuildWriteRowsBody(42, 2));

  std::vector<uint8_t> stream;
  for (const std::vector<uint8_t>* part :
       {&first_map, &first_write, &rotate, &second_map, &second_write}) {
    stream.insert(stream.end(), part->begin(), part->end());
  }
  ASSERT_EQ(engine.Feed(stream.data(), stream.size()), stream.size());
  ASSERT_FALSE(engine.IsError());
  ASSERT_EQ(engine.PendingEventCount(), 2u);

  std::vector<ChangeEvent> events;
  ChangeEvent event;
  while (engine.NextEvent(&event)) events.push_back(std::move(event));
  ASSERT_EQ(events.size(), 2u);

  EXPECT_EQ(events[0].database, "testdb");
  EXPECT_EQ(events[0].table, "users");
  EXPECT_EQ(events[1].database, "otherdb");
  EXPECT_EQ(events[1].table, "other_table");
  EXPECT_NE(events[0].database.data(), events[1].database.data());
  for (const ChangeEvent& row_event : events) {
    ASSERT_TRUE(row_event.table_metadata);
    EXPECT_EQ(row_event.database.data(), row_event.table_metadata->database_name.data());
    EXPECT_EQ(row_event.table.data(), row_event.table_metadata->table_name.data());
    EXPECT_EQ(row_event.database.data()[row_event.database.size()], '\0');
    EXPECT_EQ(row_event.table.data()[row_event.table.size()], '\0');
  }
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

TEST(CdcEngineTest, UnknownEventTypeIsParseErrorWithoutAdvancingCheckpoint) {
  CdcEngine engine;

  // Event type 99 is emitted by no supported server. Skipping it would advance
  // the caller's checkpoint past a change the engine could not decode.
  std::vector<uint8_t> body = {0x01, 0x02, 0x03, 0x04};
  auto event = BuildEvent(99, 1000, 100, body);

  ScopedErrorLogCapture capture;
  engine.Feed(event.data(), event.size());
  EXPECT_FALSE(engine.HasEvents());
  EXPECT_TRUE(engine.IsError());
  EXPECT_EQ(engine.ErrorCode(), MES_ERR_PARSE);
  EXPECT_EQ(engine.CurrentPosition().offset, 0u);
  EXPECT_NE(g_last_error_log.find("event=unknown_binlog_event"), std::string::npos);
}

TEST(CdcEngineTest, StopEventDoesNotInterruptSurroundingEvents) {
  CdcEngine engine;

  const auto table_map = BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000,
                                    100, BuildTableMapBody(1, "mydb", "users"));
  const auto write = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1001, 200,
                                BuildWriteRowsBody(1, 4242));
  // Every clean mysqld/mariadbd shutdown writes a STOP_EVENT, and the stream
  // resumes with the ROTATE that opens the next binlog file.
  const auto stop = BuildEvent(static_cast<uint8_t>(BinlogEventType::kStopEvent), 1002, 300, {});
  const auto rotate = BuildEvent(static_cast<uint8_t>(BinlogEventType::kRotateEvent), 1003, 400,
                                 BuildRotateBody(4, "mysql-bin.000002"));

  std::vector<uint8_t> stream;
  for (const std::vector<uint8_t>* event : {&table_map, &write, &stop, &rotate}) {
    stream.insert(stream.end(), event->begin(), event->end());
  }

  EXPECT_EQ(engine.Feed(stream.data(), stream.size()), stream.size());
  EXPECT_FALSE(engine.IsError());

  ChangeEvent event;
  ASSERT_TRUE(engine.NextEvent(&event));
  EXPECT_EQ(event.type, EventType::kInsert);
  EXPECT_EQ(event.table, "users");
  ASSERT_EQ(event.after.columns.size(), 1u);
  EXPECT_EQ(event.after.columns[0].int_val, 4242);
  EXPECT_FALSE(engine.HasEvents());

  // The ROTATE after the STOP was applied, so a reconnect resumes in the new
  // file instead of re-reading the STOP forever.
  EXPECT_EQ(engine.CurrentPosition().binlog_file, "mysql-bin.000002");
  EXPECT_EQ(engine.CurrentPosition().offset, 4u);
}

TEST(CdcEngineTest, StopEventAdvancesPositionWithoutError) {
  CdcEngine engine;
  const auto stop = BuildEvent(static_cast<uint8_t>(BinlogEventType::kStopEvent), 1000, 512, {});

  EXPECT_EQ(engine.Feed(stop.data(), stop.size()), stop.size());
  EXPECT_FALSE(engine.IsError());
  EXPECT_FALSE(engine.HasEvents());
  EXPECT_EQ(engine.CurrentPosition().offset, 512u);
}

TEST(CdcEngineTest, StandardControlEventsAdvancePositionWithoutError) {
  const uint8_t control_types[] = {
      static_cast<uint8_t>(BinlogEventType::kIntvarEvent),
      static_cast<uint8_t>(BinlogEventType::kRandEvent),
      static_cast<uint8_t>(BinlogEventType::kUserVarEvent),
      static_cast<uint8_t>(BinlogEventType::kAppendBlockEvent),
      static_cast<uint8_t>(BinlogEventType::kDeleteFileEvent),
      static_cast<uint8_t>(BinlogEventType::kBeginLoadQueryEvent),
      static_cast<uint8_t>(BinlogEventType::kExecuteLoadQueryEvent),
      static_cast<uint8_t>(BinlogEventType::kXaPrepareLogEvent),
  };

  for (uint8_t type_code : control_types) {
    SCOPED_TRACE(static_cast<int>(type_code));
    CdcEngine engine;
    const auto event = BuildEvent(type_code, 1000, 640, {0x01, 0x02, 0x03, 0x04});

    EXPECT_EQ(engine.Feed(event.data(), event.size()), event.size());
    EXPECT_FALSE(engine.IsError());
    EXPECT_FALSE(engine.HasEvents());
    EXPECT_EQ(engine.CurrentPosition().offset, 640u);
  }
}

TEST(CdcEngineTest, UnsupportedEventTypesFailDistinctlyFromUnknownOnes) {
  // INCIDENT reports that the server lost events; TRANSACTION_PAYLOAD carries
  // a compressed transaction. Both are documented types this engine cannot
  // represent, so they must fail as unsupported rather than as unknown.
  const uint8_t unsupported_types[] = {
      static_cast<uint8_t>(BinlogEventType::kIncidentEvent),
      static_cast<uint8_t>(BinlogEventType::kTransactionPayloadEvent),
  };

  for (uint8_t type_code : unsupported_types) {
    SCOPED_TRACE(static_cast<int>(type_code));
    CdcEngine engine;
    const auto event = BuildEvent(type_code, 1000, 700, {0x01, 0x02, 0x03, 0x04});

    ScopedErrorLogCapture capture;
    engine.Feed(event.data(), event.size());
    EXPECT_TRUE(engine.IsError());
    EXPECT_EQ(engine.ErrorCode(), MES_ERR_PARSE);
    EXPECT_EQ(engine.CurrentPosition().offset, 0u);
    EXPECT_NE(g_last_error_log.find("event=unsupported_binlog_event"), std::string::npos);
  }
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

/**
 * @brief The entry overshoot is every row of the event that crossed the limit.
 *
 * The cap is looked at once per binlog event while a ROWS event queues one
 * entry per row, and that gap is what the documented overshoot bound describes:
 * not a rounding allowance, but as many entries beyond the limit as the event
 * carried rows. A single-row body cannot tell those two readings apart, which
 * is why this feeds a body holding several.
 */
TEST(CdcEngineTest, QueueOvershootIsEveryRowOfTheEventThatCrossedTheLimit) {
  CdcEngine engine;
  engine.SetMaxQueueSize(1);

  auto table_map_body = BuildTableMapBody(1, "db", "t");
  auto table_map_event =
      BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000, 100, table_map_body);

  const std::vector<int32_t> rows = {10, 20, 30, 40, 50};
  auto write_event = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1001, 200,
                                BuildWriteRowsBodyMultiRow(1, rows));

  std::vector<uint8_t> combined;
  combined.insert(combined.end(), table_map_event.begin(), table_map_event.end());
  combined.insert(combined.end(), write_event.begin(), write_event.end());

  // Nothing stops this call: the queue is under the limit at every point the
  // limit is examined, which is once before the event rather than once per row.
  ASSERT_EQ(engine.Feed(combined.data(), combined.size()), combined.size());

  EXPECT_EQ(engine.PendingEventCount(), rows.size());
  EXPECT_GT(engine.PendingEventCount(), engine.MaxQueueSize());

  // The backlog is that one event's own rows, in order -- the bound is a
  // function of rows per event, not an unrelated amount of accumulated queue.
  ChangeEvent event;
  for (int32_t expected : rows) {
    ASSERT_TRUE(engine.NextEvent(&event));
    EXPECT_EQ(event.after.columns[0].int_val, expected);
  }
  EXPECT_FALSE(engine.HasEvents());
}

/**
 * @brief A drain-and-re-feed loop finishes a buffer that backpressure stopped.
 *
 * At a limit of one, the first Feed() returns short and the caller has to
 * alternate draining and re-feeding to get through the rest. What ends each
 * Feed() is the capacity check at the top of the outer loop: the stream parser
 * readies at most one event per call, so the check inside the per-event loop
 * sees the queue the outer one just accepted and cannot be the mechanism.
 * The property worth holding is that the loop terminates and delivers every
 * row once, which is what this asserts.
 */
TEST(CdcEngineTest, BackpressureLetsADrainAndRefeedLoopFinishTheBuffer) {
  CdcEngine engine;
  engine.SetMaxQueueSize(1);

  // Register table map
  auto table_map_body = BuildTableMapBody(1, "db", "t");
  auto table_map_event =
      BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000, 100, table_map_body);

  // Three single-row write events combined into one buffer, so that one Feed()
  // call has more than one event's worth of bytes available to it.
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

  // Feed the whole buffer; the capacity check ends the call after one event
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

TEST(CdcEngineTest, UnqualifiedExcludeWildcardIgnoresTheDatabaseName) {
  CdcEngine engine;
  // "log*" names bare tables, so it must not exclude logs.events on the
  // strength of the database name alone.
  engine.SetExcludeTables({"log*"});

  const auto table_map = BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000,
                                    100, BuildTableMapBody(1, "logs", "events"));
  const auto write = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1001, 200,
                                BuildWriteRowsBody(1, 7));
  ASSERT_EQ(engine.Feed(table_map.data(), table_map.size()), table_map.size());
  ASSERT_EQ(engine.Feed(write.data(), write.size()), write.size());

  ChangeEvent event;
  ASSERT_TRUE(engine.NextEvent(&event));
  EXPECT_EQ(event.database, "logs");
  EXPECT_EQ(event.table, "events");
  EXPECT_FALSE(engine.HasEvents());
}

TEST(CdcEngineTest, UnqualifiedIncludeWildcardIgnoresTheDatabaseName) {
  CdcEngine engine;
  // "user*" admits tables whose bare name starts with "user", not every table
  // that happens to live in the users_db database.
  engine.SetIncludeTables({"user*"});

  const auto sessions_map = BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000,
                                       100, BuildTableMapBody(1, "users_db", "sessions"));
  const auto profiles_map = BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1001,
                                       200, BuildTableMapBody(2, "users_db", "user_profiles"));
  ASSERT_EQ(engine.Feed(sessions_map.data(), sessions_map.size()), sessions_map.size());
  ASSERT_EQ(engine.Feed(profiles_map.data(), profiles_map.size()), profiles_map.size());

  const auto sessions_row = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1002,
                                       300, BuildWriteRowsBody(1, 10));
  const auto profiles_row = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1003,
                                       400, BuildWriteRowsBody(2, 20));
  ASSERT_EQ(engine.Feed(sessions_row.data(), sessions_row.size()), sessions_row.size());
  ASSERT_EQ(engine.Feed(profiles_row.data(), profiles_row.size()), profiles_row.size());

  ChangeEvent event;
  ASSERT_TRUE(engine.NextEvent(&event));
  EXPECT_EQ(event.table, "user_profiles");
  EXPECT_FALSE(engine.HasEvents());
  EXPECT_FALSE(engine.IsError());
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

// An engine held with static storage duration, the way a C++ consumer may keep
// one in a global. Its destructor runs during process teardown, after the point
// at which the logging state it emits through would ordinarily be destroyed.
CdcEngine g_static_engine;

void WriteMessageToStderr(mes_log_level_t, const char* message, void*) {
  std::fputs(message, stderr);
  std::fputc('\n', stderr);
  std::fflush(stderr);
}

TEST(CdcEngineTest, StaticStorageEngineCompletesDestructionAtProcessExit) {
  // The child gives the static engine an include filter that matches nothing,
  // so its destructor has a warning to emit, installs a callback (initializing
  // the logging state, hence after the engine was constructed) and exits. The
  // warning reaching stderr together with the exit status shows the destructor
  // ran to completion against logging machinery that was still alive.
  EXPECT_EXIT(
      {
        g_static_engine.SetIncludeTables({"mydb.missing"});
        const auto table_map = BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent),
                                          1000, 100, BuildTableMapBody(1, "mydb", "users"));
        g_static_engine.Feed(table_map.data(), table_map.size());
        LogConfig::SetCallback(WriteMessageToStderr, MES_LOG_WARN, nullptr);
        std::exit(0);
      },
      ::testing::ExitedWithCode(0), "event=include_filter_matched_nothing");
}

// A second static engine, used to keep decoded rows queued past the end of
// main(). Nothing in CdcEngine's constructor allocates a row, so the memory
// resource backing RowData::columns is first used later than this engine was
// constructed.
CdcEngine g_static_queued_rows_engine;

TEST(CdcEngineTest, StaticStorageEngineDestroysQueuedRowsAtProcessExit) {
  // The child queues several row events and deliberately never drains them, so
  // the pmr::vector<ColumnValue> of each one is still owned by the engine when
  // its destructor runs at process teardown. A clean exit shows those vectors
  // deallocated into a memory resource that was still alive at that point.
  EXPECT_EXIT(
      {
        const auto table_map = BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent),
                                          1000, 100, BuildTableMapBody(42, "testdb", "users"));
        const auto write = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1000,
                                      150, BuildWriteRowsBody(42, 7777));
        g_static_queued_rows_engine.Feed(table_map.data(), table_map.size());
        for (int i = 0; i < 8; ++i) {
          g_static_queued_rows_engine.Feed(write.data(), write.size());
        }
        if (!g_static_queued_rows_engine.HasEvents()) {
          std::fputs("no rows queued\n", stderr);
          std::exit(1);
        }
        std::fputs("rows left queued\n", stderr);
        std::fflush(stderr);
        std::exit(0);
      },
      ::testing::ExitedWithCode(0), "rows left queued");
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

// A MariaDB compressed column value: the method/length-size byte, the original
// length big-endian, then a raw deflate stream. Built with a four-byte original
// length so it matches the metadata the TABLE_MAP helper below writes.
std::vector<uint8_t> BuildCompressedColumnValue(const std::string& input) {
  std::vector<uint8_t> result;
  result.push_back(static_cast<uint8_t>(0x80 | 0x08 | 4));  // zlib, raw stream, 4 length bytes
  for (int shift = 24; shift >= 0; shift -= 8) {
    result.push_back(static_cast<uint8_t>(input.size() >> shift));
  }

  z_stream stream{};
  if (deflateInit2(&stream, Z_BEST_COMPRESSION, Z_DEFLATED, -MAX_WBITS, 8, Z_DEFAULT_STRATEGY) !=
      Z_OK) {
    return {};
  }
  const size_t payload_offset = result.size();
  result.resize(payload_offset + compressBound(static_cast<uLong>(input.size())));
  stream.next_in = reinterpret_cast<Bytef*>(const_cast<char*>(input.data()));
  stream.avail_in = static_cast<uInt>(input.size());
  stream.next_out = result.data() + payload_offset;
  stream.avail_out = static_cast<uInt>(result.size() - payload_offset);
  const int deflate_result = deflate(&stream, Z_FINISH);
  const size_t compressed_size = stream.total_out;
  if (deflate_result != Z_STREAM_END || deflateEnd(&stream) != Z_OK) return {};
  result.resize(payload_offset + compressed_size);
  return result;
}

// TABLE_MAP body for a table of one MariaDB compressed BLOB column.
std::vector<uint8_t> BuildCompressedBlobTableMapBody(uint64_t table_id, const std::string& db,
                                                     const std::string& table) {
  test::EventBuilder b;
  b.WriteU48Le(table_id);
  b.WriteU16Le(0);  // flags
  b.WriteU8(static_cast<uint8_t>(db.size()));
  b.WriteString(db);
  b.WriteU8(0);
  b.WriteU8(static_cast<uint8_t>(table.size()));
  b.WriteString(table);
  b.WriteU8(0);
  b.WriteU8(1);                                                  // column_count
  b.WriteU8(static_cast<uint8_t>(ColumnType::kBlobCompressed));  // column type
  b.WriteU8(1);                                                  // metadata length
  b.WriteU8(4);                                                  // four-byte length prefix
  b.WriteU8(0x01);                                               // nullable bitmap
  return b.Data();
}

// WRITE_ROWS_EVENT V2 body carrying one compressed BLOB value.
std::vector<uint8_t> BuildCompressedBlobWriteRowsBody(uint64_t table_id,
                                                      const std::vector<uint8_t>& value) {
  test::EventBuilder b;
  b.WriteU48Le(table_id);
  b.WriteU16Le(0);  // flags
  b.WriteU16Le(2);  // V2 var_header_len
  b.WriteU8(1);     // column_count
  b.WriteU8(0x01);  // columns_present
  b.WriteU8(0x00);  // null bitmap
  b.WriteU32Le(static_cast<uint32_t>(value.size()));
  b.WriteBytes(value);
  return b.Data();
}

// The queue's memory has to follow its byte budget, not its entry count: a
// compressed column's wire length says nothing about how much it decodes to, so
// a stream too small to fill the budget on the wire can hold many times the
// budget once decoded.
TEST(CdcEngineQueueBudgetTest, CompressedColumnExpansionStaysWithinTheQueueByteBudget) {
  constexpr uint64_t kTableId = 7;
  constexpr size_t kExpandedBytes = 64U * 1024U;
  constexpr size_t kQueueBytes = 256U * 1024U;
  constexpr size_t kRowEvents = 64;

  const auto value = BuildCompressedColumnValue(std::string(kExpandedBytes, 'a'));
  ASSERT_FALSE(value.empty());
  ASSERT_LT(value.size() * 100, kExpandedBytes) << "the column must expand by at least 100x";

  std::vector<uint8_t> stream =
      BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1, 100,
                 BuildCompressedBlobTableMapBody(kTableId, "mes_test", "compressed_blob"));
  const auto rows_body = BuildCompressedBlobWriteRowsBody(kTableId, value);
  for (size_t i = 0; i < kRowEvents; ++i) {
    const auto row_event = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1,
                                      static_cast<uint32_t>(200 + i), rows_body);
    stream.insert(stream.end(), row_event.begin(), row_event.end());
  }
  // Every wire byte of the whole stream still fits in the budget many times
  // over, so nothing but the decoded size can push the queue past it.
  ASSERT_LT(stream.size(), kQueueBytes);

  CdcEngine engine;
  engine.SetMaxQueueBytes(kQueueBytes);
  ASSERT_EQ(engine.MaxQueueBytes(), kQueueBytes);
  // The entry limit is left at its default, far above the events fed here, so
  // the byte budget is the only thing that can apply backpressure.
  ASSERT_GT(engine.MaxQueueSize(), kRowEvents);

  const size_t consumed = engine.Feed(stream.data(), stream.size());
  ASSERT_FALSE(engine.IsError());
  EXPECT_LT(consumed, stream.size()) << "the byte budget should have stopped the feed";
  EXPECT_LE(engine.QueuedBytes(), 2 * kQueueBytes);

  // Recount the decoded payloads rather than trusting the accounting under
  // test. The bound is the budget plus the one event Feed() pushes after its
  // last capacity check, whose decode budget is itself capped by the same
  // configured limit.
  size_t resident_bytes = 0;
  size_t queued_events = 0;
  ChangeEvent event;
  while (engine.NextEvent(&event)) {
    ASSERT_EQ(event.after.columns.size(), 1u);
    resident_bytes += event.after.columns[0].string_val.size();
    ++queued_events;
  }

  EXPECT_LE(resident_bytes, kQueueBytes + kExpandedBytes);
  EXPECT_LT(resident_bytes, 2 * kQueueBytes);
  EXPECT_LT(queued_events, kRowEvents) << "the entry count must not be what bounded the queue";
  EXPECT_GT(queued_events, 0u);
  EXPECT_EQ(engine.QueuedBytes(), 0u);
}

// ---- Resume position ----

// A filename and an offset the assertions below can compare against exactly,
// plus a registration for table_id 1 so a row event can be decoded from it.
constexpr const char* kPrimedBinlogFile = "mysql-bin.000042";
constexpr uint32_t kPrimedOffset = 100;

void PrimeResumePosition(CdcEngine* engine) {
  const auto rotate = BuildEvent(static_cast<uint8_t>(BinlogEventType::kRotateEvent), 1000, 50,
                                 BuildRotateBody(4, kPrimedBinlogFile));
  const auto table_map = BuildEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), 1000,
                                    kPrimedOffset, BuildTableMapBody(1, "db", "t"));
  ASSERT_EQ(engine->Feed(rotate.data(), rotate.size()), rotate.size());
  ASSERT_EQ(engine->Feed(table_map.data(), table_map.size()), table_map.size());
  ASSERT_FALSE(engine->IsError());
  ASSERT_EQ(engine->CurrentPosition().binlog_file, kPrimedBinlogFile);
  ASSERT_EQ(engine->CurrentPosition().offset, kPrimedOffset);
}

// Every event that fails to decode must leave the resume position exactly where
// it was, filename included, so a reconnect re-reads the offending event. A
// position that advanced past it makes a reconnect skip the rows it carried.
TEST(CdcEngineResumePositionTest, AFailedEventLeavesTheResumePositionExactlyWhereItWas) {
  struct FailingEvent {
    const char* name;
    uint8_t type_code;
    std::vector<uint8_t> body;
    mes_error_t error;
    /// Fragment of the structured log record that identifies the branch which
    /// rejected the event, so a case cannot pass on some other failure.
    const char* log_marker;
  };

  std::vector<uint8_t> truncated_rows = BuildWriteRowsBody(1, 42);
  truncated_rows.pop_back();

  const std::vector<FailingEvent> cases = {
      {"table map shorter than its table_id",
       static_cast<uint8_t>(BinlogEventType::kTableMapEvent),
       {0x02, 0x00, 0x00},
       MES_ERR_PARSE,
       "event=table_map_parse_failed reason=body_too_short"},
      {"table map carrying nothing but a table_id",
       static_cast<uint8_t>(BinlogEventType::kTableMapEvent),
       {0x02, 0x00, 0x00, 0x00, 0x00, 0x00},
       MES_ERR_PARSE,
       "event=table_map_parse_failed table_id=2 body_length=6"},
      {"truncated row event", static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent),
       truncated_rows, MES_ERR_DECODE_ROW, "event=row_decode_failed kind=write_rows"},
      {"row event for an unregistered table",
       static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), BuildWriteRowsBody(9, 42),
       MES_ERR_DECODE_ROW, "event=rows_event_no_table_map table_id=9"},
      {"row event shorter than its table_id",
       static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent),
       {0x00, 0x00, 0x00},
       MES_ERR_DECODE_ROW,
       "event=row_decode_failed kind=row_event_header reason=body_too_short"},
      {"annotate rows without a statement",
       static_cast<uint8_t>(BinlogEventType::kMariaDBAnnotateRowsEvent),
       {},
       MES_ERR_PARSE,
       "event=mariadb_annotate_rows_parse_failed"},
      {"rotate shorter than its position field",
       static_cast<uint8_t>(BinlogEventType::kRotateEvent),
       {0x00, 0x00, 0x00, 0x00},
       MES_ERR_PARSE,
       "event=rotate_event_parse_failed"},
      {"unsupported event type",
       static_cast<uint8_t>(BinlogEventType::kIncidentEvent),
       {0x01, 0x02, 0x03, 0x04},
       MES_ERR_PARSE,
       "event=unsupported_binlog_event type_code=26"},
      {"unknown event type",
       99,
       {0x01, 0x02, 0x03, 0x04},
       MES_ERR_PARSE,
       "event=unknown_binlog_event type_code=99"},
  };

  for (const FailingEvent& failing_case : cases) {
    SCOPED_TRACE(failing_case.name);
    CdcEngine engine;
    PrimeResumePosition(&engine);

    // A next_position far from the primed offset, so an advance shows up as an
    // exact value rather than as an off-by-one.
    const auto failing = BuildEvent(failing_case.type_code, 1001, 900, failing_case.body);
    ScopedEventLogCapture capture(failing_case.log_marker);
    engine.Feed(failing.data(), failing.size());

    ASSERT_TRUE(engine.IsError());
    EXPECT_EQ(engine.ErrorCode(), failing_case.error);
    EXPECT_GE(capture.Count(), 1) << "the intended branch did not report the failure";
    EXPECT_EQ(engine.CurrentPosition().offset, kPrimedOffset);
    EXPECT_EQ(engine.CurrentPosition().binlog_file, kPrimedBinlogFile);
  }
}

// Every event that decodes advances the resume position to its own
// next_position and touches nothing else, so a reconnect resumes after it
// instead of re-delivering the rows it carried.
TEST(CdcEngineResumePositionTest, ASucceedingEventAdvancesTheResumePositionToItsNextPosition) {
  struct SucceedingEvent {
    const char* name;
    uint8_t type_code;
    std::vector<uint8_t> body;
    uint32_t next_position;
  };

  const std::string annotated_sql = "INSERT INTO t VALUES (1)";
  // Ordered so the events that need the table_id 1 registration run before the
  // DDL statement that drops it.
  const std::vector<SucceedingEvent> cases = {
      {"single-row event", static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent),
       BuildWriteRowsBody(1, 42), 200},
      {"annotate rows", static_cast<uint8_t>(BinlogEventType::kMariaDBAnnotateRowsEvent),
       std::vector<uint8_t>(annotated_sql.begin(), annotated_sql.end()), 300},
      {"multi-row event", static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent),
       BuildWriteRowsBodyMultiRow(1, {1, 2, 3}), 400},
      {"control event", static_cast<uint8_t>(BinlogEventType::kXidEvent), {}, 500},
      {"non-DDL query", static_cast<uint8_t>(BinlogEventType::kQueryEvent),
       BuildQueryEventBody("db", "BEGIN"), 600},
      {"DDL query", static_cast<uint8_t>(BinlogEventType::kQueryEvent),
       BuildQueryEventBody("db", "ALTER TABLE t ADD c INT"), 700},
      {"table map", static_cast<uint8_t>(BinlogEventType::kTableMapEvent),
       BuildTableMapBody(2, "db", "t2"), 800},
  };

  CdcEngine engine;
  PrimeResumePosition(&engine);
  for (const SucceedingEvent& succeeding_case : cases) {
    SCOPED_TRACE(succeeding_case.name);
    const auto event = BuildEvent(succeeding_case.type_code, 1001, succeeding_case.next_position,
                                  succeeding_case.body);
    ASSERT_EQ(engine.Feed(event.data(), event.size()), event.size());
    ASSERT_FALSE(engine.IsError());
    EXPECT_EQ(engine.CurrentPosition().offset, succeeding_case.next_position);
    // Only a ROTATE event carries a filename, so nothing here may change it.
    EXPECT_EQ(engine.CurrentPosition().binlog_file, kPrimedBinlogFile);
  }
}

// A ROTATE resumes from the coordinates in its body, which is the one event
// whose resume position is not its header's next_position.
TEST(CdcEngineResumePositionTest, RotateTakesTheFileAndOffsetFromItsBodyNotItsNextPosition) {
  CdcEngine engine;
  PrimeResumePosition(&engine);

  const auto rotate = BuildEvent(static_cast<uint8_t>(BinlogEventType::kRotateEvent), 1001, 9999,
                                 BuildRotateBody(1234, "mysql-bin.000077"));
  ASSERT_EQ(engine.Feed(rotate.data(), rotate.size()), rotate.size());
  ASSERT_FALSE(engine.IsError());
  EXPECT_EQ(engine.CurrentPosition().binlog_file, "mysql-bin.000077");
  EXPECT_EQ(engine.CurrentPosition().offset, 1234u);
}

// Every row of one event resumes from the same coordinates: that event's
// next_position, in the file that applied when it was decoded. The filename is
// one copy shared by the rows rather than one copy per row, and a rotation that
// happens before the rows are drained must not retarget them.
TEST(CdcEngineResumePositionTest, EveryRowOfAnEventSharesOneCopyOfItsResumePosition) {
  CdcEngine engine;
  PrimeResumePosition(&engine);

  const auto write = BuildEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent), 1001, 500,
                                BuildWriteRowsBodyMultiRow(1, {1, 2, 3}));
  ASSERT_EQ(engine.Feed(write.data(), write.size()), write.size());
  ASSERT_EQ(engine.PendingEventCount(), 3u);

  const auto rotate = BuildEvent(static_cast<uint8_t>(BinlogEventType::kRotateEvent), 1002, 600,
                                 BuildRotateBody(4, "mysql-bin.000043"));
  ASSERT_EQ(engine.Feed(rotate.data(), rotate.size()), rotate.size());
  ASSERT_EQ(engine.CurrentPosition().binlog_file, "mysql-bin.000043");

  std::vector<ChangeEvent> events;
  ChangeEvent event;
  while (engine.NextEvent(&event)) events.push_back(std::move(event));
  ASSERT_EQ(events.size(), 3u);

  for (const ChangeEvent& row_event : events) {
    EXPECT_EQ(row_event.position.offset, 500u);
    EXPECT_EQ(row_event.position.BinlogFile(), kPrimedBinlogFile);
    EXPECT_EQ(&row_event.position.BinlogFile(), &events.front().position.BinlogFile());
  }
}

}  // namespace
}  // namespace mes
