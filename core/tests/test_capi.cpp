// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <vector>

#include "event_header.h"
#include "mes.h"
#include "test_helpers.h"

namespace {

using mes::BinlogEventType;
using mes::test::BuildDeleteRowsBody;
using mes::test::BuildEvent;
using mes::test::BuildRotateBody;
using mes::test::BuildTableMapBody;
using mes::test::BuildUpdateRowsBody;
using mes::test::BuildWriteRowsBody;

// ---- Engine lifecycle ----

TEST(CApi, CreateAndDestroy) {
  mes_engine_t* engine = mes_create();
  ASSERT_NE(engine, nullptr);
  mes_destroy(engine);
}

TEST(CApi, DestroyNull) {
  mes_destroy(nullptr);  // Should not crash
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
  EXPECT_EQ(mes_has_events(engine), 0);

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

TEST(CApi, SetMaxQueueSizeZeroUnlimited) {
  auto* engine = mes_create();

  // Setting max_queue_size to 0 means unlimited
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
  // With unlimited queue, all data should be consumed
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

}  // namespace
