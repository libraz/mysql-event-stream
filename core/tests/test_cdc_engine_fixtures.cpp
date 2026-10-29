// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @file test_cdc_engine_fixtures.cpp
 * @brief CdcEngine over binlog files captured from real servers
 *
 * The captures in fixtures/ are whole binlog files copied byte for byte (see
 * libmybinlog's tests/fixtures/README.md for how they were taken). The engine is
 * fed the events raw, the way a caller replaying a binlog file does, and what it
 * emits is compared with what libmybinlog decodes from the same bytes.
 */

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <set>
#include <unordered_map>
#include <vector>

#include "cdc_engine.h"
#include "mybinlog/checksum.h"
#include "mybinlog/event_header.h"
#include "mybinlog/event_types.h"
#include "mybinlog/row_decoder.h"
#include "mybinlog/server_flavor.h"
#include "mybinlog/status.h"
#include "mybinlog/table_map.h"
#include "source_scan.h"

namespace mes {
namespace {

std::vector<uint8_t> ReadFixture(const char* name) {
  const std::filesystem::path path = source_scan::RepoRoot() / "core" / "tests" / "fixtures" / name;
  std::ifstream in(path, std::ios::binary);
  return std::vector<uint8_t>(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
}

/** @brief What a capture holds, decoded independently of the engine. */
struct CaptureContents {
  size_t rows_events = 0;
  size_t row_changes = 0;  // one per WRITE/DELETE row and per UPDATE pair
};

// Frames every event with the shared checksum tracker and decodes each rows
// event against the TABLE_MAP before it.
bool DecodeCapture(const std::vector<uint8_t>& events, CaptureContents* out) {
  mybinlog::ChecksumTracker tracker;
  std::unordered_map<uint64_t, mybinlog::TableMap> tables;
  size_t offset = 0;
  while (offset < events.size()) {
    mybinlog::EventHeader header;
    if (mybinlog::ParseEventHeader(events.data() + offset, events.size() - offset, &header) !=
            mybinlog::Status::kOk ||
        header.event_length > events.size() - offset) {
      return false;
    }
    const uint8_t* frame = events.data() + offset;
    mybinlog::FrameLayout layout;
    if (tracker.OnEvent(header, frame, header.event_length, &layout) != mybinlog::Status::kOk) {
      return false;
    }
    const uint8_t* body = frame + mybinlog::kEventHeaderSize;
    const size_t body_size = layout.body_length();
    if (header.type_code == static_cast<uint8_t>(mybinlog::EventType::kTableMapEvent)) {
      mybinlog::TableMap table;
      if (mybinlog::ParseTableMapEvent(header, body, body_size, mybinlog::ServerFlavor::kMySQL,
                                       &table) != mybinlog::Status::kOk) {
        return false;
      }
      tables[table.table_id] = std::move(table);
    } else if (mybinlog::IsRowsEvent(header.type_code)) {
      mybinlog::RowsEventHeader rows_header;
      if (mybinlog::ParseRowsEventHeader(header, body, body_size, &rows_header) !=
              mybinlog::Status::kOk ||
          tables.count(rows_header.table_id) == 0) {
        return false;
      }
      mybinlog::RowsEvent rows;
      if (mybinlog::DecodeRowsEvent(header, body, body_size, tables[rows_header.table_id],
                                    mybinlog::DecodeOptions{}, &rows) != mybinlog::Status::kOk) {
        return false;
      }
      ++out->rows_events;
      out->row_changes += rows.rows.size() + rows.updates.size();
    }
    offset += header.event_length;
  }
  return true;
}

// A binlog_checksum=NONE capture still ends its FDE with a trailer, and no other
// event has one. Read raw, the engine must take its framing from that FDE rather
// than reject the stream as a checksum mismatch, and emit every row (MES-1).
TEST(CdcEngineFixtures, RawFeedNoneChecksum) {
  const std::vector<uint8_t> file = ReadFixture("mysql84_none_tagged.bin");
  ASSERT_GT(file.size(), mybinlog::kBinlogMagic.size());
  ASSERT_TRUE(
      std::equal(mybinlog::kBinlogMagic.begin(), mybinlog::kBinlogMagic.end(), file.begin()));
  const std::vector<uint8_t> events(file.begin() + mybinlog::kBinlogMagic.size(), file.end());

  CaptureContents expected;
  ASSERT_TRUE(DecodeCapture(events, &expected));
  ASSERT_GT(expected.rows_events, 0u);

  CdcEngine engine;  // the default CRC32 framing; the capture's FDE must override it
  size_t fed = 0;
  size_t changes = 0;
  std::set<uint64_t> rows_event_offsets;
  while (fed < events.size()) {
    const size_t consumed = engine.Feed(events.data() + fed, events.size() - fed);
    ASSERT_NE(engine.ErrorCode(), MES_ERR_CHECKSUM);
    ASSERT_FALSE(engine.IsError()) << "error=" << engine.ErrorCode() << " at byte " << fed;
    ChangeEvent event;
    while (engine.NextEvent(&event)) {
      ++changes;
      rows_event_offsets.insert(event.position.offset);
      EXPECT_EQ(event.database, "fx");
      EXPECT_EQ(event.table, "t");
    }
    if (consumed == 0) break;
    fed += consumed;
  }

  EXPECT_EQ(fed, events.size());
  EXPECT_FALSE(engine.IsError());
  EXPECT_EQ(changes, expected.row_changes);
  // Every row of one rows event resumes from that event's end, so the distinct
  // resume offsets count the rows events.
  EXPECT_EQ(rows_event_offsets.size(), expected.rows_events);
}

}  // namespace
}  // namespace mes
