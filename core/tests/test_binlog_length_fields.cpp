// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @file test_binlog_length_fields.cpp
 * @brief Packed-integer length fields and the column-count ceiling in binlog
 *        event bodies
 *
 * Every length, count or index a binlog event carries as a packed integer is
 * non-nullable, so the encoding's NULL marker (0xFB) is malformed input there
 * rather than the value 0. These tests pin each such read at the level of the
 * function that owns the event body, and they pin both sides of the
 * column-count ceiling.
 *
 * The cases are the cross product of an explicit parameter model rather than a
 * hand-picked selection: the read site, the server family whose column types
 * reach it, how the event enters the parser (direct call, or fed to the engine
 * with binlog_checksum ON or OFF), and whether the field carries a legal value
 * or the marker.
 */

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <set>
#include <string>
#include <vector>

#include "binary_util.h"
#include "cdc_engine.h"
#include "event_header.h"
#include "row_decoder.h"
#include "table_map.h"
#include "test_helpers.h"

namespace mes {
namespace {

// First byte of a packed integer that stands for NULL instead of a value.
constexpr uint8_t kNullMarkerByte = 0xFB;

constexpr uint64_t kTableId = 990;

/// A packed-integer field read from a binlog event body.
enum class LengthField {
  kColumnCount,             ///< TABLE_MAP column_count
  kMetadataLength,          ///< TABLE_MAP metadata block length
  kOptionalFieldLength,     ///< Length of one optional-metadata field
  kColumnNameLength,        ///< One column name inside COLUMN_NAME
  kDefaultCharsetDefault,   ///< DEFAULT_CHARSET's table-wide collation
  kDefaultCharsetIndex,     ///< DEFAULT_CHARSET's per-column index
  kDefaultCharsetOverride,  ///< DEFAULT_CHARSET's per-column collation
  kColumnCharsetValue,      ///< One collation inside COLUMN_CHARSET
  kRowsColumnCount,         ///< ROWS_EVENT column_count
};

/// Column types a server family writes for the same logical schema.
enum class SchemaShape {
  kMySql,    ///< VARCHAR and BLOB
  kMariaDb,  ///< The compressed variants of both
};

/// How a case reaches the parser.
enum class Entry {
  kDirect,            ///< The parse function called on the event body
  kEngineChecksum,    ///< Whole event fed to the engine, binlog_checksum=CRC32
  kEngineNoChecksum,  ///< Whole event fed to the engine, binlog_checksum=NONE
};

/// What the field under test carries.
enum class FieldValue {
  kLegal,       ///< A value a server can emit; the parse must accept it
  kNullMarker,  ///< 0xFB; the parse must fail
};

struct MarkerCase {
  LengthField field;
  SchemaShape shape;
  Entry entry;
  FieldValue value;
};

const char* FieldName(LengthField field) {
  switch (field) {
    case LengthField::kColumnCount:
      return "ColumnCount";
    case LengthField::kMetadataLength:
      return "MetadataLength";
    case LengthField::kOptionalFieldLength:
      return "OptionalFieldLength";
    case LengthField::kColumnNameLength:
      return "ColumnNameLength";
    case LengthField::kDefaultCharsetDefault:
      return "DefaultCharsetDefault";
    case LengthField::kDefaultCharsetIndex:
      return "DefaultCharsetIndex";
    case LengthField::kDefaultCharsetOverride:
      return "DefaultCharsetOverride";
    case LengthField::kColumnCharsetValue:
      return "ColumnCharsetValue";
    case LengthField::kRowsColumnCount:
      return "RowsColumnCount";
  }
  return "Unknown";
}

const char* ShapeName(SchemaShape shape) {
  return shape == SchemaShape::kMySql ? "MySql" : "MariaDb";
}

const char* EntryName(Entry entry) {
  switch (entry) {
    case Entry::kDirect:
      return "Direct";
    case Entry::kEngineChecksum:
      return "EngineChecksum";
    case Entry::kEngineNoChecksum:
      return "EngineNoChecksum";
  }
  return "Unknown";
}

const char* ValueName(FieldValue value) {
  return value == FieldValue::kLegal ? "Legal" : "NullMarker";
}

// The schema shape is a parameter only where the column types decide which
// branch of the metadata or charset walk performs the read. metadata_length
// and the optional-field length are reached with an all-fixed-size schema (see
// BuildTableMapBody), which no family writes differently, and a ROWS_EVENT
// body has one layout on both families.
std::vector<SchemaShape> ShapesFor(LengthField field) {
  switch (field) {
    case LengthField::kMetadataLength:
    case LengthField::kOptionalFieldLength:
    case LengthField::kRowsColumnCount:
      return {SchemaShape::kMySql};
    default:
      return {SchemaShape::kMySql, SchemaShape::kMariaDb};
  }
}

std::vector<MarkerCase> MarkerCases() {
  constexpr LengthField kFields[] = {
      LengthField::kColumnCount,
      LengthField::kMetadataLength,
      LengthField::kOptionalFieldLength,
      LengthField::kColumnNameLength,
      LengthField::kDefaultCharsetDefault,
      LengthField::kDefaultCharsetIndex,
      LengthField::kDefaultCharsetOverride,
      LengthField::kColumnCharsetValue,
      LengthField::kRowsColumnCount,
  };
  constexpr Entry kEntries[] = {Entry::kDirect, Entry::kEngineChecksum, Entry::kEngineNoChecksum};
  constexpr FieldValue kValues[] = {FieldValue::kLegal, FieldValue::kNullMarker};

  std::vector<MarkerCase> cases;
  for (LengthField field : kFields) {
    for (SchemaShape shape : ShapesFor(field)) {
      for (Entry entry : kEntries) {
        for (FieldValue value : kValues) {
          cases.push_back(MarkerCase{field, shape, entry, value});
        }
      }
    }
  }
  return cases;
}

// --- Wire builders ---

void WritePackedInt(test::EventBuilder& b, uint64_t value) {
  if (value < 251) {
    b.WriteU8(static_cast<uint8_t>(value));
  } else if (value <= 0xFFFF) {
    b.WriteU8(0xFC);
    b.WriteU16Le(static_cast<uint16_t>(value));
  } else if (value <= 0xFFFFFF) {
    b.WriteU8(0xFD);
    b.WriteU24Le(static_cast<uint32_t>(value));
  } else {
    b.WriteU8(0xFE);
    b.WriteU64Le(value);
  }
}

std::vector<uint8_t> ColumnTypeBytes(SchemaShape shape) {
  if (shape == SchemaShape::kMySql) {
    return {static_cast<uint8_t>(ColumnType::kLong), static_cast<uint8_t>(ColumnType::kVarchar),
            static_cast<uint8_t>(ColumnType::kBlob)};
  }
  return {static_cast<uint8_t>(ColumnType::kLong),
          static_cast<uint8_t>(ColumnType::kVarcharCompressed),
          static_cast<uint8_t>(ColumnType::kBlobCompressed)};
}

// Build a three-column TABLE_MAP body that reaches the read named by `field`.
// The legal and the marker variant differ in exactly the one byte that field
// occupies, so accepting the first and rejecting the second isolates the read.
std::vector<uint8_t> BuildTableMapBody(LengthField field, SchemaShape shape, FieldValue value) {
  const bool marker = value == FieldValue::kNullMarker;

  // metadata_length and the optional-field length are only pinned by a schema
  // whose legal length is 0: with any other schema a length of 0 fails further
  // along on the shifted offsets rather than on the field itself. Fixed-size
  // column types declare no metadata bytes.
  const bool fixed_size_schema =
      field == LengthField::kMetadataLength || field == LengthField::kOptionalFieldLength;

  const std::vector<uint8_t> types =
      fixed_size_schema ? std::vector<uint8_t>{static_cast<uint8_t>(ColumnType::kLong),
                                               static_cast<uint8_t>(ColumnType::kLongLong),
                                               static_cast<uint8_t>(ColumnType::kTiny)}
                        : ColumnTypeBytes(shape);
  // VARCHAR length 255 (2 bytes) plus the BLOB pack length (1 byte).
  const std::vector<uint8_t> column_metadata =
      fixed_size_schema ? std::vector<uint8_t>{} : std::vector<uint8_t>{0xFF, 0x00, 0x01};

  test::EventBuilder b;
  b.WriteU48Le(kTableId);
  b.WriteU16Le(0);  // flags
  b.WriteU8(2);
  b.WriteString("db");
  b.WriteU8(0);
  b.WriteU8(1);
  b.WriteString("t");
  b.WriteU8(0);

  b.WriteU8(marker && field == LengthField::kColumnCount ? kNullMarkerByte
                                                         : static_cast<uint8_t>(types.size()));
  b.WriteBytes(types);
  b.WriteU8(marker && field == LengthField::kMetadataLength
                ? kNullMarkerByte
                : static_cast<uint8_t>(column_metadata.size()));
  b.WriteBytes(column_metadata);
  b.WriteU8(0x07);  // null bitmap: three nullable columns

  switch (field) {
    case LengthField::kOptionalFieldLength:
      // COLUMN_VISIBILITY is skipped rather than decoded, so a zero-length
      // field is structurally complete and the length byte stands alone.
      b.WriteU8(12);
      b.WriteU8(marker ? kNullMarkerByte : 0);
      break;

    case LengthField::kColumnNameLength:
      // COLUMN_NAME: one length-encoded name per column.
      b.WriteU8(4);
      if (marker) {
        b.WriteU8(3);
        b.WriteU8(kNullMarkerByte);
        b.WriteU8(kNullMarkerByte);
        b.WriteU8(kNullMarkerByte);
      } else {
        b.WriteU8(6);
        for (const char* name : {"a", "b", "c"}) {
          b.WriteU8(1);
          b.WriteString(name);
        }
      }
      break;

    case LengthField::kDefaultCharsetDefault:
      // DEFAULT_CHARSET: the table-wide collation, then (index, collation)
      // overrides for the character columns that differ from it.
      b.WriteU8(2);
      b.WriteU8(1);
      b.WriteU8(marker ? kNullMarkerByte : 45);
      break;

    case LengthField::kDefaultCharsetIndex:
      b.WriteU8(2);
      b.WriteU8(3);
      b.WriteU8(45);
      b.WriteU8(marker ? kNullMarkerByte : 1);
      b.WriteU8(63);
      break;

    case LengthField::kDefaultCharsetOverride:
      b.WriteU8(2);
      b.WriteU8(3);
      b.WriteU8(45);
      b.WriteU8(1);
      b.WriteU8(marker ? kNullMarkerByte : 63);
      break;

    case LengthField::kColumnCharsetValue:
      // COLUMN_CHARSET: one collation per character column, in TABLE_MAP order.
      b.WriteU8(3);
      b.WriteU8(2);
      b.WriteU8(marker ? kNullMarkerByte : 45);
      b.WriteU8(63);
      break;

    default:
      // The read under test precedes the optional metadata block.
      break;
  }

  return b.Data();
}

// Build a TABLE_MAP body declaring `columns` TINYINT columns.
std::vector<uint8_t> BuildWideTableMapBody(uint64_t columns) {
  test::EventBuilder b;
  b.WriteU48Le(kTableId);
  b.WriteU16Le(0);
  b.WriteU8(2);
  b.WriteString("db");
  b.WriteU8(0);
  b.WriteU8(1);
  b.WriteString("t");
  b.WriteU8(0);
  WritePackedInt(b, columns);
  for (uint64_t i = 0; i < columns; ++i) {
    b.WriteU8(static_cast<uint8_t>(ColumnType::kTiny));
  }
  b.WriteU8(0);  // metadata_length: TINYINT declares no metadata
  for (size_t i = 0; i < binary::BitmapBytes(static_cast<size_t>(columns)); ++i) {
    b.WriteU8(0);  // null bitmap: no nullable column
  }
  return b.Data();
}

// Build a v2 WRITE_ROWS body for one fully present row of `columns` TINYINT
// values. `wire_column_count` is what the body declares, which the marker and
// ceiling cases vary independently of the image that follows.
std::vector<uint8_t> BuildWideWriteRowsBody(uint64_t columns, uint64_t wire_column_count) {
  test::EventBuilder b;
  b.WriteU48Le(kTableId);
  b.WriteU16Le(0);
  b.WriteU16Le(2);  // v2 variable header length
  WritePackedInt(b, wire_column_count);
  const size_t bitmap_bytes = binary::BitmapBytes(static_cast<size_t>(columns));
  for (size_t i = 0; i < bitmap_bytes; ++i) {
    b.WriteU8(0xFF);  // columns_present: every column
  }
  for (size_t i = 0; i < bitmap_bytes; ++i) {
    b.WriteU8(0);  // null bitmap
  }
  for (uint64_t i = 0; i < columns; ++i) {
    b.WriteU8(static_cast<uint8_t>(i & 0x7F));
  }
  return b.Data();
}

// A single-column WRITE_ROWS body whose declared column_count is the field
// under test.
std::vector<uint8_t> BuildRowsBodyWithColumnCount(FieldValue value) {
  test::EventBuilder b;
  b.WriteU48Le(kTableId);
  b.WriteU16Le(0);
  b.WriteU16Le(2);
  b.WriteU8(value == FieldValue::kNullMarker ? kNullMarkerByte : 1);
  b.WriteU8(0x01);  // columns_present
  b.WriteU8(0x00);  // null bitmap
  b.WriteU32Le(4242);
  return b.Data();
}

std::vector<uint8_t> WrapEvent(uint8_t type_code, const std::vector<uint8_t>& body, bool checksum) {
  return checksum ? test::BuildEvent(type_code, 1000, 100, body)
                  : test::BuildEventNoChecksum(type_code, 1000, 100, body);
}

// --- The NULL marker in every packed-integer field ---

class BinlogLengthMarkerTest : public ::testing::TestWithParam<MarkerCase> {};

TEST_P(BinlogLengthMarkerTest, MarkerFailsTheParseAndLegalValueParses) {
  const MarkerCase c = GetParam();
  const bool expect_parsed = c.value == FieldValue::kLegal;
  const bool rows_field = c.field == LengthField::kRowsColumnCount;

  const std::vector<uint8_t> table_map = rows_field ? test::BuildTableMapBody(kTableId, "db", "t")
                                                    : BuildTableMapBody(c.field, c.shape, c.value);

  if (c.entry == Entry::kDirect) {
    TableMetadata metadata;
    const bool table_map_parsed = ParseTableMapEvent(table_map.data(), table_map.size(), &metadata);
    if (!rows_field) {
      ASSERT_EQ(table_map_parsed, expect_parsed);
      if (expect_parsed) {
        EXPECT_EQ(metadata.columns.size(), 3u);
      }
      return;
    }

    ASSERT_TRUE(table_map_parsed);
    const std::vector<uint8_t> rows = BuildRowsBodyWithColumnCount(c.value);
    std::vector<RowData> decoded;
    ASSERT_EQ(DecodeWriteRows(rows.data(), rows.size(), metadata, true, &decoded), expect_parsed);
    if (expect_parsed) {
      ASSERT_EQ(decoded.size(), 1u);
      EXPECT_EQ(decoded[0].columns[0].int_val, 4242);
    }
    return;
  }

  const bool checksum = c.entry == Entry::kEngineChecksum;
  CdcEngine engine;
  engine.SetChecksumEnabled(checksum);

  const std::vector<uint8_t> table_map_event =
      WrapEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), table_map, checksum);
  ASSERT_EQ(engine.Feed(table_map_event.data(), table_map_event.size()), table_map_event.size());

  if (!rows_field) {
    EXPECT_EQ(engine.IsError(), !expect_parsed);
    if (!expect_parsed) {
      EXPECT_EQ(engine.ErrorCode(), MES_ERR_PARSE);
    }
    return;
  }

  ASSERT_FALSE(engine.IsError()) << "error=" << engine.ErrorCode();
  const std::vector<uint8_t> rows_event =
      WrapEvent(static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent),
                BuildRowsBodyWithColumnCount(c.value), checksum);
  ASSERT_EQ(engine.Feed(rows_event.data(), rows_event.size()), rows_event.size());

  ChangeEvent event;
  EXPECT_EQ(engine.NextEvent(&event), expect_parsed);
  EXPECT_EQ(engine.IsError(), !expect_parsed);
  if (!expect_parsed) {
    EXPECT_EQ(engine.ErrorCode(), MES_ERR_DECODE_ROW);
  }
}

INSTANTIATE_TEST_SUITE_P(AllFields, BinlogLengthMarkerTest, ::testing::ValuesIn(MarkerCases()),
                         [](const ::testing::TestParamInfo<MarkerCase>& info) {
                           return std::string(FieldName(info.param.field)) + "_" +
                                  ShapeName(info.param.shape) + "_" + EntryName(info.param.entry) +
                                  "_" + ValueName(info.param.value);
                         });

// --- Both sides of the column-count ceiling ---

/// Which parser bounds the column count.
enum class CountParser {
  kTableMap,   ///< TABLE_MAP column_count
  kRowsEvent,  ///< ROWS_EVENT column_count
};

struct CeilingCase {
  CountParser parser;
  uint64_t columns;
  Entry entry;
};

const char* ParserName(CountParser parser) {
  return parser == CountParser::kTableMap ? "TableMap" : "RowsEvent";
}

std::vector<CeilingCase> CeilingCases() {
  const uint64_t counts[] = {binary::kMaxTableColumns - 1, binary::kMaxTableColumns,
                             binary::kMaxTableColumns + 1, binary::kMaxTableColumns + 2};
  std::vector<CeilingCase> cases;
  for (uint64_t columns : counts) {
    for (Entry entry : {Entry::kDirect, Entry::kEngineChecksum, Entry::kEngineNoChecksum}) {
      cases.push_back(CeilingCase{CountParser::kTableMap, columns, entry});
    }
    // A count above the ceiling never survives a TABLE_MAP, so the metadata a
    // ROWS_EVENT is decoded against is built directly for every count and the
    // decoder is called on the body: an engine feed could not reach the
    // over-ceiling half of the boundary at all.
    cases.push_back(CeilingCase{CountParser::kRowsEvent, columns, Entry::kDirect});
  }
  return cases;
}

class ColumnCountCeilingTest : public ::testing::TestWithParam<CeilingCase> {};

TEST_P(ColumnCountCeilingTest, AcceptsUpToTheCeilingAndRejectsAbove) {
  const CeilingCase c = GetParam();
  const bool expect_accepted = c.columns <= binary::kMaxTableColumns;

  if (c.parser == CountParser::kRowsEvent) {
    TableMetadata metadata;
    metadata.table_id = kTableId;
    metadata.columns.assign(static_cast<size_t>(c.columns), ColumnMetadata{});
    for (auto& column : metadata.columns) {
      column.type = ColumnType::kTiny;
      column.is_nullable = false;
    }
    const std::vector<uint8_t> rows = BuildWideWriteRowsBody(c.columns, c.columns);
    std::vector<RowData> decoded;
    ASSERT_EQ(DecodeWriteRows(rows.data(), rows.size(), metadata, true, &decoded), expect_accepted);
    if (expect_accepted) {
      ASSERT_EQ(decoded.size(), 1u);
      EXPECT_EQ(decoded[0].columns.size(), static_cast<size_t>(c.columns));
    }
    return;
  }

  const std::vector<uint8_t> body = BuildWideTableMapBody(c.columns);

  if (c.entry == Entry::kDirect) {
    TableMetadata metadata;
    ASSERT_EQ(ParseTableMapEvent(body.data(), body.size(), &metadata), expect_accepted);
    if (expect_accepted) {
      EXPECT_EQ(metadata.columns.size(), static_cast<size_t>(c.columns));
    }
    return;
  }

  const bool checksum = c.entry == Entry::kEngineChecksum;
  CdcEngine engine;
  engine.SetChecksumEnabled(checksum);
  const std::vector<uint8_t> event =
      WrapEvent(static_cast<uint8_t>(BinlogEventType::kTableMapEvent), body, checksum);
  ASSERT_EQ(engine.Feed(event.data(), event.size()), event.size());
  EXPECT_EQ(engine.IsError(), !expect_accepted);
  if (!expect_accepted) {
    EXPECT_EQ(engine.ErrorCode(), MES_ERR_PARSE);
  }
}

INSTANTIATE_TEST_SUITE_P(AtTheBoundary, ColumnCountCeilingTest, ::testing::ValuesIn(CeilingCases()),
                         [](const ::testing::TestParamInfo<CeilingCase>& info) {
                           return std::string(ParserName(info.param.parser)) + "_" +
                                  std::to_string(info.param.columns) + "Columns_" +
                                  EntryName(info.param.entry);
                         });

// --- One definition of the ceiling ---

/** @brief The repository root, located relative to this test's own source. */
std::filesystem::path RepoRoot() {
  return std::filesystem::path(__FILE__).parent_path().parent_path().parent_path();
}

/** @brief Every regular file under @p root, or an empty list if it is unreadable. */
std::vector<std::filesystem::path> FilesUnder(const std::filesystem::path& root) {
  std::vector<std::filesystem::path> files;
  std::error_code ec;
  // The error_code overloads are used throughout: this translation unit is
  // built without exceptions, so a throwing filesystem call would abort.
  for (std::filesystem::recursive_directory_iterator it(root, ec), end; !ec && it != end;
       it.increment(ec)) {
    if (it->is_regular_file(ec)) {
      files.push_back(it->path());
    }
  }
  return files;
}

/** @brief Number of lines in @p file containing @p needle; -1 if unreadable. */
int CountLinesContaining(const std::filesystem::path& file, const std::string& needle) {
  std::ifstream in(file);
  if (!in.is_open()) {
    return -1;
  }
  int hits = 0;
  std::string line;
  while (std::getline(in, line)) {
    if (line.find(needle) != std::string::npos) {
      ++hits;
    }
  }
  return hits;
}

/** @brief Number of lines in @p file containing both needles; -1 if unreadable. */
int CountLinesContainingBoth(const std::filesystem::path& file, const std::string& first,
                             const std::string& second) {
  std::ifstream in(file);
  if (!in.is_open()) {
    return -1;
  }
  int hits = 0;
  std::string line;
  while (std::getline(in, line)) {
    if (line.find(first) != std::string::npos && line.find(second) != std::string::npos) {
      ++hits;
    }
  }
  return hits;
}

/**
 * @brief The binlog column-count ceiling has one definition and every parser
 *        bounding a column count reads it.
 *
 * A second constant holding the same value changes no behaviour until one of
 * the two is edited, so no test of the parsers can observe it. Scanning the
 * sources is what proves its absence, and that absence is the point: the
 * TABLE_MAP parser and the ROWS_EVENT parser must not be able to disagree on
 * the ceiling.
 *
 * Every pattern below is assembled from fragments rather than written out, so
 * that this file does not become a hit in the searches it exists to keep empty.
 */
TEST(ColumnCeilingTest, IsDefinedOnceAndReadByEveryBoundingParser) {
  const std::string ceiling_value = std::string("40") + "96";
  const std::string definition_keyword = std::string("const") + "expr";
  const std::string shared_name = std::string("kMaxTable") + "Columns";
  const std::string private_name = std::string("kMax") + "Columns";

  const std::filesystem::path root = RepoRoot();
  const std::filesystem::path definition_file = root / "core" / "src" / "binary_util.h";
  // A COM_QUERY result set is bounded independently of a binlog table, for the
  // reason recorded at that constant, so its value is expected here.
  const std::filesystem::path result_set_bound =
      root / "core" / "src" / "protocol" / "mysql_query.cpp";
  const std::filesystem::path bounding_parsers[] = {
      root / "core" / "src" / "table_map.cpp",
      root / "core" / "src" / "row_decoder.cpp",
  };
  const std::filesystem::path scanned_roots[] = {
      root / "core" / "src",
      root / "core" / "include",
      root / "bindings" / "node" / "src",
      root / "bindings" / "python" / "src",
  };

  std::vector<std::filesystem::path> files;
  for (const std::filesystem::path& scanned : scanned_roots) {
    const std::vector<std::filesystem::path> found = FilesUnder(scanned);
    ASSERT_FALSE(found.empty()) << "no files found under " << scanned;
    files.insert(files.end(), found.begin(), found.end());
  }

  std::set<std::string> value_files;
  int private_name_hits = 0;
  for (const std::filesystem::path& file : files) {
    const int value_hits = CountLinesContaining(file, ceiling_value);
    const int private_hits = CountLinesContaining(file, private_name);
    ASSERT_GE(value_hits, 0) << "cannot read " << file;
    ASSERT_GE(private_hits, 0) << "cannot read " << file;
    if (value_hits > 0) {
      value_files.insert(file.string());
    }
    private_name_hits += private_hits;
  }

  // Reading the definition through its own name is what proves the scan is
  // looking at the sources it is meant to: without this the assertions below
  // would pass over an empty or mislocated file list.
  ASSERT_GT(CountLinesContaining(definition_file, shared_name), 0)
      << "the shared constant was not found at " << definition_file;

  const std::set<std::string> expected = {definition_file.string(), result_set_bound.string()};
  EXPECT_EQ(value_files, expected);
  EXPECT_EQ(CountLinesContainingBoth(definition_file, definition_keyword, ceiling_value), 1);
  EXPECT_EQ(private_name_hits, 0) << "a column ceiling is defined outside " << definition_file;

  for (const std::filesystem::path& parser : bounding_parsers) {
    EXPECT_GT(CountLinesContaining(parser, shared_name), 0)
        << parser << " bounds a column count without reading the shared constant";
  }
}

}  // namespace
}  // namespace mes
