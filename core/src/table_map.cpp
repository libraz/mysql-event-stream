// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include "table_map.h"

#include <algorithm>
#include <cstring>

#include "binary_util.h"

namespace mes {

namespace {

// Maximum column count for safety.
constexpr size_t kMaxColumns = 4096;

struct MetadataValue {
  uint32_t value = 0;
  size_t consumed = 0;
  bool valid = true;
  bool is_array = false;
  ColumnType array_element_type = ColumnType::kLong;
};

// Read per-column metadata from the metadata block. A zero-byte result is
// valid for fixed-size types, so malformed input is reported separately via
// `valid`.
MetadataValue ReadColumnMetadataValue(uint8_t col_type, const uint8_t* data, size_t remaining,
                                      bool array_element = false) {
  MetadataValue result;
  auto require = [&](size_t bytes) {
    if (remaining < bytes) {
      result.valid = false;
      return false;
    }
    result.consumed = bytes;
    return true;
  };

  if (col_type == static_cast<uint8_t>(ColumnType::kTypedArray)) {
    if (!require(1)) return result;
    const uint8_t element_type = binary::ReadU8(data);

    // Field_typed_array::do_save_field_metadata only emits these element
    // types. Rejecting any other value prevents a corrupt element marker from
    // shifting the metadata cursor into the following column.
    switch (static_cast<ColumnType>(element_type)) {
      case ColumnType::kVarchar:
      case ColumnType::kNewDecimal:
      case ColumnType::kLongLong:
      case ColumnType::kNewDate:
      case ColumnType::kTime2:
      case ColumnType::kDatetime2:
      case ColumnType::kYear:
        break;
      default:
        result.valid = false;
        return result;
    }

    MetadataValue element = ReadColumnMetadataValue(element_type, data + 1, remaining - 1, true);
    if (!element.valid) {
      result.valid = false;
      return result;
    }
    result.value = element.value;
    result.consumed = 1 + element.consumed;
    result.is_array = true;
    result.array_element_type = static_cast<ColumnType>(element_type);
    return result;
  }

  switch (col_type) {
    case static_cast<uint8_t>(ColumnType::kVarchar):
    case static_cast<uint8_t>(ColumnType::kVarString):
    case static_cast<uint8_t>(ColumnType::kVarcharCompressed):
      if (array_element) {
        if (!require(3)) return result;
        result.value = binary::ReadU24Le(data);
        return result;
      }
      if (!require(2)) return result;
      result.value = binary::ReadU16Le(data);
      return result;

    case static_cast<uint8_t>(ColumnType::kBlob):
    case static_cast<uint8_t>(ColumnType::kBlobCompressed):
    case static_cast<uint8_t>(ColumnType::kTinyBlob):
    case static_cast<uint8_t>(ColumnType::kMediumBlob):
    case static_cast<uint8_t>(ColumnType::kLongBlob):
    case static_cast<uint8_t>(ColumnType::kGeometry):
    case static_cast<uint8_t>(ColumnType::kJson):
    case static_cast<uint8_t>(ColumnType::kVector):
      if (!require(1)) return result;
      result.value = binary::ReadU8(data);
      return result;

    case static_cast<uint8_t>(ColumnType::kFloat):
    case static_cast<uint8_t>(ColumnType::kDouble):
      if (!require(1)) return result;
      result.value = binary::ReadU8(data);
      return result;

    case static_cast<uint8_t>(ColumnType::kNewDecimal):
      if (!require(2)) return result;
      result.value = static_cast<uint32_t>((binary::ReadU8(data) << 8) | binary::ReadU8(data + 1));
      return result;

    case static_cast<uint8_t>(ColumnType::kBit):
      if (!require(2)) return result;
      result.value = static_cast<uint32_t>((binary::ReadU8(data + 1) << 8) | binary::ReadU8(data));
      return result;

    case static_cast<uint8_t>(ColumnType::kEnum):
    case static_cast<uint8_t>(ColumnType::kSet):
      if (!require(2)) return result;
      result.value = binary::ReadU16Le(data);
      return result;

    case static_cast<uint8_t>(ColumnType::kString):
      if (!require(2)) return result;
      result.value = static_cast<uint32_t>((binary::ReadU8(data) << 8) | binary::ReadU8(data + 1));
      return result;

    case static_cast<uint8_t>(ColumnType::kTimestamp2):
    case static_cast<uint8_t>(ColumnType::kDatetime2):
    case static_cast<uint8_t>(ColumnType::kTime2):
      if (!require(1)) return result;
      result.value = binary::ReadU8(data);
      return result;

    // Fixed-size types: no metadata bytes
    case static_cast<uint8_t>(ColumnType::kTiny):
    case static_cast<uint8_t>(ColumnType::kShort):
    case static_cast<uint8_t>(ColumnType::kLong):
    case static_cast<uint8_t>(ColumnType::kLongLong):
    case static_cast<uint8_t>(ColumnType::kInt24):
    case static_cast<uint8_t>(ColumnType::kYear):
    case static_cast<uint8_t>(ColumnType::kDate):
    case static_cast<uint8_t>(ColumnType::kNewDate):
    case static_cast<uint8_t>(ColumnType::kTime):
    case static_cast<uint8_t>(ColumnType::kDatetime):
    case static_cast<uint8_t>(ColumnType::kTimestamp):
      return result;

    default:
      // Unknown column type: assume no metadata bytes. If the type
      // actually has metadata, the equality check after the loop will
      // catch the mismatch and fail the parse.
      return result;
  }
}

// Optional metadata field types appended to a TABLE_MAP event when
// binlog_row_metadata is enabled (SIGNEDNESS is present even in MINIMAL mode,
// the MySQL default; COLUMN_NAME requires FULL). Values match MySQL's
// Table_map_event::Optional_metadata_field_type.
enum class OptionalMetadataFieldType : uint8_t {
  kSignedness = 1,
  kDefaultCharset = 2,
  kColumnCharset = 3,
  kColumnName = 4,
  kSetStrValue = 5,
  kEnumStrValue = 6,
  kGeometryType = 7,
  kSimplePrimaryKey = 8,
  kPrimaryKeyWithPrefix = 9,
  kEnumAndSetDefaultCharset = 10,
  kEnumAndSetColumnCharset = 11,
  kColumnVisibility = 12,
  kVectorDimensionality = 13,
};

// MySQL's Field::has_signedness_information_type() decides which TABLE_MAP
// columns receive a SIGNEDNESS bit. This is deliberately based on the actual
// binlog column type: TYPED_ARRAY itself has no bit, even when its element
// type is numeric.
bool IsNumericColumnType(const ColumnMetadata& column) {
  const uint8_t col_type = static_cast<uint8_t>(column.type);
  switch (col_type) {
    case static_cast<uint8_t>(ColumnType::kDecimal):
    case static_cast<uint8_t>(ColumnType::kTiny):
    case static_cast<uint8_t>(ColumnType::kShort):
    case static_cast<uint8_t>(ColumnType::kInt24):
    case static_cast<uint8_t>(ColumnType::kLong):
    case static_cast<uint8_t>(ColumnType::kLongLong):
    case static_cast<uint8_t>(ColumnType::kNewDecimal):
    case static_cast<uint8_t>(ColumnType::kFloat):
    case static_cast<uint8_t>(ColumnType::kDouble):
    case static_cast<uint8_t>(ColumnType::kYear):
      return true;
    default:
      return false;
  }
}

// Apply a complete SIGNEDNESS bitmap to the relevant columns of `metadata`.
// A truncated bitmap is malformed; do not claim that signedness is
// authoritative when values would otherwise silently retain their defaults.
bool ApplySignedness(const uint8_t* bitmap, size_t bitmap_len, TableMetadata* metadata) {
  const size_t numeric_count = static_cast<size_t>(
      std::count_if(metadata->columns.begin(), metadata->columns.end(), IsNumericColumnType));
  const size_t required_bytes = (numeric_count + 7) / 8;
  if (bitmap_len < required_bytes) {
    return false;
  }

  size_t numeric_index = 0;
  for (auto& column : metadata->columns) {
    if (!IsNumericColumnType(column)) {
      continue;
    }
    size_t byte_index = numeric_index / 8;
    if (byte_index >= bitmap_len) {
      break;
    }
    // MySQL packs signedness MSB-first within each byte.
    uint8_t bit = static_cast<uint8_t>(0x80 >> (numeric_index % 8));
    column.is_unsigned = (bitmap[byte_index] & bit) != 0;
    ++numeric_index;
  }
  metadata->signedness_from_binlog = true;
  return true;
}

// Apply COLUMN_NAME entries (one length-encoded string per column).
// Returns false unless the field contains exactly one complete name for every
// column and no trailing bytes.
bool ApplyColumnNames(const uint8_t* data, size_t value_len, TableMetadata* metadata) {
  size_t pos = 0;
  for (auto& column : metadata->columns) {
    if (pos >= value_len) return false;

    size_t packed_bytes = 0;
    uint64_t name_len = binary::ReadPackedInt(data + pos, value_len - pos, packed_bytes);
    if (packed_bytes == 0 || packed_bytes > value_len - pos) return false;
    pos += packed_bytes;
    if (name_len > value_len - pos) return false;

    const size_t name_size = static_cast<size_t>(name_len);
    column.name = std::string(reinterpret_cast<const char*>(data + pos), name_size);
    pos += name_size;
  }
  return pos == value_len;
}

// MySQL's is_character_type() decides which columns the DEFAULT_CHARSET and
// COLUMN_CHARSET fields index, and it is applied to each column's *real* type.
// The index space must be reproduced exactly, because both fields are packed
// positionally: counting one column too many or too few shifts every
// following collation id onto the wrong column.
//
// ENUM and SET travel on the wire as MYSQL_TYPE_STRING and are therefore
// easily miscounted here. The server keeps their collations in the separate
// ENUM_AND_SET_DEFAULT_CHARSET / ENUM_AND_SET_COLUMN_CHARSET fields, so they
// must not consume a slot. VECTOR (MySQL 9.0+) does consume one; its
// collation is always binary.
bool IsCharacterColumnType(const ColumnMetadata& column) {
  switch (column.type) {
    case ColumnType::kString: {
      // Field_enum and Field_set write their real type into metadata byte 0.
      // A CHAR column never collides with those values because byte 0 carries
      // MYSQL_TYPE_STRING XORed with the high bits of the field length.
      const uint8_t real_type = static_cast<uint8_t>(column.metadata >> 8);
      return real_type != static_cast<uint8_t>(ColumnType::kEnum) &&
             real_type != static_cast<uint8_t>(ColumnType::kSet);
    }
    case ColumnType::kVarchar:
    case ColumnType::kVarcharCompressed:
    case ColumnType::kVarString:
    case ColumnType::kVector:
    case ColumnType::kBlob:
    case ColumnType::kBlobCompressed:
    case ColumnType::kTinyBlob:
    case ColumnType::kMediumBlob:
    case ColumnType::kLongBlob:
      return true;
    // Servers transmit ENUM and SET as MYSQL_TYPE_STRING, but reject them
    // explicitly as well so a raw type byte cannot re-open the shift.
    case ColumnType::kEnum:
    case ColumnType::kSet:
      return false;
    default:
      return false;
  }
}

bool ReadPackedField(const uint8_t* data, size_t len, size_t* offset, uint32_t* value) {
  if (*offset >= len) return false;
  size_t packed_bytes = 0;
  const uint64_t packed = binary::ReadPackedInt(data + *offset, len - *offset, packed_bytes);
  if (packed_bytes == 0 || packed_bytes > len - *offset || packed > UINT32_MAX) return false;
  *offset += packed_bytes;
  *value = static_cast<uint32_t>(packed);
  return true;
}

std::vector<ColumnMetadata*> CharacterColumns(TableMetadata* metadata) {
  std::vector<ColumnMetadata*> columns;
  for (auto& column : metadata->columns) {
    if (IsCharacterColumnType(column)) columns.push_back(&column);
  }
  return columns;
}

bool ApplyDefaultCharset(const uint8_t* data, size_t len, TableMetadata* metadata) {
  size_t offset = 0;
  uint32_t default_charset = 0;
  if (!ReadPackedField(data, len, &offset, &default_charset)) return false;
  auto columns = CharacterColumns(metadata);
  for (auto* column : columns) {
    column->charset_id = default_charset;
    column->charset_known = true;
  }
  while (offset < len) {
    uint32_t column_index = 0;
    uint32_t charset = 0;
    if (!ReadPackedField(data, len, &offset, &column_index) ||
        !ReadPackedField(data, len, &offset, &charset) || column_index >= columns.size()) {
      return false;
    }
    columns[column_index]->charset_id = charset;
  }
  return offset == len;
}

bool ApplyColumnCharset(const uint8_t* data, size_t len, TableMetadata* metadata) {
  size_t offset = 0;
  auto columns = CharacterColumns(metadata);
  for (auto* column : columns) {
    uint32_t charset = 0;
    if (!ReadPackedField(data, len, &offset, &charset)) return false;
    column->charset_id = charset;
    column->charset_known = true;
  }
  return offset == len;
}

// Parse the optional metadata block following the null bitmap. Unknown field
// types are skipped, but every TLV must be structurally complete.
bool ParseOptionalMetadata(const uint8_t* data, size_t offset, size_t len,
                           TableMetadata* metadata) {
  while (offset < len) {
    uint8_t field_type = binary::ReadU8(data + offset);
    offset += 1;
    if (offset >= len) return false;

    size_t packed_bytes = 0;
    uint64_t field_len = binary::ReadPackedInt(data + offset, len - offset, packed_bytes);
    if (packed_bytes == 0 || packed_bytes > len - offset) return false;
    offset += packed_bytes;
    if (field_len > len - offset) return false;

    const size_t field_size = static_cast<size_t>(field_len);
    const uint8_t* value = data + offset;

    switch (static_cast<OptionalMetadataFieldType>(field_type)) {
      case OptionalMetadataFieldType::kSignedness:
        if (!ApplySignedness(value, field_size, metadata)) return false;
        break;
      case OptionalMetadataFieldType::kDefaultCharset:
        if (!ApplyDefaultCharset(value, field_size, metadata)) return false;
        break;
      case OptionalMetadataFieldType::kColumnCharset:
        if (!ApplyColumnCharset(value, field_size, metadata)) return false;
        break;
      case OptionalMetadataFieldType::kColumnName:
        if (!ApplyColumnNames(value, field_size, metadata)) return false;
        break;
      default:
        // Other fields (charsets, enum/set values, primary keys, visibility)
        // are not consumed by the decoder yet; skip their bytes.
        break;
    }
    offset += field_size;
  }
  return offset == len;
}

}  // namespace

bool ParseTableMapEvent(const uint8_t* data, size_t len, TableMetadata* metadata) {
  if (data == nullptr || metadata == nullptr) {
    return false;
  }

  // Minimum: 6 (table_id) + 2 (flags) + 1 (db_name_len) + 1 (null) + 1 (tbl_name_len) + 1 (null)
  //        + 1 (column_count packed) = 13
  if (len < 13) {
    return false;
  }

  size_t offset = 0;

  // table_id: 6 bytes LE
  metadata->table_id = binary::ReadU48Le(data + offset);
  offset += 6;

  // flags: 2 bytes (skip)
  offset += 2;

  // database_name_length: 1 byte
  if (offset >= len) return false;
  uint8_t db_name_len = binary::ReadU8(data + offset);
  offset += 1;

  // database_name: db_name_len bytes + null terminator
  const size_t db_field_len = static_cast<size_t>(db_name_len) + 1;
  if (db_field_len > len - offset) return false;
  metadata->database_name = std::string(reinterpret_cast<const char*>(data + offset), db_name_len);
  offset += db_field_len;  // skip null terminator

  // table_name_length: 1 byte
  if (offset >= len) return false;
  uint8_t tbl_name_len = binary::ReadU8(data + offset);
  offset += 1;

  // table_name: tbl_name_len bytes + null terminator
  const size_t table_field_len = static_cast<size_t>(tbl_name_len) + 1;
  if (table_field_len > len - offset) return false;
  metadata->table_name = std::string(reinterpret_cast<const char*>(data + offset), tbl_name_len);
  offset += table_field_len;  // skip null terminator

  // column_count: packed integer
  if (offset >= len) return false;
  size_t packed_bytes = 0;
  uint64_t column_count = binary::ReadPackedInt(data + offset, len - offset, packed_bytes);
  if (packed_bytes == 0 || packed_bytes > len - offset) return false;
  offset += packed_bytes;

  // Note: column_count == 0 is rejected. MySQL does not allow
  // zero-column tables, so a TABLE_MAP with column_count == 0 indicates
  // a corrupt or truncated event. Rejecting here also simplifies the
  // downstream allocation math (null bitmap bytes = 0 would pass the
  // size check trivially and then cause a zero-length decode loop).
  if (column_count == 0 || column_count > kMaxColumns) {
    return false;
  }

  // column_types: 1 byte per column
  const size_t column_count_size = static_cast<size_t>(column_count);
  if (column_count_size > len - offset) return false;
  const uint8_t* col_types_ptr = data + offset;
  offset += column_count_size;

  // metadata_length: packed integer
  if (offset >= len) return false;
  size_t meta_packed_bytes = 0;
  uint64_t metadata_length = binary::ReadPackedInt(data + offset, len - offset, meta_packed_bytes);
  if (meta_packed_bytes == 0 || meta_packed_bytes > len - offset) return false;
  offset += meta_packed_bytes;

  // column metadata block
  if (metadata_length > len - offset) return false;
  const size_t metadata_size = static_cast<size_t>(metadata_length);

  // Single-pass: resize the target columns vector and write type+metadata
  // directly, avoiding intermediate std::vector<uint8_t> and <uint16_t>.
  metadata->columns.resize(column_count_size);
  size_t meta_offset = 0;
  for (size_t i = 0; i < column_count_size; i++) {
    // Note: `>` (not `>=`) is intentional. Fixed-size column types
    // (kTiny, kShort, kLong, etc.) consume 0 metadata bytes, so
    // meta_offset == metadata_length is valid mid-loop. The strict equality
    // check at the end (line ~187) catches actual length mismatches.
    if (meta_offset > metadata_size) return false;
    MetadataValue meta = ReadColumnMetadataValue(col_types_ptr[i], data + offset + meta_offset,
                                                 metadata_size - meta_offset);
    if (!meta.valid) return false;
    metadata->columns[i].type = static_cast<ColumnType>(col_types_ptr[i]);
    metadata->columns[i].metadata = meta.value;
    metadata->columns[i].is_array = meta.is_array;
    metadata->columns[i].array_element_type = meta.array_element_type;
    meta_offset += meta.consumed;
  }
  // Verify metadata was fully consumed. A mismatch indicates a corrupt
  // or unsupported TABLE_MAP event. Strict equality is intentional:
  // MySQL 8.4+ and MariaDB do not pad metadata for fixed-size column
  // types, so exact consumption is the expected behavior.
  if (meta_offset != metadata_size) return false;
  offset += metadata_size;

  // null_bitmap: ceil(column_count / 8) bytes
  size_t null_bitmap_bytes = binary::BitmapBytes(column_count_size);
  if (null_bitmap_bytes > len - offset) return false;
  const uint8_t* null_bitmap = data + offset;

  for (size_t i = 0; i < column_count_size; i++) {
    metadata->columns[i].is_nullable = binary::BitmapIsSet(null_bitmap, i);
  }
  offset += null_bitmap_bytes;

  // Optional metadata (SIGNEDNESS, COLUMN_NAME, ...) follows when the server
  // has binlog_row_metadata enabled. SIGNEDNESS is present in the default
  // MINIMAL mode, so this is what lets the engine decode UNSIGNED columns
  // correctly from raw binlog bytes without a metadata side-connection.
  return ParseOptionalMetadata(data, offset, len, metadata);
}

bool TableMapRegistry::ProcessTableMapEvent(const uint8_t* data, size_t len,
                                            uint64_t* evicted_table_id, bool* unchanged) {
  if (unchanged != nullptr) *unchanged = false;
  if (data == nullptr || len < 6) return false;
  const uint64_t table_id = binary::ReadU48Le(data);
  auto existing = entries_.find(table_id);
  if (existing != entries_.end() && existing->second.raw_body.size() == len &&
      std::memcmp(existing->second.raw_body.data(), data, len) == 0) {
    Touch(existing);
    if (unchanged != nullptr) *unchanged = true;
    return true;
  }

  TableMetadata metadata;
  if (!ParseTableMapEvent(data, len, &metadata)) {
    return false;
  }
  auto it = entries_.find(table_id);
  if (it != entries_.end()) {
    it->second.metadata = std::make_shared<TableMetadata>(std::move(metadata));
    it->second.raw_body.assign(data, data + len);
    Touch(it);
    return true;
  }
  if (entries_.size() == kMaxEntries) {
    const uint64_t evicted = lru_.back();
    lru_.pop_back();
    entries_.erase(evicted);
    if (evicted_table_id != nullptr) *evicted_table_id = evicted;
  }
  lru_.push_front(table_id);
  entries_.emplace(table_id, Entry{std::make_shared<TableMetadata>(std::move(metadata)),
                                   std::vector<uint8_t>(data, data + len), lru_.begin()});
  return true;
}

void TableMapRegistry::Touch(std::unordered_map<uint64_t, Entry>::iterator it) {
  lru_.splice(lru_.begin(), lru_, it->second.lru_position);
  it->second.lru_position = lru_.begin();
}

const TableMetadata* TableMapRegistry::Lookup(uint64_t table_id) {
  auto it = entries_.find(table_id);
  if (it == entries_.end()) {
    return nullptr;
  }
  Touch(it);
  return it->second.metadata.get();
}

std::shared_ptr<const TableMetadata> TableMapRegistry::SharedLookup(uint64_t table_id) {
  auto it = entries_.find(table_id);
  if (it == entries_.end()) return {};
  Touch(it);
  return it->second.metadata;
}

void TableMapRegistry::Clear() {
  entries_.clear();
  lru_.clear();
}

size_t TableMapRegistry::Size() const { return entries_.size(); }

void TableMapRegistry::ForEach(
    const std::function<void(uint64_t, const TableMetadata&)>& visitor) const {
  for (const auto& [table_id, entry] : entries_) {
    visitor(table_id, *entry.metadata);
  }
}

TableMetadata* TableMapRegistry::MutableLookup(uint64_t table_id) {
  auto it = entries_.find(table_id);
  if (it == entries_.end()) return nullptr;
  Touch(it);
  return it->second.metadata.get();
}

}  // namespace mes
