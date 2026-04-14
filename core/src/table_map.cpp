// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include "table_map.h"

#include "binary_util.h"

namespace mes {

namespace {

// Maximum column count for safety.
constexpr size_t kMaxColumns = 4096;

// Maximum number of TABLE_MAP entries retained in the registry. When this
// threshold is exceeded (e.g. after heavy DDL or long-running replication
// that cycles through many table_ids), the entire registry is cleared to
// prevent unbounded memory growth. The server re-emits TABLE_MAP events
// before each ROWS event, so clearing is safe: the next ROWS event will
// simply be preceded by a fresh TABLE_MAP that re-populates the cache.
// A simple single-threshold flush is preferred over strict LRU because it
// keeps the hot path allocation-free and the worst case affects at most
// one stale table_id per cleared entry.
constexpr size_t kMaxTableMapEntries = 8192;

// Read per-column metadata from the metadata block.
// Returns the number of bytes consumed, or 0 on error.
size_t ReadColumnMetadataValue(uint8_t col_type, const uint8_t* data, size_t remaining,
                               uint16_t* out) {
  switch (col_type) {
    case static_cast<uint8_t>(ColumnType::kVarchar):
    case static_cast<uint8_t>(ColumnType::kVarString):
      if (remaining < 2) return 0;
      *out = binary::ReadU16Le(data);
      return 2;

    case static_cast<uint8_t>(ColumnType::kBlob):
    case static_cast<uint8_t>(ColumnType::kTinyBlob):
    case static_cast<uint8_t>(ColumnType::kMediumBlob):
    case static_cast<uint8_t>(ColumnType::kLongBlob):
    case static_cast<uint8_t>(ColumnType::kGeometry):
    case static_cast<uint8_t>(ColumnType::kJson):
    case static_cast<uint8_t>(ColumnType::kVector):
      if (remaining < 1) return 0;
      *out = binary::ReadU8(data);
      return 1;

    case static_cast<uint8_t>(ColumnType::kFloat):
    case static_cast<uint8_t>(ColumnType::kDouble):
      if (remaining < 1) return 0;
      *out = binary::ReadU8(data);
      return 1;

    case static_cast<uint8_t>(ColumnType::kNewDecimal):
      if (remaining < 2) return 0;
      *out = static_cast<uint16_t>((binary::ReadU8(data) << 8) | binary::ReadU8(data + 1));
      return 2;

    case static_cast<uint8_t>(ColumnType::kBit):
      if (remaining < 2) return 0;
      *out = static_cast<uint16_t>((binary::ReadU8(data + 1) << 8) | binary::ReadU8(data));
      return 2;

    case static_cast<uint8_t>(ColumnType::kEnum):
    case static_cast<uint8_t>(ColumnType::kSet):
      if (remaining < 2) return 0;
      *out = binary::ReadU16Le(data);
      return 2;

    case static_cast<uint8_t>(ColumnType::kString):
      if (remaining < 2) return 0;
      *out = static_cast<uint16_t>((binary::ReadU8(data) << 8) | binary::ReadU8(data + 1));
      return 2;

    case static_cast<uint8_t>(ColumnType::kTimestamp2):
    case static_cast<uint8_t>(ColumnType::kDatetime2):
    case static_cast<uint8_t>(ColumnType::kTime2):
      if (remaining < 1) return 0;
      *out = binary::ReadU8(data);
      return 1;

    // Fixed-size types: no metadata bytes
    case static_cast<uint8_t>(ColumnType::kTiny):
    case static_cast<uint8_t>(ColumnType::kShort):
    case static_cast<uint8_t>(ColumnType::kLong):
    case static_cast<uint8_t>(ColumnType::kLongLong):
    case static_cast<uint8_t>(ColumnType::kInt24):
    case static_cast<uint8_t>(ColumnType::kYear):
    case static_cast<uint8_t>(ColumnType::kDate):
    case static_cast<uint8_t>(ColumnType::kTime):
    case static_cast<uint8_t>(ColumnType::kDatetime):
    case static_cast<uint8_t>(ColumnType::kTimestamp):
      *out = 0;
      return 0;

    default:
      *out = 0;
      return 0;
  }
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
  if (offset + db_name_len + 1 > len) return false;
  metadata->database_name = std::string(reinterpret_cast<const char*>(data + offset), db_name_len);
  offset += db_name_len + 1;  // skip null terminator

  // table_name_length: 1 byte
  if (offset >= len) return false;
  uint8_t tbl_name_len = binary::ReadU8(data + offset);
  offset += 1;

  // table_name: tbl_name_len bytes + null terminator
  if (offset + tbl_name_len + 1 > len) return false;
  metadata->table_name = std::string(reinterpret_cast<const char*>(data + offset), tbl_name_len);
  offset += tbl_name_len + 1;  // skip null terminator

  // column_count: packed integer
  if (offset >= len) return false;
  size_t packed_bytes = 0;
  uint64_t column_count = binary::ReadPackedInt(data + offset, len - offset, packed_bytes);
  if (packed_bytes == 0) return false;
  offset += packed_bytes;

  if (column_count == 0 || column_count > kMaxColumns) {
    return false;
  }

  // column_types: 1 byte per column
  if (offset + column_count > len) return false;
  const uint8_t* col_types_ptr = data + offset;
  offset += column_count;

  // metadata_length: packed integer
  if (offset >= len) return false;
  size_t meta_packed_bytes = 0;
  uint64_t metadata_length = binary::ReadPackedInt(data + offset, len - offset, meta_packed_bytes);
  if (meta_packed_bytes == 0) return false;
  offset += meta_packed_bytes;

  // column metadata block
  if (offset + metadata_length > len) return false;

  // Single-pass: resize the target columns vector and write type+metadata
  // directly, avoiding intermediate std::vector<uint8_t> and <uint16_t>.
  metadata->columns.resize(column_count);
  size_t meta_offset = 0;
  for (size_t i = 0; i < column_count; i++) {
    if (meta_offset > metadata_length) return false;
    uint16_t meta_val = 0;
    size_t consumed = ReadColumnMetadataValue(col_types_ptr[i], data + offset + meta_offset,
                                              metadata_length - meta_offset, &meta_val);
    metadata->columns[i].type = static_cast<ColumnType>(col_types_ptr[i]);
    metadata->columns[i].metadata = meta_val;
    meta_offset += consumed;
  }
  // Verify metadata was fully consumed. A mismatch indicates a corrupt
  // or unsupported TABLE_MAP event. Strict equality is intentional:
  // MySQL 8.4+ and MariaDB do not pad metadata for fixed-size column
  // types, so exact consumption is the expected behavior.
  if (meta_offset != metadata_length) return false;
  offset += metadata_length;

  // null_bitmap: ceil(column_count / 8) bytes
  size_t null_bitmap_bytes = binary::BitmapBytes(column_count);
  if (offset + null_bitmap_bytes > len) return false;
  const uint8_t* null_bitmap = data + offset;

  for (size_t i = 0; i < column_count; i++) {
    metadata->columns[i].is_nullable = binary::BitmapIsSet(null_bitmap, i);
  }

  return true;
}

bool TableMapRegistry::ProcessTableMapEvent(const uint8_t* data, size_t len) {
  TableMetadata metadata;
  if (!ParseTableMapEvent(data, len, &metadata)) {
    return false;
  }
  uint64_t table_id = metadata.table_id;
  // Bound registry growth. Clearing on overflow is acceptable because each
  // ROWS event is always preceded by a TABLE_MAP event that re-registers the
  // table; after a flush, the first subsequent ROWS event for any table will
  // see a fresh TABLE_MAP before decoding begins.
  if (entries_.size() >= kMaxTableMapEntries && entries_.find(table_id) == entries_.end()) {
    entries_.clear();
  }
  entries_[table_id] = std::move(metadata);
  return true;
}

const TableMetadata* TableMapRegistry::Lookup(uint64_t table_id) const {
  auto it = entries_.find(table_id);
  if (it == entries_.end()) {
    return nullptr;
  }
  return &it->second;
}

void TableMapRegistry::Clear() { entries_.clear(); }

size_t TableMapRegistry::Size() const { return entries_.size(); }

TableMetadata* TableMapRegistry::MutableLookup(uint64_t table_id) {
  auto it = entries_.find(table_id);
  if (it == entries_.end()) return nullptr;
  return &it->second;
}

}  // namespace mes
