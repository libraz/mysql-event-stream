// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include "value_render.h"

#include <cstdint>
#include <string>
#include <utility>

namespace mes {

namespace {

// Room for any temporal rendering below: an optional sign, six integer fields
// at the widest rendering WritePaddedInt can produce (11 characters), five
// separators and a fractional part.
constexpr size_t kTemporalBufferSize = 96;

// Write the fractional part at the column's declared precision. fsp must not
// exceed 6.
char* WriteFractional(char* out, uint32_t usec, uint8_t fsp) {
  if (fsp == 0) return out;
  static constexpr uint32_t kDivisor[] = {1, 100000, 10000, 1000, 100, 10, 1};
  *out++ = '.';
  return WritePaddedInt(out, static_cast<int>(usec / kDivisor[fsp]), static_cast<int>(fsp));
}

// Write "YYYY-MM-DD" with MySQL's zero padding.
char* WriteCalendarDate(char* out, int year, int month, int day) {
  out = WritePaddedInt(out, year, 4);
  *out++ = '-';
  out = WritePaddedInt(out, month, 2);
  *out++ = '-';
  return WritePaddedInt(out, day, 2);
}

// Write "hh:mm:ss" with MySQL's zero padding.
char* WriteClockTime(char* out, int hour, int minute, int second) {
  out = WritePaddedInt(out, hour, 2);
  *out++ = ':';
  out = WritePaddedInt(out, minute, 2);
  *out++ = ':';
  return WritePaddedInt(out, second, 2);
}

// A value is set in place: a temporary ColumnValue would be built and moved
// for every column of every row.
void SetNonNull(ColumnType type, ColumnValue* out) {
  out->type = type;
  out->is_null = false;
  out->is_binary = false;
  out->int_val = 0;
  out->real_val = 0.0;
  out->string_val.clear();
}

// An unsigned value above INT64_MAX is carried as its decimal string so that
// no caller reads the sign bit as a negative number.
void SetUnsigned(ColumnType type, uint64_t value, ColumnValue* out) {
  SetNonNull(type, out);
  if (value > static_cast<uint64_t>(INT64_MAX)) {
    out->string_val = std::to_string(value);
  } else {
    out->int_val = static_cast<int64_t>(value);
  }
}

void SetText(ColumnType type, const char* begin, const char* end, ColumnValue* out) {
  SetNonNull(type, out);
  out->string_val.assign(begin, static_cast<size_t>(end - begin));
}

}  // namespace

bool RenderValue(mybinlog::Value&& value, ColumnValue* out) {
  using Kind = mybinlog::Value::Kind;
  const ColumnType type = value.type;
  char buf[kTemporalBufferSize];
  char* end = buf;
  switch (value.kind) {
    case Kind::kNull:
      *out = ColumnValue::Null(type);
      return true;
    case Kind::kInt:
      SetNonNull(type, out);
      out->int_val = value.i;
      return true;
    case Kind::kUInt:
    case Kind::kYear:
    case Kind::kEnum:
    case Kind::kSet:
    case Kind::kBit:
      SetUnsigned(type, value.u, out);
      return true;
    case Kind::kDouble:
      SetNonNull(type, out);
      out->real_val = value.d;
      return true;
    case Kind::kDecimal:
    case Kind::kText:
      SetNonNull(type, out);
      out->string_val = std::move(value.bytes);
      return true;
    case Kind::kBytes:
    case Kind::kJson:
      SetNonNull(type, out);
      out->is_binary = true;
      out->string_val = std::move(value.bytes);
      return true;
    case Kind::kDate:
      end = WriteCalendarDate(buf, value.t.year, value.t.month, value.t.day);
      SetText(type, buf, end, out);
      return true;
    case Kind::kDateTime:
      if (value.t.negative) *end++ = '-';
      end = WriteCalendarDate(end, value.t.year, value.t.month, value.t.day);
      *end++ = ' ';
      end = WriteClockTime(end, value.t.hour, value.t.minute, value.t.second);
      end = WriteFractional(end, value.t.microsecond, value.t.fsp);
      SetText(type, buf, end, out);
      return true;
    case Kind::kTime:
      if (value.t.negative) *end++ = '-';
      end = WriteClockTime(end, value.t.hour, value.t.minute, value.t.second);
      end = WriteFractional(end, value.t.microsecond, value.t.fsp);
      SetText(type, buf, end, out);
      return true;
    case Kind::kTimestamp:
      SetNonNull(type, out);
      out->string_val = std::to_string(value.epoch_seconds);
      end = WriteFractional(buf, value.microsecond, value.fsp);
      out->string_val.append(buf, static_cast<size_t>(end - buf));
      return true;
    case Kind::kJsonDiff:
    case Kind::kAbsent:
      return false;
  }
  return false;
}

bool RenderRow(mybinlog::Row&& row, RowData* out, mybinlog::DecodeBudget* budget) {
  out->columns.resize(row.size());
  for (size_t i = 0; i < row.size(); ++i) {
    mybinlog::Value& value = row[i];
    // The decode already charged the payload it materialized; text rendered
    // here from fields is new.
    const size_t charged = value.bytes.size();
    ColumnValue& column = out->columns[i];
    if (!RenderValue(std::move(value), &column)) return false;
    if (budget != nullptr && column.string_val.size() > charged &&
        !budget->Charge(column.string_val.size() - charged)) {
      return false;
    }
  }
  return true;
}

}  // namespace mes
