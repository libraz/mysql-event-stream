// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @file value_render.h
 * @brief Render libmybinlog's structured values into the ColumnValue strings mes.h defines
 *
 * The codec returns temporals as fields, TIMESTAMP as epoch seconds and
 * integers by signedness; mes.h fixes how each reaches the caller. Temporals
 * become MySQL's zero-padded text (TIMESTAMP as epoch seconds), an unsigned
 * value above INT64_MAX becomes its decimal string, character data follows
 * its collation into text or bytes, and JSON stays binary JSON bytes.
 */

#ifndef MES_CORE_SRC_VALUE_RENDER_H_
#define MES_CORE_SRC_VALUE_RENDER_H_

#include <cstdint>
#include <string>

#include "mybinlog/row_decoder.h"
#include "mybinlog/value.h"
#include "types.h"

namespace mes {

/** @brief Number of decimal digits in @p value (1 for zero). */
inline int DecimalDigitCount(unsigned value) {
  if (value < 10U) return 1;
  if (value < 100U) return 2;
  if (value < 1000U) return 3;
  if (value < 10000U) return 4;
  if (value < 100000U) return 5;
  if (value < 1000000U) return 6;
  if (value < 10000000U) return 7;
  if (value < 100000000U) return 8;
  if (value < 1000000000U) return 9;
  return 10;
}

/**
 * @brief Write @p value in decimal at @p out, zero-padded to @p width characters
 *
 * Reproduces printf's "%0*d" for int: the pad width counts a leading '-', and a
 * value needing more digits than @p width is written in full rather than
 * truncated. Nothing is null-terminated.
 *
 * Temporal columns are formatted several times per row, so this deliberately
 * avoids the varargs formatter: printf resolves the decimal point through
 * localeconv_l on every call, which takes a process-wide lock and serializes
 * concurrent decoders.
 *
 * @param out Destination; must have room for max(@p width, 11) characters.
 * @return Pointer one past the last character written.
 */
inline char* WritePaddedInt(char* out, int value, int width) {
  unsigned magnitude;
  if (value < 0) {
    *out++ = '-';
    --width;
    magnitude = 0U - static_cast<unsigned>(value);
  } else {
    magnitude = static_cast<unsigned>(value);
  }

  const int digits = DecimalDigitCount(magnitude);
  const int len = width > digits ? width : digits;
  char* const end = out + len;
  char* cursor = end;
  do {
    *--cursor = static_cast<char>('0' + magnitude % 10);
    magnitude /= 10;
  } while (magnitude != 0);
  while (cursor > out) {
    *--cursor = '0';
  }
  return end;
}

/**
 * @brief Append @p value zero-padded to @p width characters to @p out
 *
 * Formats exactly as @ref WritePaddedInt. @p width must not exceed 20.
 */
inline void AppendPaddedInt(std::string& out, int value, int width) {
  char buf[24];
  out.append(buf, static_cast<size_t>(WritePaddedInt(buf, value, width) - buf));
}

/**
 * @brief Render one decoded value into @p out.
 *
 * The payload of a kDecimal, kText, kBytes or kJson value is moved, not copied.
 * The column name is left for the caller to attach.
 *
 * @param value Decoded value; its #mybinlog::Value::bytes is moved from.
 * @param[out] out Overwritten with the rendered value.
 * @return false for a kind mes.h has no representation for (kJsonDiff, kAbsent),
 *   leaving @p out unspecified.
 */
bool RenderValue(mybinlog::Value&& value, ColumnValue* out);

/**
 * @brief Render one decoded row image into @p out.
 *
 * Text the renderer produces itself (temporals, out-of-range unsigned values)
 * was not charged by the decode, so it is charged to @p budget here: the event
 * then pays for every byte its ColumnValues hold, as the queue's byte bound
 * requires.
 *
 * @param row Decoded row; every value's payload is moved from.
 * @param[out] out Receives one ColumnValue per column, names left empty.
 * @param budget Decode budget of the event the row came from; nullptr charges nothing.
 * @return false when a value cannot be rendered or the budget runs out.
 */
bool RenderRow(mybinlog::Row&& row, RowData* out, mybinlog::DecodeBudget* budget);

}  // namespace mes

#endif  // MES_CORE_SRC_VALUE_RENDER_H_
