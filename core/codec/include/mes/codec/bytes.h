/**
 * @file bytes.h
 * @brief Little-/big-endian readers, packed integers and bitmaps for binlog bytes.
 *
 * The unchecked `ReadU*` loaders assume the caller has already proven the
 * bytes are present. `ByteReader` is the checked cursor: every bound is tested
 * as `remaining < n`, never as `ptr + n > end`, so a length taken from the wire
 * cannot overflow pointer arithmetic. Lengths are `size_t` and the codec may be
 * built for 32-bit targets, so a 64-bit wire length must be compared before it
 * is narrowed.
 */

#ifndef MES_CODEC_BYTES_H_
#define MES_CODEC_BYTES_H_

#include <cstddef>
#include <cstdint>
#include <limits>

#include "mes/codec/status.h"

namespace mes::codec {

/** @brief Read 1 byte. */
inline uint8_t ReadU8(const uint8_t* data) { return data[0]; }

/** @brief Read 2 bytes little-endian. */
inline uint16_t ReadU16Le(const uint8_t* data) {
  return static_cast<uint16_t>(static_cast<uint16_t>(data[0]) |
                               (static_cast<uint16_t>(data[1]) << 8));
}

/** @brief Read 3 bytes little-endian. */
inline uint32_t ReadU24Le(const uint8_t* data) {
  return static_cast<uint32_t>(data[0]) | (static_cast<uint32_t>(data[1]) << 8) |
         (static_cast<uint32_t>(data[2]) << 16);
}

/** @brief Read 4 bytes little-endian. */
inline uint32_t ReadU32Le(const uint8_t* data) {
  return static_cast<uint32_t>(data[0]) | (static_cast<uint32_t>(data[1]) << 8) |
         (static_cast<uint32_t>(data[2]) << 16) | (static_cast<uint32_t>(data[3]) << 24);
}

/** @brief Read 6 bytes little-endian (table ids). */
inline uint64_t ReadU48Le(const uint8_t* data) {
  return static_cast<uint64_t>(ReadU32Le(data)) |
         (static_cast<uint64_t>(ReadU16Le(data + 4)) << 32);
}

/** @brief Read 8 bytes little-endian. */
inline uint64_t ReadU64Le(const uint8_t* data) {
  return static_cast<uint64_t>(ReadU32Le(data)) |
         (static_cast<uint64_t>(ReadU32Le(data + 4)) << 32);
}

/** @brief Read 2 bytes big-endian (temporal2 and DECIMAL values). */
inline uint16_t ReadU16Be(const uint8_t* data) {
  return static_cast<uint16_t>((static_cast<uint16_t>(data[0]) << 8) |
                               static_cast<uint16_t>(data[1]));
}

/** @brief Read 3 bytes big-endian. */
inline uint32_t ReadU24Be(const uint8_t* data) {
  return (static_cast<uint32_t>(data[0]) << 16) | (static_cast<uint32_t>(data[1]) << 8) |
         static_cast<uint32_t>(data[2]);
}

/** @brief Read 4 bytes big-endian. */
inline uint32_t ReadU32Be(const uint8_t* data) {
  return (static_cast<uint32_t>(data[0]) << 24) | (static_cast<uint32_t>(data[1]) << 16) |
         (static_cast<uint32_t>(data[2]) << 8) | static_cast<uint32_t>(data[3]);
}

/**
 * @brief Decode a MySQL packed (length-encoded) integer.
 *
 * `< 0xFB` is the value itself; `0xFC`, `0xFD`, `0xFE` prefix 2, 3 and 8 LE
 * bytes. No binlog field that carries a packed integer is nullable, so the NULL
 * marker 0xFB and the undefined prefix 0xFF are rejected rather than read as 0
 * or as the 8-byte form.
 *
 * @param data Bytes to decode.
 * @param len Bytes readable at @p data.
 * @param[out] value Decoded integer; untouched on failure.
 * @param[out] consumed Bytes consumed (1, 3, 4 or 9); untouched on failure.
 * @return kOk; kTruncated when @p len is short; kInvalidMetadata for 0xFB / 0xFF.
 */
Status ReadPackedInt(const uint8_t* data, size_t len, uint64_t* value, size_t* consumed);

/**
 * @brief Bytes needed for a bitmap of @p bit_count bits.
 *
 * Saturates instead of wrapping: a wire count within 7 of SIZE_MAX would
 * otherwise round to a tiny byte count and defeat the caller's bounds check.
 */
inline size_t BitmapBytes(size_t bit_count) {
  constexpr size_t kMax = std::numeric_limits<size_t>::max();
  if (bit_count > kMax - 7) return kMax / 8 + 1;
  return (bit_count + 7) / 8;
}

/** @brief Whether bit @p bit_index of an LSB-first bitmap is set. */
inline bool BitmapIsSet(const uint8_t* bitmap, size_t bit_index) {
  return (bitmap[bit_index / 8] & (1U << (bit_index % 8))) != 0;
}

/** @brief Bounds-checked forward cursor over a byte range. */
class ByteReader {
 public:
  ByteReader(const uint8_t* data, size_t len) : data_(data), remaining_(len) {}

  /** @brief Bytes not yet consumed. */
  size_t remaining() const { return remaining_; }
  /** @brief Current read position. */
  const uint8_t* position() const { return data_; }

  /** @brief Advance @p n bytes; kTruncated if fewer remain. */
  Status Skip(size_t n) {
    if (remaining_ < n) return Status::kTruncated;
    data_ += n;
    remaining_ -= n;
    return Status::kOk;
  }

  /** @brief Point @p out at the next @p n bytes and advance past them. */
  Status ReadBytes(size_t n, const uint8_t** out) {
    if (remaining_ < n) return Status::kTruncated;
    *out = data_;
    data_ += n;
    remaining_ -= n;
    return Status::kOk;
  }

  Status ReadU8(uint8_t* out) { return Fixed(1, out, &codec::ReadU8); }
  Status ReadU16Le(uint16_t* out) { return Fixed(2, out, &codec::ReadU16Le); }
  Status ReadU24Le(uint32_t* out) { return Fixed(3, out, &codec::ReadU24Le); }
  Status ReadU32Le(uint32_t* out) { return Fixed(4, out, &codec::ReadU32Le); }
  Status ReadU48Le(uint64_t* out) { return Fixed(6, out, &codec::ReadU48Le); }
  Status ReadU64Le(uint64_t* out) { return Fixed(8, out, &codec::ReadU64Le); }

  /** @brief Read a packed integer; see ReadPackedInt(). */
  Status ReadPacked(uint64_t* out) {
    size_t consumed = 0;
    const Status status = ReadPackedInt(data_, remaining_, out, &consumed);
    if (status != Status::kOk) return status;
    data_ += consumed;
    remaining_ -= consumed;
    return Status::kOk;
  }

 private:
  template <typename T>
  Status Fixed(size_t n, T* out, T (*load)(const uint8_t*)) {
    if (remaining_ < n) return Status::kTruncated;
    *out = load(data_);
    data_ += n;
    remaining_ -= n;
    return Status::kOk;
  }

  const uint8_t* data_;
  size_t remaining_;
};

}  // namespace mes::codec

#endif  // MES_CODEC_BYTES_H_
