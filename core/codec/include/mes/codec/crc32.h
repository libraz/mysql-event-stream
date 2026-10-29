/**
 * @file crc32.h
 * @brief CRC32 (zlib polynomial) as used by binlog event checksums.
 */

#ifndef MES_CODEC_CRC32_H_
#define MES_CODEC_CRC32_H_

#include <cstddef>
#include <cstdint>

namespace mes::codec {

/**
 * @brief Extend a running CRC32 with @p length bytes.
 *
 * Inputs longer than zlib's `uInt` are fed in chunks, so any `size_t` length
 * yields the true CRC.
 *
 * @param crc Previous value; 0 starts a new checksum.
 */
uint32_t UpdateCrc32(uint32_t crc, const void* data, size_t length);

/** @brief CRC32 of @p length bytes at @p data (`data` may be null when `length` is 0). */
inline uint32_t ComputeCrc32(const void* data, size_t length) {
  return UpdateCrc32(0, data, length);
}

}  // namespace mes::codec

#endif  // MES_CODEC_CRC32_H_
