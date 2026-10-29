#include "mes/codec/crc32.h"

#include <zlib.h>

#include <limits>

namespace mes::codec {

uint32_t UpdateCrc32(uint32_t crc, const void* data, size_t length) {
  static_assert(sizeof(uInt) >= 4, "zlib uInt must be at least 32 bits");
  constexpr size_t kMaxChunk = static_cast<size_t>(std::numeric_limits<uInt>::max());
  const auto* bytes = static_cast<const Bytef*>(data);
  uLong running = static_cast<uLong>(crc);
  while (length > 0) {
    const size_t chunk = length > kMaxChunk ? kMaxChunk : length;
    running = crc32(running, bytes, static_cast<uInt>(chunk));
    bytes += chunk;
    length -= chunk;
  }
  return static_cast<uint32_t>(running);
}

}  // namespace mes::codec
