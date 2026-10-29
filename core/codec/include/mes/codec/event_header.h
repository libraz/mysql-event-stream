/**
 * @file event_header.h
 * @brief The 19-byte binlog v4 common event header.
 */

#ifndef MES_CODEC_EVENT_HEADER_H_
#define MES_CODEC_EVENT_HEADER_H_

#include <cstddef>
#include <cstdint>

#include "mes/codec/event_types.h"
#include "mes/codec/status.h"

namespace mes::codec {

/** @brief Size of the common header that opens every event. */
inline constexpr size_t kEventHeaderSize = 19;

/** @brief Size of the CRC32 trailer a checksummed event ends with. */
inline constexpr size_t kChecksumSize = 4;

/** @brief Magic number opening a binlog file (`\xfe` `bin`). */
inline constexpr uint8_t kBinlogMagic[4] = {0xFE, 0x62, 0x69, 0x6E};

/** @brief Offset of the first event in a binlog file: the lowest position that names an event. */
inline constexpr uint64_t kBinlogMagicOffset = sizeof(kBinlogMagic);

/** @brief Binlog v4 common header, all fields little-endian on the wire. */
struct EventHeader {
  uint32_t timestamp = 0;
  uint8_t type_code = 0;  ///< An EventType value, or an unknown code.
  uint32_t server_id = 0;
  uint32_t event_length = 0;  ///< Whole event: header, body and any trailer.
  uint32_t next_position = 0;
  uint16_t flags = 0;  ///< `kLogEvent*Flag` bits.
};

/**
 * @brief Parse the common header at the start of @p data.
 * @param[out] header Filled on success.
 * @return kOk, or kTruncated when fewer than kEventHeaderSize bytes are given.
 */
Status ParseEventHeader(const uint8_t* data, size_t len, EventHeader* header);

}  // namespace mes::codec

#endif  // MES_CODEC_EVENT_HEADER_H_
