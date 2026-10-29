/**
 * @file status.h
 * @brief Result code returned by every codec function.
 */

#ifndef MES_CODEC_STATUS_H_
#define MES_CODEC_STATUS_H_

#include <cstdint>

namespace mes::codec {

/** @brief Outcome of a codec call; results are returned through out parameters. */
enum class Status : uint8_t {
  kOk,                ///< Success.
  kTruncated,         ///< Input ended before the structure was complete.
  kInvalidMetadata,   ///< Column or table metadata is inconsistent or out of range.
  kMalformedValue,    ///< A value's bytes are not a valid encoding for its type.
  kUnsupported,       ///< A well-formed type or event the codec does not decode.
  kChecksumMismatch,  ///< CRC32 mismatch, or a frame too short to carry its trailer.
  kLimitExceeded,     ///< A configured size or budget limit was exceeded.
};

/** @brief Stable identifier of @p status (e.g. "kTruncated"), for logs and messages. */
const char* StatusName(Status status) noexcept;

}  // namespace mes::codec

#endif  // MES_CODEC_STATUS_H_
