/**
 * @file rotate.h
 * @brief ROTATE_EVENT body.
 */

#ifndef MES_CODEC_ROTATE_H_
#define MES_CODEC_ROTATE_H_

#include <cstddef>
#include <cstdint>
#include <string>

#include "mes/codec/status.h"

namespace mes::codec {

/** @brief Where the next binlog file starts. */
struct RotateEvent {
  uint64_t position = 0;  ///< First event position in the next file.
  std::string next_file;  ///< Next file name, without any trailing NUL.
};

/**
 * @brief Parse a ROTATE_EVENT body: 8-byte LE position, then the file name.
 * @param body Bytes after the common header.
 * @param len Body length, trailer excluded.
 * @return kOk, or kTruncated when the position does not fit.
 */
Status ParseRotateEvent(const uint8_t* body, size_t len, RotateEvent* out);

}  // namespace mes::codec

#endif  // MES_CODEC_ROTATE_H_
