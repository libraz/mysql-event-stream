#include "mes/codec/bytes.h"

namespace mes::codec {

Status ReadPackedInt(const uint8_t* data, size_t len, uint64_t* value, size_t* consumed) {
  if (len < 1) return Status::kTruncated;
  const uint8_t first = data[0];
  if (first < 0xFB) {
    *value = first;
    *consumed = 1;
    return Status::kOk;
  }
  size_t width = 0;
  switch (first) {
    case 0xFC:
      width = 2;
      break;
    case 0xFD:
      width = 3;
      break;
    case 0xFE:
      width = 8;
      break;
    default:
      // 0xFB is the NULL marker and 0xFF is undefined; neither is a length.
      return Status::kInvalidMetadata;
  }
  if (len - 1 < width) return Status::kTruncated;
  switch (width) {
    case 2:
      *value = ReadU16Le(data + 1);
      break;
    case 3:
      *value = ReadU24Le(data + 1);
      break;
    default:
      *value = ReadU64Le(data + 1);
      break;
  }
  *consumed = 1 + width;
  return Status::kOk;
}

}  // namespace mes::codec
