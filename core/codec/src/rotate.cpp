#include "mes/codec/rotate.h"

#include "mes/codec/bytes.h"

namespace mes::codec {

Status ParseRotateEvent(const uint8_t* body, size_t len, RotateEvent* out) {
  constexpr size_t kPositionSize = 8;
  if (body == nullptr || len < kPositionSize) return Status::kTruncated;
  out->position = ReadU64Le(body);
  size_t name_len = len - kPositionSize;
  // Some servers terminate the name with a NUL.
  if (name_len > 0 && body[kPositionSize + name_len - 1] == '\0') --name_len;
  out->next_file.assign(reinterpret_cast<const char*>(body + kPositionSize), name_len);
  return Status::kOk;
}

}  // namespace mes::codec
