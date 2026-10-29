#include "mes/codec/event_header.h"

#include "mes/codec/bytes.h"

namespace mes::codec {

Status ParseEventHeader(const uint8_t* data, size_t len, EventHeader* header) {
  if (data == nullptr || len < kEventHeaderSize) return Status::kTruncated;
  header->timestamp = ReadU32Le(data);
  header->type_code = ReadU8(data + 4);
  header->server_id = ReadU32Le(data + 5);
  header->event_length = ReadU32Le(data + 9);
  header->next_position = ReadU32Le(data + 13);
  header->flags = ReadU16Le(data + 17);
  return Status::kOk;
}

}  // namespace mes::codec
