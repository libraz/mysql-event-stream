// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include "rotate_event.h"

#include "binary_util.h"

namespace mes {

bool ParseRotateEvent(const uint8_t* data, size_t len, RotateEventData* result) {
  if (data == nullptr || result == nullptr) {
    return false;
  }

  // ROTATE_EVENT body: 8-byte position + filename
  if (len < 8) {
    return false;
  }

  result->position = binary::ReadU64Le(data);

  if (len > 8) {
    size_t fname_len = len - 8;
    // Some MySQL servers include a trailing null byte in the filename
    if (fname_len > 0 && data[8 + fname_len - 1] == '\0') {
      --fname_len;
    }
    result->new_log_file = std::string(reinterpret_cast<const char*>(data + 8), fname_len);
  } else {
    result->new_log_file.clear();
  }

  return true;
}

}  // namespace mes
