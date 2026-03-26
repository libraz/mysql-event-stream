// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include "protocol/mysql_packet.h"

#include <cstring>

#include "protocol/mysql_socket.h"

namespace mes::protocol {

namespace {
constexpr size_t kMaxPacketPayload = 0xFFFFFF;  // 16 MB - 1
constexpr size_t kPacketHeaderSize = 4;          // 3 length + 1 sequence
}  // namespace

// --- PacketBuffer ---

void PacketBuffer::Clear() { buf_.clear(); }

void PacketBuffer::WritePacket(const uint8_t* payload, size_t len,
                               uint8_t* sequence_id) {
  size_t offset = 0;

  // Split into chunks of kMaxPacketPayload
  while (offset < len) {
    size_t chunk = len - offset;
    if (chunk > kMaxPacketPayload) {
      chunk = kMaxPacketPayload;
    }

    // 3-byte LE payload length
    buf_.push_back(static_cast<uint8_t>(chunk & 0xFF));
    buf_.push_back(static_cast<uint8_t>((chunk >> 8) & 0xFF));
    buf_.push_back(static_cast<uint8_t>((chunk >> 16) & 0xFF));
    // 1-byte sequence ID
    buf_.push_back(*sequence_id);
    ++(*sequence_id);

    // Payload bytes
    buf_.insert(buf_.end(), payload + offset, payload + offset + chunk);
    offset += chunk;
  }

  // If the payload was an exact multiple of kMaxPacketPayload (including 0),
  // send a trailing zero-length packet to signal end of multi-packet sequence.
  if (len == 0 || (len % kMaxPacketPayload) == 0) {
    // For len == 0, this sends a single zero-length packet (the normal case).
    // For exact multiples, this terminates the multi-packet sequence.
    if (len != 0) {
      // Only append the trailing empty packet for exact multiples > 0.
      // len == 0 already skipped the while loop, so we just write below.
    }
    buf_.push_back(0x00);
    buf_.push_back(0x00);
    buf_.push_back(0x00);
    buf_.push_back(*sequence_id);
    ++(*sequence_id);
  }
}

const uint8_t* PacketBuffer::Data() const { return buf_.data(); }

size_t PacketBuffer::Size() const { return buf_.size(); }

// --- ReadPacket ---

mes_error_t ReadPacket(SocketHandle* sock, std::vector<uint8_t>* payload,
                       uint8_t* sequence_id) {
  payload->clear();

  for (;;) {
    uint8_t header[kPacketHeaderSize];
    mes_error_t err = sock->ReadExact(header, kPacketHeaderSize);
    if (err != MES_OK) {
      return MES_ERR_STREAM;
    }

    uint32_t payload_length = static_cast<uint32_t>(header[0]) |
                              (static_cast<uint32_t>(header[1]) << 8) |
                              (static_cast<uint32_t>(header[2]) << 16);
    *sequence_id = header[3];

    if (payload_length > 0) {
      size_t prev_size = payload->size();
      payload->resize(prev_size + payload_length);
      err = sock->ReadExact(payload->data() + prev_size, payload_length);
      if (err != MES_OK) {
        return MES_ERR_STREAM;
      }
    }

    // Multi-packet: continue reading if payload was exactly 0xFFFFFF
    if (payload_length < kMaxPacketPayload) {
      break;
    }
  }

  return MES_OK;
}

// --- Length-encoded integer ---

uint64_t ReadLenEncInt(const uint8_t* data, size_t len, size_t* pos) {
  if (*pos >= len) {
    return 0;
  }

  uint8_t first = data[*pos];
  ++(*pos);

  if (first < 0xFB) {
    return first;
  }

  if (first == 0xFB) {
    // NULL marker
    return 0;
  }

  if (first == 0xFC) {
    if (*pos + 2 > len) {
      return 0;
    }
    uint64_t val = ReadFixedInt(data + *pos, 2);
    *pos += 2;
    return val;
  }

  if (first == 0xFD) {
    if (*pos + 3 > len) {
      return 0;
    }
    uint64_t val = ReadFixedInt(data + *pos, 3);
    *pos += 3;
    return val;
  }

  if (first == 0xFE) {
    if (*pos + 8 > len) {
      return 0;
    }
    uint64_t val = ReadFixedInt(data + *pos, 8);
    *pos += 8;
    return val;
  }

  // 0xFF: error/undefined
  return 0;
}

void WriteLenEncInt(std::vector<uint8_t>* buf, uint64_t val) {
  if (val < 251) {
    buf->push_back(static_cast<uint8_t>(val));
  } else if (val < 0x10000) {
    buf->push_back(0xFC);
    WriteFixedInt(buf, val, 2);
  } else if (val < 0x1000000) {
    buf->push_back(0xFD);
    WriteFixedInt(buf, val, 3);
  } else {
    buf->push_back(0xFE);
    WriteFixedInt(buf, val, 8);
  }
}

void WriteLenEncString(std::vector<uint8_t>* buf, const std::string& s) {
  WriteLenEncInt(buf, s.size());
  buf->insert(buf->end(), s.begin(), s.end());
}

// --- Fixed-width integer helpers ---

void WriteFixedInt(std::vector<uint8_t>* buf, uint64_t val, size_t width) {
  for (size_t i = 0; i < width; ++i) {
    buf->push_back(static_cast<uint8_t>(val & 0xFF));
    val >>= 8;
  }
}

uint64_t ReadFixedInt(const uint8_t* data, size_t width) {
  uint64_t val = 0;
  for (size_t i = 0; i < width; ++i) {
    val |= static_cast<uint64_t>(data[i]) << (i * 8);
  }
  return val;
}

}  // namespace mes::protocol
