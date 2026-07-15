// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include "state_machine.h"

#include <algorithm>
#include <cstring>

#include "binary_util.h"
#include "crc32.h"
#include "logger.h"

namespace mes {

size_t EventStreamParser::Feed(const uint8_t* data, size_t len) {
  if (state_ == ParserState::kEventReady || state_ == ParserState::kError) {
    return 0;
  }

  if (data == nullptr || len == 0) {
    return 0;
  }

  size_t total_consumed = 0;

  while (total_consumed < len) {
    if (state_ == ParserState::kEventReady || state_ == ParserState::kError) {
      break;
    }

    size_t want = bytes_needed_ - buffer_.size();
    size_t available = len - total_consumed;
    size_t to_copy = std::min(want, available);

    buffer_.insert(buffer_.end(), data + total_consumed, data + total_consumed + to_copy);
    total_consumed += to_copy;

    if (buffer_.size() < bytes_needed_) {
      // Still need more data
      break;
    }

    if (state_ == ParserState::kWaitingHeader) {
      // Parse the header
      if (!ParseEventHeader(buffer_.data(), buffer_.size(), &current_header_)) {
        state_ = ParserState::kError;
        error_code_ = MES_ERR_PARSE;
        StructuredLog()
            .Event("parse_error")
            .Field("reason", "invalid_header")
            .Field("buffer_size", static_cast<uint64_t>(buffer_.size()))
            .Error();
        break;
      }

      // Validate event_length
      size_t min_event_length = kEventHeaderSize + (has_checksum_ ? kChecksumSize : 0);
      if (current_header_.event_length < min_event_length) {
        state_ = ParserState::kError;
        error_code_ = MES_ERR_PARSE;
        StructuredLog()
            .Event("parse_error")
            .Field("reason", "invalid_event_length")
            .Field("event_length", static_cast<uint64_t>(current_header_.event_length))
            .Field("event_type", static_cast<int64_t>(current_header_.type_code))
            .Error();
        break;
      }

      if (current_header_.event_length > max_event_size_) {
        state_ = ParserState::kError;
        error_code_ = MES_ERR_PARSE;
        StructuredLog()
            .Event("parse_error")
            .Field("reason", "event_too_large")
            .Field("size", static_cast<uint64_t>(current_header_.event_length))
            .Field("max_size", static_cast<uint64_t>(max_event_size_))
            .Field("event_type", static_cast<int64_t>(current_header_.type_code))
            .Error();
        break;
      }

      bytes_needed_ = current_header_.event_length;

      // Reserve the full event size up front so the subsequent
      // buffer_.insert() calls append in amortized O(1) instead of
      // triggering geometric reallocation. Zero-copy ring-buffer redesign
      // is out of scope; reserving is a targeted micro-optimization that
      // eliminates the observed O(N^2) behavior for large BLOB events.
      if (bytes_needed_ > buffer_.capacity()) {
        buffer_.reserve(bytes_needed_);
      }

      if (buffer_.size() >= bytes_needed_) {
        // Entire event already in buffer (unlikely but possible for tiny events)
        state_ = ParserState::kEventReady;
      } else {
        state_ = ParserState::kWaitingBody;
      }
    } else if (state_ == ParserState::kWaitingBody) {
      state_ = ParserState::kEventReady;
    }

    // When a FORMAT_DESCRIPTION_EVENT completes, auto-detect the checksum
    // algorithm from the stream itself so subsequent events are framed
    // correctly regardless of the server's binlog_checksum setting.
    if (state_ == ParserState::kEventReady &&
        current_header_.type_code ==
            static_cast<uint8_t>(BinlogEventType::kFormatDescriptionEvent)) {
      DetectChecksumFromFde();
    }
    if (state_ == ParserState::kEventReady) {
      DetectArtificialRotateChecksum();
    }
    if (state_ == ParserState::kEventReady && !VerifyChecksum()) {
      state_ = ParserState::kError;
      error_code_ = MES_ERR_CHECKSUM;
      StructuredLog()
          .Event("parse_error")
          .Field("reason", "crc32_checksum_mismatch")
          .Field("event_type", static_cast<int64_t>(current_header_.type_code))
          .Field("event_length", static_cast<uint64_t>(buffer_.size()))
          .Error();
    }
  }

  return total_consumed;
}

bool EventStreamParser::HasEvent() const { return state_ == ParserState::kEventReady; }

const EventHeader& EventStreamParser::CurrentHeader() const { return current_header_; }

void EventStreamParser::CurrentBody(const uint8_t** body_data, size_t* body_len) const {
  if (body_data != nullptr) {
    *body_data = buffer_.data() + kEventHeaderSize;
  }
  if (body_len != nullptr) {
    *body_len = EventBodySize(current_header_, current_has_checksum_);
  }
}

const uint8_t* EventStreamParser::RawData() const { return buffer_.data(); }

size_t EventStreamParser::RawSize() const { return buffer_.size(); }

void EventStreamParser::Advance() {
  if (buffer_.capacity() > kRetainedBufferLimit) {
    std::vector<uint8_t>().swap(buffer_);
  } else {
    buffer_.clear();
  }
  current_header_ = EventHeader{};
  state_ = ParserState::kWaitingHeader;
  bytes_needed_ = kEventHeaderSize;
  current_has_checksum_ = has_checksum_;
  error_code_ = MES_OK;
}

void EventStreamParser::Reset() {
  Advance();
  // Reset is used for reconnect/error recovery, where retaining even a
  // sub-threshold allocation has no locality benefit. Release it eagerly.
  std::vector<uint8_t>().swap(buffer_);
}

ParserState EventStreamParser::GetState() const { return state_; }

void EventStreamParser::SetMaxEventSize(uint32_t max_event_size) {
  // 0 means "no limit", consistent with how max_queue_size == 0 means
  // default/unlimited elsewhere. The absolute hard cap still applies so a
  // malicious peer cannot force unbounded per-event allocation; "no limit"
  // therefore resolves to the largest value the parser will ever accept.
  max_event_size_ = NormalizeMaxEventSize(max_event_size);
}

uint32_t EventStreamParser::MaxEventSize() const { return max_event_size_; }

void EventStreamParser::SetChecksumEnabled(bool enabled) {
  has_checksum_ = enabled;
  if (state_ == ParserState::kWaitingHeader) current_has_checksum_ = enabled;
}

bool EventStreamParser::ChecksumEnabled() const { return has_checksum_; }

mes_error_t EventStreamParser::ErrorCode() const { return error_code_; }

void EventStreamParser::DetectChecksumFromFde() {
  switch (DetectFormatDescriptionChecksum(buffer_.data(), buffer_.size())) {
    case BinlogChecksumAlgorithm::kCrc32:
      has_checksum_ = true;
      current_has_checksum_ = true;
      break;
    case BinlogChecksumAlgorithm::kOff:
      has_checksum_ = false;
      current_has_checksum_ = false;
      break;
    case BinlogChecksumAlgorithm::kUnknown:
      break;
  }
}

bool EventStreamParser::VerifyChecksum() const {
  if (!current_has_checksum_) return true;
  if (buffer_.size() < kEventHeaderSize + kChecksumSize) return false;
  const size_t data_length = buffer_.size() - kChecksumSize;
  const uint32_t computed = ComputeCRC32(buffer_.data(), data_length);
  const uint32_t stored = binary::ReadU32Le(buffer_.data() + data_length);
  return computed == stored;
}

void EventStreamParser::DetectArtificialRotateChecksum() {
  current_has_checksum_ = has_checksum_;
  if (has_checksum_ ||
      current_header_.type_code != static_cast<uint8_t>(BinlogEventType::kRotateEvent) ||
      (current_header_.flags & kLogEventArtificialFlag) == 0 ||
      buffer_.size() < kEventHeaderSize + kChecksumSize) {
    return;
  }
  const size_t data_length = buffer_.size() - kChecksumSize;
  const uint32_t computed = ComputeCRC32(buffer_.data(), data_length);
  const uint32_t stored = binary::ReadU32Le(buffer_.data() + data_length);
  current_has_checksum_ = computed == stored;
}

}  // namespace mes
