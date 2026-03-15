// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include "state_machine.h"

#include <algorithm>
#include <cstring>

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
        break;
      }

      // Validate event_length
      if (current_header_.event_length < kEventHeaderSize + kChecksumSize) {
        state_ = ParserState::kError;
        break;
      }

      bytes_needed_ = current_header_.event_length;

      if (buffer_.size() >= bytes_needed_) {
        // Entire event already in buffer (unlikely but possible for tiny events)
        state_ = ParserState::kEventReady;
      } else {
        state_ = ParserState::kWaitingBody;
      }
    } else if (state_ == ParserState::kWaitingBody) {
      state_ = ParserState::kEventReady;
    }
  }

  return total_consumed;
}

bool EventStreamParser::HasEvent() const {
  return state_ == ParserState::kEventReady;
}

const EventHeader& EventStreamParser::CurrentHeader() const {
  return current_header_;
}

void EventStreamParser::CurrentBody(const uint8_t** body_data, size_t* body_len) const {
  if (body_data != nullptr) {
    *body_data = buffer_.data() + kEventHeaderSize;
  }
  if (body_len != nullptr) {
    *body_len = EventBodySize(current_header_);
  }
}

const uint8_t* EventStreamParser::RawData() const { return buffer_.data(); }

size_t EventStreamParser::RawSize() const { return buffer_.size(); }

void EventStreamParser::Advance() {
  buffer_.clear();
  current_header_ = EventHeader{};
  state_ = ParserState::kWaitingHeader;
  bytes_needed_ = kEventHeaderSize;
}

void EventStreamParser::Reset() { Advance(); }

ParserState EventStreamParser::GetState() const { return state_; }

}  // namespace mes
