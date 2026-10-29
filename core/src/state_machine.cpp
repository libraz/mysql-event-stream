// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include "state_machine.h"

#include <algorithm>
#include <cstring>

#include "logger.h"
#include "mybinlog/checksum.h"
#include "mybinlog/event_header.h"
#include "mybinlog/event_types.h"

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
      mybinlog::EventHeader wire_header;
      if (mybinlog::ParseEventHeader(buffer_.data(), buffer_.size(), &wire_header) !=
          mybinlog::Status::kOk) {
        state_ = ParserState::kError;
        error_code_ = MES_ERR_PARSE;
        StructuredLog()
            .Event("parse_error")
            .Field("reason", "invalid_header")
            .Field("buffer_size", static_cast<uint64_t>(buffer_.size()))
            .Error();
        break;
      }
      current_header_.timestamp = wire_header.timestamp;
      current_header_.type_code = wire_header.type_code;
      current_header_.server_id = wire_header.server_id;
      current_header_.event_length = wire_header.event_length;
      current_header_.next_position = wire_header.next_position;
      current_header_.flags = wire_header.flags;

      // Validate event_length
      size_t min_event_length =
          mybinlog::kEventHeaderSize + (has_checksum_ ? mybinlog::kChecksumSize : 0);
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

      // Reserve ahead of the body so the subsequent buffer_.insert() calls
      // append in amortized O(1) instead of reallocating per chunk, which is
      // what produced the observed O(N^2) behavior for large BLOB events.
      // The reserve follows the declared event length only up to
      // kMaxEagerReserve; past that, the vector's geometric growth sizes the
      // buffer from the bytes that have actually arrived.
      const size_t eager_reserve = std::min(bytes_needed_, kMaxEagerReserve);
      if (eager_reserve > buffer_.capacity()) {
        buffer_.reserve(eager_reserve);
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

    // A completed event is framed by its file's checksum algorithm: an FDE
    // re-derives it for the rest of the stream, and the dump-start artificial
    // ROTATE may carry a trailer under OFF framing.
    if (state_ == ParserState::kEventReady && !VerifyChecksum()) {
      state_ = ParserState::kError;
    }
  }

  return total_consumed;
}

bool EventStreamParser::HasEvent() const { return state_ == ParserState::kEventReady; }

const EventHeader& EventStreamParser::CurrentHeader() const { return current_header_; }

void EventStreamParser::CurrentBody(const uint8_t** body_data, size_t* body_len) const {
  if (body_data != nullptr) {
    *body_data = buffer_.data() + mybinlog::kEventHeaderSize;
  }
  if (body_len != nullptr) {
    const size_t trailer = current_has_checksum_ ? mybinlog::kChecksumSize : 0;
    const size_t framing = mybinlog::kEventHeaderSize + trailer;
    *body_len = current_header_.event_length > framing ? current_header_.event_length - framing : 0;
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
  bytes_needed_ = mybinlog::kEventHeaderSize;
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

void EventStreamParser::SetTrailerPreVerified(bool pre_verified) {
  trailer_pre_verified_ = pre_verified;
}

bool EventStreamParser::TrailerPreVerified() const { return trailer_pre_verified_; }

mes_error_t EventStreamParser::ErrorCode() const { return error_code_; }

bool EventStreamParser::VerifyChecksum() {
  // The framing state lives in has_checksum_ / current_has_checksum_, so a
  // fresh tracker seeded with it frames this one event.
  mybinlog::ChecksumTracker tracker;
  tracker.SetDefaultAlgorithm(current_has_checksum_ ? mybinlog::ChecksumAlgorithm::kCrc32
                                                    : mybinlog::ChecksumAlgorithm::kOff);
  tracker.SetVerify(!trailer_pre_verified_);

  mybinlog::EventHeader header;
  header.timestamp = current_header_.timestamp;
  header.type_code = current_header_.type_code;
  header.server_id = current_header_.server_id;
  header.event_length = current_header_.event_length;
  header.next_position = current_header_.next_position;
  header.flags = current_header_.flags;

  mybinlog::FrameLayout layout;
  const mybinlog::Status status = tracker.OnEvent(header, buffer_.data(), buffer_.size(), &layout);

  // The tracker compares a trailer under CRC32 when verifying, and always
  // checks the dump-start artificial ROTATE under OFF framing.
  const bool crc32 = tracker.algorithm() == mybinlog::ChecksumAlgorithm::kCrc32;
  const bool artificial_rotate =
      header.type_code == static_cast<uint8_t>(mybinlog::EventType::kRotateEvent) &&
      (header.flags & mybinlog::kLogEventArtificialFlag) != 0 &&
      buffer_.size() >= mybinlog::kEventHeaderSize + mybinlog::kChecksumSize;
  if ((crc32 && tracker.verify()) || (!crc32 && artificial_rotate)) ++crc32_passes_;

  if (status != mybinlog::Status::kOk) {
    const bool mismatch = status == mybinlog::Status::kChecksumMismatch;
    error_code_ = mismatch ? MES_ERR_CHECKSUM : MES_ERR_PARSE;
    StructuredLog()
        .Event("parse_error")
        .Field("reason", mismatch ? "crc32_checksum_mismatch" : "invalid_format_description")
        .Field("status", mybinlog::StatusName(status))
        .Field("event_type", static_cast<int64_t>(current_header_.type_code))
        .Field("event_length", static_cast<uint64_t>(buffer_.size()))
        .Error();
    return false;
  }

  // Only an FDE moves the stream's framing; any other event keeps it and at
  // most gains a trailer of its own (the artificial ROTATE).
  current_has_checksum_ = layout.has_trailer;
  if (header.type_code == static_cast<uint8_t>(mybinlog::EventType::kFormatDescriptionEvent)) {
    has_checksum_ = crc32;
  }
  return true;
}

}  // namespace mes
