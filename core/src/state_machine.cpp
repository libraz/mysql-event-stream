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
      if (mybinlog::ParseEventHeader(buffer_.data(), buffer_.size(), &current_header_) !=
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

      // Validate event_length
      const bool checksummed = checksum_tracker_.algorithm() == mybinlog::ChecksumAlgorithm::kCrc32;
      size_t min_event_length =
          mybinlog::kEventHeaderSize + (checksummed ? mybinlog::kChecksumSize : 0);
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

const mybinlog::EventHeader& EventStreamParser::CurrentHeader() const { return current_header_; }

void EventStreamParser::CurrentBody(const uint8_t** body_data, size_t* body_len) const {
  if (body_data != nullptr) {
    *body_data = buffer_.data() + mybinlog::kEventHeaderSize;
  }
  if (body_len != nullptr) {
    *body_len = current_payload_length_ > mybinlog::kEventHeaderSize
                    ? current_payload_length_ - mybinlog::kEventHeaderSize
                    : 0;
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
  current_header_ = mybinlog::EventHeader{};
  state_ = ParserState::kWaitingHeader;
  bytes_needed_ = mybinlog::kEventHeaderSize;
  current_payload_length_ = 0;
  if (pending_algorithm_.has_value()) {
    checksum_tracker_.SetDefaultAlgorithm(*pending_algorithm_);
    pending_algorithm_.reset();
  }
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
  const mybinlog::ChecksumAlgorithm algorithm =
      enabled ? mybinlog::ChecksumAlgorithm::kCrc32 : mybinlog::ChecksumAlgorithm::kOff;
  if (state_ == ParserState::kWaitingHeader) {
    checksum_tracker_.SetDefaultAlgorithm(algorithm);
    pending_algorithm_.reset();
  } else {
    pending_algorithm_ = algorithm;
  }
}

bool EventStreamParser::ChecksumEnabled() const {
  return StreamAlgorithm() == mybinlog::ChecksumAlgorithm::kCrc32;
}

void EventStreamParser::SetTrailerPreVerified(bool pre_verified) {
  checksum_tracker_.SetVerify(!pre_verified);
}

bool EventStreamParser::TrailerPreVerified() const { return !checksum_tracker_.verify(); }

mes_error_t EventStreamParser::ErrorCode() const { return error_code_; }

mybinlog::ChecksumAlgorithm EventStreamParser::StreamAlgorithm() const {
  return pending_algorithm_.value_or(checksum_tracker_.algorithm());
}

bool EventStreamParser::VerifyChecksum() {
  mybinlog::FrameLayout layout;
  const mybinlog::Status status =
      checksum_tracker_.OnEvent(current_header_, buffer_.data(), buffer_.size(), &layout);

  // The tracker compares a trailer under CRC32 when verifying, and always
  // checks the dump-start artificial ROTATE under OFF framing.
  const bool fde = current_header_.type_code ==
                   static_cast<uint8_t>(mybinlog::EventType::kFormatDescriptionEvent);
  const bool crc32 = checksum_tracker_.algorithm() == mybinlog::ChecksumAlgorithm::kCrc32;
  const bool artificial_rotate =
      current_header_.type_code == static_cast<uint8_t>(mybinlog::EventType::kRotateEvent) &&
      (current_header_.flags & mybinlog::kLogEventArtificialFlag) != 0 &&
      buffer_.size() >= mybinlog::kEventHeaderSize + mybinlog::kChecksumSize;
  if ((crc32 && checksum_tracker_.verify()) || (!crc32 && artificial_rotate)) ++crc32_passes_;

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

  // An FDE re-derives the stream's framing, and it is later than a setter call
  // made while it was buffered.
  if (fde) pending_algorithm_.reset();
  current_payload_length_ = layout.payload_length;
  return true;
}

}  // namespace mes
