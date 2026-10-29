#include "mes/codec/checksum.h"

#include "mes/codec/bytes.h"
#include "mes/codec/crc32.h"

namespace mes::codec {

namespace {

// FDE body: binlog_version(2), server_version(50), create_timestamp(4), header_length(1).
constexpr size_t kFdeServerVersionOffset = kEventHeaderSize + 2;
constexpr size_t kFdeServerVersionLength = 50;
constexpr size_t kFdeFixedEnd = kFdeServerVersionOffset + kFdeServerVersionLength + 4 + 1;
constexpr size_t kChecksumAlgorithmSize = 1;
constexpr size_t kMinChecksummedFrame = kEventHeaderSize + kChecksumSize;

bool TrailerMatches(const uint8_t* frame, size_t len) {
  const size_t data_length = len - kChecksumSize;
  return ComputeCrc32(frame, data_length) == ReadU32Le(frame + data_length);
}

}  // namespace

bool ServerVersionPredatesChecksums(std::string_view server_version) noexcept {
  uint32_t parts[3] = {0, 0, 0};
  size_t pos = 0;
  for (auto& part : parts) {
    while (pos < server_version.size() && server_version[pos] >= '0' &&
           server_version[pos] <= '9') {
      // Saturate: an absurd component must not wrap below the floor.
      if (part < 100000) part = part * 10 + static_cast<uint32_t>(server_version[pos] - '0');
      ++pos;
    }
    if (pos >= server_version.size() || server_version[pos] != '.') break;
    ++pos;
  }
  const bool mariadb = server_version.find("MariaDB") != std::string_view::npos ||
                       server_version.find("-maria-") != std::string_view::npos;
  const uint32_t floor[3] = {5, mariadb ? 3U : 6U, mariadb ? 0U : 1U};
  for (size_t i = 0; i < 3; ++i) {
    if (parts[i] != floor[i]) return parts[i] < floor[i];
  }
  return false;
}

Status ReadFdeChecksumAlgorithm(const uint8_t* frame, size_t len, ChecksumAlgorithm* out) {
  if (frame == nullptr || len < kFdeFixedEnd) return Status::kTruncated;
  std::string_view version(reinterpret_cast<const char*>(frame + kFdeServerVersionOffset),
                           kFdeServerVersionLength);
  version = version.substr(0, version.find('\0'));
  if (ServerVersionPredatesChecksums(version)) {
    *out = ChecksumAlgorithm::kUndefined;
    return Status::kOk;
  }
  if (len - kFdeFixedEnd < kChecksumAlgorithmSize + kChecksumSize) return Status::kTruncated;
  const uint8_t byte = frame[len - kChecksumSize - kChecksumAlgorithmSize];
  switch (byte) {
    case static_cast<uint8_t>(ChecksumAlgorithm::kOff):
      *out = ChecksumAlgorithm::kOff;
      return Status::kOk;
    case static_cast<uint8_t>(ChecksumAlgorithm::kCrc32):
      *out = ChecksumAlgorithm::kCrc32;
      return Status::kOk;
    default:
      return Status::kUnsupported;
  }
}

Status ChecksumTracker::OnEvent(const EventHeader& header, const uint8_t* frame,
                                size_t frame_length, FrameLayout* layout) {
  if (frame == nullptr || frame_length < kEventHeaderSize || frame_length < header.event_length) {
    return Status::kTruncated;
  }
  if (frame_length > header.event_length) return Status::kMalformedValue;

  if (header.type_code == static_cast<uint8_t>(EventType::kFormatDescriptionEvent)) {
    ChecksumAlgorithm declared = ChecksumAlgorithm::kUndefined;
    const Status status = ReadFdeChecksumAlgorithm(frame, frame_length, &declared);
    if (status != Status::kOk) return status;
    algorithm_ = declared;
    if (declared == ChecksumAlgorithm::kUndefined) {
      *layout = FrameLayout{frame_length, false};
      return Status::kOk;
    }
    // The FDE's own trailer is always present; it is only meaningful under CRC32.
    if (declared == ChecksumAlgorithm::kCrc32 && verify_ && !TrailerMatches(frame, frame_length)) {
      return Status::kChecksumMismatch;
    }
    *layout = FrameLayout{frame_length - kChecksumSize, true};
    return Status::kOk;
  }

  if (algorithm_ == ChecksumAlgorithm::kCrc32) {
    if (frame_length < kMinChecksummedFrame) return Status::kChecksumMismatch;
    if (verify_ && !TrailerMatches(frame, frame_length)) return Status::kChecksumMismatch;
    *layout = FrameLayout{frame_length - kChecksumSize, true};
    return Status::kOk;
  }

  // The dump-start ROTATE follows the replica's announced algorithm, not the file's.
  if (header.type_code == static_cast<uint8_t>(EventType::kRotateEvent) &&
      (header.flags & kLogEventArtificialFlag) != 0 && frame_length >= kMinChecksummedFrame &&
      TrailerMatches(frame, frame_length)) {
    *layout = FrameLayout{frame_length - kChecksumSize, true};
    return Status::kOk;
  }
  *layout = FrameLayout{frame_length, false};
  return Status::kOk;
}

}  // namespace mes::codec
