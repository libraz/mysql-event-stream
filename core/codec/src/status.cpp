#include "mes/codec/status.h"

namespace mes::codec {

const char* StatusName(Status status) noexcept {
  switch (status) {
    case Status::kOk:
      return "kOk";
    case Status::kTruncated:
      return "kTruncated";
    case Status::kInvalidMetadata:
      return "kInvalidMetadata";
    case Status::kMalformedValue:
      return "kMalformedValue";
    case Status::kUnsupported:
      return "kUnsupported";
    case Status::kChecksumMismatch:
      return "kChecksumMismatch";
    case Status::kLimitExceeded:
      return "kLimitExceeded";
  }
  return "kUnknown";
}

}  // namespace mes::codec
