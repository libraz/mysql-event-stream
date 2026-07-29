#include "client/gtid_set.h"

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <climits>
#include <cstdio>
#include <cstdlib>

#include "binary_util.h"
#include "logger.h"

namespace mes {
namespace {

bool IsTagFirst(char ch) {
  return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_';
}

std::string Trim(const std::string& text) {
  const size_t begin = text.find_first_not_of(" \t\n\r");
  if (begin == std::string::npos) return {};
  const size_t end = text.find_last_not_of(" \t\n\r");
  return text.substr(begin, end - begin + 1);
}

bool ReadTaggedLength(const uint8_t* data, size_t size, size_t* offset, size_t* length) {
  if (data == nullptr || offset == nullptr || length == nullptr || *offset >= size) return false;
  const uint8_t first = data[*offset];
  size_t bytes = 1;
  while (bytes <= 8 && (first & (uint8_t{1} << (bytes - 1))) != 0) ++bytes;
  if (bytes > size - *offset) return false;
  uint64_t decoded = first >> bytes;
  if (bytes > 1) {
    uint64_t trailing = 0;
    for (size_t i = 1; i < bytes; ++i) trailing |= uint64_t{data[*offset + i]} << (8 * (i - 1));
    decoded |= trailing << (bytes == 9 ? 0 : 8 - bytes);
  }
  if (decoded > 32) return false;
  *offset += bytes;
  *length = static_cast<size_t>(decoded);
  return true;
}

}  // namespace

mes_error_t GtidSet::Parse(const std::string& text, GtidSet* out) {
  if (out == nullptr) return MES_ERR_NULL_ARG;
  GtidSet parsed;
  size_t pos = 0;
  while (pos < text.size()) {
    const size_t comma = text.find(',', pos);
    std::string part =
        Trim(text.substr(pos, comma == std::string::npos ? std::string::npos : comma - pos));
    pos = comma == std::string::npos ? text.size() : comma + 1;
    if (part.empty()) continue;

    const size_t colon = part.find(':');
    if (colon == std::string::npos) return MES_ERR_INVALID_ARG;
    std::string intervals = part.substr(colon + 1);
    std::string tag;
    const size_t tag_separator = intervals.find(':');
    if (tag_separator != std::string::npos &&
        NormalizeTag(intervals.substr(0, tag_separator), &tag)) {
      intervals = intervals.substr(tag_separator + 1);
    }

    Sid sid{};
    mes_error_t rc = ParseUuid(part.substr(0, colon), &sid);
    if (rc != MES_OK) return rc;

    auto& target = parsed.sets_[{sid, tag}];
    size_t interval_pos = 0;
    while (interval_pos < intervals.size()) {
      const size_t next = intervals.find(':', interval_pos);
      const std::string interval = intervals.substr(
          interval_pos, next == std::string::npos ? std::string::npos : next - interval_pos);
      interval_pos = next == std::string::npos ? intervals.size() : next + 1;
      if (interval.empty()) continue;
      Interval value;
      rc = ParseInterval(interval, &value);
      if (rc != MES_OK || target.size() == kMaxIntervalsPerSid) {
        return rc == MES_OK ? MES_ERR_INVALID_ARG : rc;
      }
      target.push_back(value);
    }
    if (target.empty()) return MES_ERR_INVALID_ARG;
  }

  for (auto& [tsid, intervals] : parsed.sets_) {
    (void)tsid;
    if (!NormalizeIntervals(&intervals)) return MES_ERR_INVALID_ARG;
  }
  *out = std::move(parsed);
  return MES_OK;
}

bool GtidSet::DecodeBinary(const uint8_t* data, size_t size, GtidSet* out) {
  if (data == nullptr || out == nullptr || size < sizeof(uint64_t)) return false;
  size_t offset = 0;
  const uint64_t count_and_format = binary::ReadU64Le(data);
  const uint8_t format = static_cast<uint8_t>(count_and_format >> 56);
  if (format > 1) return false;
  const bool tagged = format == 1;
  if (tagged && static_cast<uint8_t>(count_and_format) != format) return false;
  const uint64_t sid_count = tagged ? ((count_and_format & 0x00ffffffffffff00ULL) >> 8)
                                    : (count_and_format & 0x00ffffffffffffffULL);
  offset += sizeof(uint64_t);
  constexpr size_t kMinSidSize = 16 + sizeof(uint64_t) + 2 * sizeof(uint64_t);
  if (sid_count > (size - offset) / kMinSidSize) return false;

  GtidSet decoded;
  for (uint64_t i = 0; i < sid_count; ++i) {
    if (size - offset < 16 + sizeof(uint64_t)) return false;
    Sid sid{};
    std::copy_n(data + offset, sid.size(), sid.begin());
    offset += sid.size();
    std::string tag;
    if (tagged) {
      size_t tag_length = 0;
      if (!ReadTaggedLength(data, size, &offset, &tag_length) || tag_length > size - offset) {
        return false;
      }
      tag.assign(reinterpret_cast<const char*>(data + offset), tag_length);
      offset += tag_length;
      if (!tag.empty() && !NormalizeTag(tag, &tag)) return false;
    }
    if (size - offset < sizeof(uint64_t)) return false;
    const uint64_t count = binary::ReadU64Le(data + offset);
    offset += sizeof(uint64_t);
    constexpr size_t kIntervalSize = 2 * sizeof(uint64_t);
    if (count == 0 || count > kMaxIntervalsPerSid || count > (size - offset) / kIntervalSize)
      return false;
    auto& intervals = decoded.sets_[{sid, tag}];
    if (intervals.size() > kMaxIntervalsPerSid - static_cast<size_t>(count)) return false;
    for (uint64_t j = 0; j < count; ++j) {
      const uint64_t start = binary::ReadU64Le(data + offset);
      const uint64_t end = binary::ReadU64Le(data + offset + sizeof(uint64_t));
      offset += kIntervalSize;
      if (start == 0 || end <= start) return false;
      intervals.push_back({start, end});
    }
  }
  if (offset != size) return false;
  for (auto& [tsid, intervals] : decoded.sets_) {
    (void)tsid;
    if (!NormalizeIntervals(&intervals)) return false;
  }
  *out = std::move(decoded);
  return true;
}

mes_error_t GtidSet::EncodeBinary(std::vector<uint8_t>* out) const {
  if (out == nullptr) return MES_ERR_NULL_ARG;
  out->clear();
  const bool tagged = std::any_of(sets_.begin(), sets_.end(),
                                  [](const auto& entry) { return !entry.first.tag.empty(); });
  size_t size = sizeof(uint64_t);
  for (const auto& [tsid, intervals] : sets_) {
    size += 16 + (tagged ? 1 + tsid.tag.size() : 0) + sizeof(uint64_t) +
            intervals.size() * 2 * sizeof(uint64_t);
  }
  out->reserve(size);
  const uint64_t count_and_format =
      tagged ? ((static_cast<uint64_t>(sets_.size()) << 8) | (static_cast<uint64_t>(1) << 56) | 1)
             : sets_.size();
  StoreU64Le(out, count_and_format);
  for (const auto& [tsid, intervals] : sets_) {
    out->insert(out->end(), tsid.sid.begin(), tsid.sid.end());
    if (tagged) {
      out->push_back(static_cast<uint8_t>(tsid.tag.size() << 1));
      out->insert(out->end(), tsid.tag.begin(), tsid.tag.end());
    }
    StoreU64Le(out, intervals.size());
    for (const auto& interval : intervals) {
      StoreU64Le(out, interval.start);
      StoreU64Le(out, interval.end);
    }
  }
  return MES_OK;
}

bool GtidSet::Merge(const GtidSet& other) {
  SetMap merged = sets_;
  for (const auto& [tsid, intervals] : other.sets_) {
    auto& target = merged[tsid];
    if (target.size() > kMaxIntervalsPerSid - intervals.size()) return false;
    target.insert(target.end(), intervals.begin(), intervals.end());
    if (!NormalizeIntervals(&target)) return false;
  }
  sets_ = std::move(merged);
  return true;
}

bool GtidSet::IsSubsetOf(const GtidSet& other) const {
  for (const auto& [tsid, intervals] : sets_) {
    const auto other_it = other.sets_.find(tsid);
    if (other_it == other.sets_.end()) return false;

    const auto& covering = other_it->second;
    size_t covering_index = 0;
    for (const auto& interval : intervals) {
      while (covering_index < covering.size() && covering[covering_index].end <= interval.start) {
        ++covering_index;
      }
      if (covering_index == covering.size() || covering[covering_index].start > interval.start ||
          covering[covering_index].end < interval.end) {
        return false;
      }
    }
  }
  return true;
}

bool GtidSet::Add(const Sid& sid, Interval interval) { return Add(sid, "", interval); }

bool GtidSet::Add(const Sid& sid, const std::string& tag, Interval interval) {
  if (interval.start == 0 || interval.end <= interval.start) return false;
  std::string normalized_tag;
  if (!tag.empty() && !NormalizeTag(tag, &normalized_tag)) return false;
  auto& intervals = sets_[{sid, normalized_tag}];
  if (intervals.size() == kMaxIntervalsPerSid) return false;
  intervals.push_back(interval);
  return NormalizeIntervals(&intervals);
}

std::string GtidSet::ToString() const {
  std::string result;
  for (const auto& [tsid, intervals] : sets_) {
    if (intervals.empty()) continue;
    if (!result.empty()) result += ',';
    result += FormatSid(tsid.sid);
    if (!tsid.tag.empty()) {
      result += ':';
      result += tsid.tag;
    }
    for (const auto& interval : intervals) {
      result += ':';
      result += std::to_string(interval.start);
      result += '-';
      result += std::to_string(interval.end - 1);
    }
  }
  return result;
}

bool GtidSet::NormalizeIntervals(std::vector<Interval>* intervals) {
  if (intervals == nullptr || intervals->size() > kMaxIntervalsPerSid) return false;
  std::sort(intervals->begin(), intervals->end(), [](const Interval& lhs, const Interval& rhs) {
    return lhs.start < rhs.start || (lhs.start == rhs.start && lhs.end < rhs.end);
  });
  size_t write = 0;
  for (const auto& interval : *intervals) {
    if (write == 0 || interval.start > (*intervals)[write - 1].end) {
      (*intervals)[write++] = interval;
    } else if (interval.end > (*intervals)[write - 1].end) {
      (*intervals)[write - 1].end = interval.end;
    }
  }
  intervals->resize(write);
  return true;
}

mes_error_t GtidSet::ParseUuid(const std::string& text, Sid* out) {
  if (out == nullptr || text.size() != 36 || text[8] != '-' || text[13] != '-' || text[18] != '-' ||
      text[23] != '-') {
    return MES_ERR_INVALID_ARG;
  }
  size_t index = 0;
  for (size_t i = 0; i < text.size(); ++i) {
    if (text[i] == '-') continue;
    const char high = text[i];
    if (++i == text.size() || text[i] == '-') return MES_ERR_INVALID_ARG;
    const char low = text[i];
    const auto nibble = [](char ch) -> int {
      if (ch >= '0' && ch <= '9') return ch - '0';
      if (ch >= 'a' && ch <= 'f') return ch - 'a' + 10;
      if (ch >= 'A' && ch <= 'F') return ch - 'A' + 10;
      return -1;
    };
    const int hi = nibble(high);
    const int lo = nibble(low);
    if (hi < 0 || lo < 0 || index == out->size()) return MES_ERR_INVALID_ARG;
    (*out)[index++] = static_cast<uint8_t>((hi << 4) | lo);
  }
  return index == out->size() ? MES_OK : MES_ERR_INVALID_ARG;
}

mes_error_t GtidSet::ParseInterval(const std::string& text, Interval* out) {
  if (out == nullptr) return MES_ERR_NULL_ARG;
  const std::string value = Trim(text);
  const size_t dash = value.find('-');
  const auto parse_number = [](const std::string& number, uint64_t* value_out) {
    if (number.empty()) return false;
    char* end = nullptr;
    errno = 0;
    const unsigned long long value = std::strtoull(number.c_str(), &end, 10);
    if (end == number.c_str() || *end != '\0' || errno == ERANGE || value == 0 ||
        value >= INT64_MAX) {
      return false;
    }
    *value_out = static_cast<uint64_t>(value);
    return true;
  };
  uint64_t start = 0;
  uint64_t end = 0;
  if (dash == std::string::npos) {
    if (!parse_number(value, &start)) return MES_ERR_INVALID_ARG;
    end = start + 1;
  } else {
    if (!parse_number(value.substr(0, dash), &start) ||
        !parse_number(value.substr(dash + 1), &end) || end < start) {
      return MES_ERR_INVALID_ARG;
    }
    ++end;
  }
  *out = {start, end};
  return MES_OK;
}

bool GtidSet::NormalizeTag(const std::string& text, std::string* out) {
  if (out == nullptr || text.empty() || text.size() > 32 || !IsTagFirst(text.front())) return false;
  std::string normalized;
  normalized.reserve(text.size());
  for (char ch : text) {
    if (!IsTagFirst(ch) && (ch < '0' || ch > '9')) return false;
    normalized += static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
  }
  *out = std::move(normalized);
  return true;
}

void GtidSet::StoreU64Le(std::vector<uint8_t>* out, uint64_t value) {
  for (int i = 0; i < 8; ++i) out->push_back(static_cast<uint8_t>(value >> (i * 8)));
}

std::string GtidSet::FormatSid(const Sid& sid) {
  static constexpr char kHex[] = "0123456789abcdef";
  std::string text;
  text.reserve(36);
  for (size_t i = 0; i < sid.size(); ++i) {
    if (i == 4 || i == 6 || i == 8 || i == 10) text += '-';
    text += kHex[sid[i] >> 4];
    text += kHex[sid[i] & 0x0F];
  }
  return text;
}

}  // namespace mes
