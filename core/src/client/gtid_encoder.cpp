#include "client/gtid_encoder.h"

#include "client/gtid_set.h"
#include "mariadb_gtid.h"

namespace mes {
namespace {

/** @brief Trim ASCII whitespace from both ends of a GTID token. */
std::string Trim(const std::string& text) {
  const size_t begin = text.find_first_not_of(" \t\n\r");
  if (begin == std::string::npos) return {};
  const size_t end = text.find_last_not_of(" \t\n\r");
  return text.substr(begin, end - begin + 1);
}

}  // namespace

mes_error_t GtidEncoder::Encode(const char* gtid_set, std::vector<uint8_t>* out) {
  if (gtid_set == nullptr || out == nullptr) return MES_ERR_NULL_ARG;
  GtidSet set;
  const mes_error_t rc = GtidSet::Parse(gtid_set, &set);
  return rc == MES_OK ? set.EncodeBinary(out) : rc;
}

std::string GtidEncoder::NormalizeSingleSid(const std::string& gtid) {
  // Padding must not decide which rule applies, so every comparison below is
  // made on the trimmed token. A token of whitespace alone carries no position
  // and is handed back untouched for the parser to reject.
  const std::string text = Trim(gtid);
  if (text.empty()) return gtid;
  if (MariaDBGtid::IsMariaDBGtidFormat(text)) return text;
  const size_t colon_pos = text.find(':');
  if (colon_pos == std::string::npos) return text;

  // A tag carries no interval semantics: "uuid:tag:N" denotes the same single
  // transaction as "uuid:N", so split a leading tag off before deciding
  // whether what remains is a bare transaction number.
  size_t intervals_pos = colon_pos + 1;
  const size_t tag_separator = text.find(':', intervals_pos);
  if (tag_separator != std::string::npos) {
    std::string tag;
    if (!GtidSet::NormalizeTag(text.substr(intervals_pos, tag_separator - intervals_pos), &tag)) {
      return text;
    }
    intervals_pos = tag_separator + 1;
  }

  const std::string intervals = text.substr(intervals_pos);
  if (intervals == "0") return {};
  if (intervals.find('-') != std::string::npos || intervals.find(':') != std::string::npos) {
    return text;
  }
  return text.substr(0, intervals_pos) + "1-" + intervals;
}

std::string GtidEncoder::ConvertSingleGtidToRange(const std::string& gtid) {
  if (gtid.empty()) return gtid;
  if (gtid.find(',') == std::string::npos) return NormalizeSingleSid(gtid);

  std::string result;
  bool has_token = false;
  size_t pos = 0;
  while (pos < gtid.size()) {
    const size_t comma = gtid.find(',', pos);
    const std::string part =
        Trim(gtid.substr(pos, comma == std::string::npos ? std::string::npos : comma - pos));
    pos = comma == std::string::npos ? gtid.size() : comma + 1;
    if (part.empty()) continue;
    has_token = true;
    const std::string converted = NormalizeSingleSid(part);
    if (converted.empty()) continue;
    if (!result.empty()) result += ',';
    result += converted;
  }
  // Only a "uuid:0" entry may reduce a non-empty request to the empty set. Text
  // that held no token at all goes back unchanged so the parser rejects it: read
  // as the empty set it would instead request every binlog the server retains.
  return has_token ? result : gtid;
}

}  // namespace mes
