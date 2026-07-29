#include "client/gtid_encoder.h"

#include "client/gtid_set.h"
#include "mariadb_gtid.h"

namespace mes {

mes_error_t GtidEncoder::Encode(const char* gtid_set, std::vector<uint8_t>* out) {
  if (gtid_set == nullptr || out == nullptr) return MES_ERR_NULL_ARG;
  GtidSet set;
  const mes_error_t rc = GtidSet::Parse(gtid_set, &set);
  return rc == MES_OK ? set.EncodeBinary(out) : rc;
}

std::string GtidEncoder::NormalizeSingleSid(const std::string& gtid) {
  if (MariaDBGtid::IsMariaDBGtidFormat(gtid)) return gtid;
  const size_t colon_pos = gtid.find(':');
  if (colon_pos == std::string::npos) return gtid;

  const std::string after_colon = gtid.substr(colon_pos + 1);
  if (after_colon == "0") return {};
  if (after_colon.find('-') != std::string::npos || after_colon.find(':') != std::string::npos) {
    return gtid;
  }
  return gtid.substr(0, colon_pos) + ":1-" + after_colon;
}

std::string GtidEncoder::ConvertSingleGtidToRange(const std::string& gtid) {
  if (gtid.empty()) return gtid;
  if (gtid.find(',') == std::string::npos) return NormalizeSingleSid(gtid);

  std::string result;
  size_t pos = 0;
  while (pos < gtid.size()) {
    const size_t comma = gtid.find(',', pos);
    std::string part =
        gtid.substr(pos, comma == std::string::npos ? std::string::npos : comma - pos);
    pos = comma == std::string::npos ? gtid.size() : comma + 1;
    const size_t begin = part.find_first_not_of(" \t\n\r");
    if (begin == std::string::npos) continue;
    const size_t end = part.find_last_not_of(" \t\n\r");
    part = part.substr(begin, end - begin + 1);
    const std::string converted = NormalizeSingleSid(part);
    if (converted.empty()) continue;
    if (!result.empty()) result += ',';
    result += converted;
  }
  return result;
}

}  // namespace mes
