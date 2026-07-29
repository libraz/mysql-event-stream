#include <cstddef>
#include <cstdint>
#ifdef MES_FUZZ_STANDALONE
#include <fstream>
#include <iterator>
#endif
#include <string>
#include <vector>

#include "mariadb_event_parser.h"
#include "protocol/mysql_auth.h"
#include "protocol/mysql_packet.h"
#include "protocol/mysql_query.h"

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  if (data == nullptr) return 0;

  size_t pos = 0;
  uint64_t fixed = 0;
  mes::protocol::ReadLenEncInt(data, size, &pos);
  pos = 0;
  mes::protocol::ReadFixedIntChecked(data, size, &pos, size % 10, &fixed);

  uint16_t error_code = 0;
  std::string error_message;
  mes::protocol::ParseErrPacketPayload(data, size, &error_code, &error_message);

  const size_t split = size / 2;
  const std::string password(reinterpret_cast<const char*>(data), split);
  const uint8_t* salt = data + split;
  const size_t salt_size = size - split;
  std::vector<uint8_t> response;
  mes::protocol::AuthNativePassword(password, salt, salt_size, &response);
  mes::protocol::AuthCachingSha2Password(password, salt, salt_size, &response);

  std::vector<uint8_t> payload(data, data + size);
  mes::protocol::QueryResultRow row;
  mes::protocol::ParseTextResultRow(payload, size == 0 ? 0 : data[0] % 32, &row);

  std::string gtid;
  bool standalone = false;
  mes::MariaDBEventParser::ExtractGtid(data, size, &gtid, &standalone);
  std::vector<mes::MariaDBGtid> gtid_list;
  mes::MariaDBEventParser::ParseGtidList(data, size, &gtid_list);
  std::string annotation;
  mes::MariaDBEventParser::ExtractAnnotateRows(data, size, false, &annotation);
  mes::MariaDBEventParser::ExtractAnnotateRows(data, size, true, &annotation);
  return 0;
}

#ifdef MES_FUZZ_STANDALONE
int main(int argc, char** argv) {
  if (argc != 2) return 2;
  std::ifstream input(argv[1], std::ios::binary);
  if (!input) return 3;
  const std::vector<uint8_t> bytes((std::istreambuf_iterator<char>(input)),
                                   std::istreambuf_iterator<char>());
  return LLVMFuzzerTestOneInput(bytes.data(), bytes.size());
}
#endif
