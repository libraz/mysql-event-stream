// libFuzzer harness for the binlog parser entry point.
//
// Feeds arbitrary attacker-controlled bytes through the public C ABI
// (mes_feed / mes_next_event), exercising the event-header parser, state
// machine, TABLE_MAP metadata parser, and row decoder against malformed
// input. Build with: cmake -B build-fuzz -DMES_ENABLE_FUZZ=ON and run the
// resulting `fuzz_feed` binary. Intended to be combined with ASan/UBSan.

#include <cstddef>
#include <cstdint>
#include <cstring>
#ifdef MES_FUZZ_STANDALONE
#include <fstream>
#include <iterator>
#endif
#include <vector>

#include "mes.h"

namespace {

// Corpus files are text so they remain reviewable in git. A MES_HEX prefix
// denotes an exact binlog byte sequence encoded as hexadecimal; normal
// libFuzzer-generated inputs still flow through untouched.
std::vector<uint8_t> DecodeHexCorpus(const uint8_t* data, size_t size) {
  constexpr char kPrefix[] = "MES_HEX\n";
  if (size < sizeof(kPrefix) - 1 || std::memcmp(data, kPrefix, sizeof(kPrefix) - 1) != 0) {
    return {};
  }

  std::vector<uint8_t> decoded;
  int high_nibble = -1;
  for (size_t i = sizeof(kPrefix) - 1; i < size; ++i) {
    const uint8_t ch = data[i];
    if (ch == ' ' || ch == '\n' || ch == '\r' || ch == '\t') continue;
    const int value = ch >= '0' && ch <= '9'   ? ch - '0'
                      : ch >= 'a' && ch <= 'f' ? ch - 'a' + 10
                      : ch >= 'A' && ch <= 'F' ? ch - 'A' + 10
                                               : -1;
    if (value < 0) return {};
    if (high_nibble < 0) {
      high_nibble = value;
    } else {
      decoded.push_back(static_cast<uint8_t>((high_nibble << 4) | value));
      high_nibble = -1;
    }
  }
  return high_nibble < 0 ? decoded : std::vector<uint8_t>{};
}

// Feed one input to a fresh engine configured for the given checksum framing.
void FeedWithChecksumFraming(const uint8_t* data, size_t size, bool checksum_enabled) {
  mes_engine_t* engine = mes_create();
  if (engine == nullptr) {
    return;
  }
  mes_set_checksum_enabled(engine, checksum_enabled ? 1 : 0);

  // Feed in small chunks to exercise the partial-event reassembly path.
  constexpr size_t kChunk = 7;
  size_t offset = 0;
  while (offset < size) {
    const size_t len = (size - offset < kChunk) ? (size - offset) : kChunk;
    size_t consumed = 0;
    if (mes_feed(engine, data + offset, len, &consumed) != MES_OK) {
      // On a parse/decode error the contract requires a reset before
      // feeding can continue; exercise that recovery path too.
      mes_reset(engine);
    }
    offset += len;

    // Drain any events produced so the row decoder runs on the input.
    const mes_event_t* event = nullptr;
    while (mes_next_event(engine, &event) == MES_OK && event != nullptr) {
    }
  }

  mes_destroy(engine);
}

}  // namespace

/**
 * @brief libFuzzer entry point.
 *
 * Each input is fed to a fresh engine in chunks so that the incremental
 * feeding path (partial events spanning multiple mes_feed calls) is also
 * exercised. All drained events are read out to drive the decoder.
 *
 * The input is run twice, once per checksum framing. Checksums are enabled by
 * default and a real-server corpus seed pins them on via its
 * FORMAT_DESCRIPTION_EVENT, so a single run rejects every mutated byte at the
 * CRC32 gate before the TABLE_MAP parser or the row decoder ever sees it. The
 * checksum-disabled run is what carries mutated bytes into those parsers.
 */
extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  const std::vector<uint8_t> decoded = DecodeHexCorpus(data, size);
  if (!decoded.empty()) {
    data = decoded.data();
    size = decoded.size();
  }

  FeedWithChecksumFraming(data, size, true);
  FeedWithChecksumFraming(data, size, false);
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
