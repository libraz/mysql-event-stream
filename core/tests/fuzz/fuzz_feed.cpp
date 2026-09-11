// libFuzzer harness for the binlog parser entry point.
//
// Feeds arbitrary attacker-controlled bytes through the public C ABI
// (mes_feed / mes_next_event), exercising the event-header parser, state
// machine, TABLE_MAP metadata parser, and row decoder against malformed
// input. Build with: cmake -B build-fuzz -DMES_ENABLE_FUZZ=ON and run the
// resulting `fuzz_feed` binary. Intended to be combined with ASan/UBSan.
//
// Compiled a second time with MES_FUZZ_STANDALONE, the same harness replays one
// corpus file per invocation and additionally checks that the file reaches the
// code path its name states, so CTest covers every checked-in seed.

#include <cstddef>
#include <cstdint>
#include <cstring>
#ifdef MES_FUZZ_STANDALONE
#include <cstdio>
#include <fstream>
#include <iterator>
#include <string>

#include "event_header.h"
#include "state_machine.h"
#endif
#include <vector>

#include "mes.h"

namespace {

/// @brief Outcome of interpreting an input as a hex-encoded corpus file.
enum class HexCorpusStatus {
  kNotHexCorpus,      ///< No MES_HEX prefix: a raw or fuzzer-generated input.
  kDecoded,           ///< Prefix present and the whole body decoded.
  kInvalidCharacter,  ///< Prefix present, a byte in the body is not a hex digit.
  kOddDigitCount,     ///< Prefix present, the body ends on a half-decoded byte.
};

struct HexCorpusDecode {
  HexCorpusStatus status = HexCorpusStatus::kNotHexCorpus;
  size_t offset = 0;      ///< Byte offset in the input of the offending character.
  uint8_t character = 0;  ///< The offending character, for kInvalidCharacter.
  size_t digits = 0;      ///< Hex digits seen, for kOddDigitCount.
  std::vector<uint8_t> bytes;
};

// Corpus files are text so they remain reviewable in git. A MES_HEX prefix
// denotes an exact binlog byte sequence encoded as hexadecimal; normal
// libFuzzer-generated inputs still flow through untouched.
//
// "no prefix" and "prefix present but the body is malformed" are reported
// distinctly. Collapsing them lets a corrupted seed fall back to being fed as
// its own ASCII text, which reaches no parser and looks like a framing failure
// rather than an encoding failure.
HexCorpusDecode DecodeHexCorpus(const uint8_t* data, size_t size) {
  constexpr char kPrefix[] = "MES_HEX\n";
  HexCorpusDecode result;
  if (size < sizeof(kPrefix) - 1 || std::memcmp(data, kPrefix, sizeof(kPrefix) - 1) != 0) {
    return result;
  }

  int high_nibble = -1;
  for (size_t i = sizeof(kPrefix) - 1; i < size; ++i) {
    const uint8_t ch = data[i];
    if (ch == ' ' || ch == '\n' || ch == '\r' || ch == '\t') continue;
    const int value = ch >= '0' && ch <= '9'   ? ch - '0'
                      : ch >= 'a' && ch <= 'f' ? ch - 'a' + 10
                      : ch >= 'A' && ch <= 'F' ? ch - 'A' + 10
                                               : -1;
    if (value < 0) {
      result.status = HexCorpusStatus::kInvalidCharacter;
      result.offset = i;
      result.character = ch;
      result.bytes.clear();
      return result;
    }
    ++result.digits;
    if (high_nibble < 0) {
      high_nibble = value;
    } else {
      result.bytes.push_back(static_cast<uint8_t>((high_nibble << 4) | value));
      high_nibble = -1;
    }
  }
  if (high_nibble >= 0) {
    result.status = HexCorpusStatus::kOddDigitCount;
    result.offset = size;
    result.bytes.clear();
    return result;
  }
  result.status = HexCorpusStatus::kDecoded;
  return result;
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
 *
 * A malformed hex wrapper is not an error here: a generated input may open with
 * the prefix by chance, and aborting the run over it would end the campaign.
 * Only the corpus replay mode treats that as a failure.
 */
extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  const HexCorpusDecode decoded = DecodeHexCorpus(data, size);
  if (decoded.status == HexCorpusStatus::kDecoded && !decoded.bytes.empty()) {
    data = decoded.bytes.data();
    size = decoded.bytes.size();
  }

  FeedWithChecksumFraming(data, size, true);
  FeedWithChecksumFraming(data, size, false);
  return 0;
}

#ifdef MES_FUZZ_STANDALONE
namespace {

/// @brief The code path a corpus file's name states it exercises.
enum class SeedTarget {
  kQueryEventBody,    ///< A QUERY_EVENT must be framed and dispatched.
  kRowEventDecode,    ///< A row event must be framed and reach the decoder.
  kEventHeaderGuard,  ///< The event header / length guard itself is the target.
};

struct SeedTargetRule {
  const char* name_fragment;
  SeedTarget target;
};

// Matched against the corpus file name, most specific fragment first. A name
// matching no rule is an error rather than a pass: without a stated target
// nothing distinguishes a seed that exercises a parser from placeholder text
// that the framing guard discards.
constexpr SeedTargetRule kSeedTargetRules[] = {
    {"oversized_event_header", SeedTarget::kEventHeaderGuard},
    {"query_event", SeedTarget::kQueryEventBody},
    {"rows", SeedTarget::kRowEventDecode},
};

struct FramingOutcome {
  bool target_framed = false;   ///< An event of the named kind is fully present.
  bool guard_rejected = false;  ///< A framing/length guard stopped the walk.
};

/**
 * @brief Re-derive the event framing independently of the engine.
 *
 * Mirrors the guards EventStreamParser::Feed applies before a body reaches any
 * parser: a full common header, an event_length no smaller than the header plus
 * the checksum trailer and no larger than the engine's default cap, and the
 * whole declared length present in the input. Deciding this outside the engine
 * is what separates "the named parser ran on this seed" from "the seed never
 * got past the framing guard" — a distinction no C ABI return value exposes,
 * because control events produce no event for mes_next_event() to hand back.
 *
 * @param data            Seed bytes, hex corpus wrapper already decoded.
 * @param size            Number of seed bytes.
 * @param checksum_enabled Framing to assume, matching mes_set_checksum_enabled().
 * @param target          Code path the file name states the seed covers.
 */
FramingOutcome WalkFraming(const uint8_t* data, size_t size, bool checksum_enabled,
                           SeedTarget target) {
  const size_t min_event_length =
      mes::kEventHeaderSize + (checksum_enabled ? mes::kChecksumSize : 0);
  FramingOutcome outcome;
  size_t offset = 0;
  while (offset < size) {
    mes::EventHeader header;
    if (!mes::ParseEventHeader(data + offset, size - offset, &header) ||
        header.event_length < min_event_length || header.event_length > mes::kDefaultMaxEventSize ||
        size - offset < header.event_length) {
      outcome.guard_rejected = true;
      break;
    }
    switch (target) {
      case SeedTarget::kQueryEventBody:
        if (header.type_code == static_cast<uint8_t>(mes::BinlogEventType::kQueryEvent)) {
          outcome.target_framed = true;
        }
        break;
      case SeedTarget::kRowEventDecode:
        if (mes::IsRowEvent(header.type_code)) outcome.target_framed = true;
        break;
      case SeedTarget::kEventHeaderGuard:
        break;
    }
    offset += header.event_length;
  }
  return outcome;
}

/**
 * @brief Check that a seed can reach the code path its file name names.
 *
 * Both framings are required to agree: the engine is configured one way per
 * run, so a seed whose framing only lines up with one of them silently stops
 * covering the other.
 *
 * @param name Corpus file base name; it selects the expectation.
 * @param data Seed bytes, hex corpus wrapper already decoded.
 * @param size Number of seed bytes.
 * @return true when the seed reaches its named path (or, for a seed naming the
 *         header guard, when that guard rejects it).
 */
bool SeedReachesNamedPath(const char* name_arg, const uint8_t* data, size_t size) {
  const std::string name(name_arg);
  for (const SeedTargetRule& rule : kSeedTargetRules) {
    if (name.find(rule.name_fragment) == std::string::npos) continue;
    const FramingOutcome with_checksum = WalkFraming(data, size, true, rule.target);
    const FramingOutcome without_checksum = WalkFraming(data, size, false, rule.target);

    if (rule.target == SeedTarget::kEventHeaderGuard) {
      if (with_checksum.guard_rejected && without_checksum.guard_rejected) return true;
      std::fprintf(stderr, "%s: names the event header guard but frames cleanly instead\n",
                   name.c_str());
      return false;
    }
    if (with_checksum.target_framed && without_checksum.target_framed) return true;
    std::fprintf(stderr,
                 "%s: names '%s' but no such event is fully framed (checksum on: %s, checksum "
                 "off: %s); the framing/length guard rejects the seed before that parser runs\n",
                 name.c_str(), rule.name_fragment, with_checksum.target_framed ? "yes" : "no",
                 without_checksum.target_framed ? "yes" : "no");
    return false;
  }

  std::fprintf(stderr,
               "%s: no rule matches this file name, so nothing states which code path the seed "
               "covers. Add a name fragment for it to kSeedTargetRules in "
               "core/tests/fuzz/fuzz_feed.cpp, paired with the SeedTarget the seed must reach, "
               "and extend the SeedTarget enum if the path is a new one.\n",
               name.c_str());
  return false;
}

/**
 * @brief Report a hex corpus file whose wrapper is present but undecodable.
 *
 * @param name   Corpus file base name.
 * @param decode Decoder outcome for that file.
 * @return true when the file is usable as a seed, false when it is not.
 */
bool ReportHexCorpusDecode(const char* name, const HexCorpusDecode& decode) {
  switch (decode.status) {
    case HexCorpusStatus::kNotHexCorpus:
    case HexCorpusStatus::kDecoded:
      return true;
    case HexCorpusStatus::kInvalidCharacter:
      std::fprintf(stderr,
                   "%s: carries the MES_HEX prefix but the body did not decode: byte offset %zu is "
                   "'%c' (0x%02x), not a hex digit. The seed's bytes never reach the engine.\n",
                   name, decode.offset,
                   decode.character >= 0x20 && decode.character < 0x7f
                       ? static_cast<char>(decode.character)
                       : '?',
                   static_cast<unsigned>(decode.character));
      return false;
    case HexCorpusStatus::kOddDigitCount:
      std::fprintf(stderr,
                   "%s: carries the MES_HEX prefix but the body did not decode: %zu hex digits is "
                   "an odd count, so the last byte is half written. The seed's bytes never reach "
                   "the engine.\n",
                   name, decode.digits);
      return false;
  }
  return false;
}

}  // namespace

int main(int argc, char** argv) {
  if (argc != 2) return 2;
  std::ifstream input(argv[1], std::ios::binary);
  if (!input) return 3;
  const std::vector<uint8_t> bytes((std::istreambuf_iterator<char>(input)),
                                   std::istreambuf_iterator<char>());

  std::string name(argv[1]);
  const size_t separator = name.find_last_of("/\\");
  if (separator != std::string::npos) name = name.substr(separator + 1);

  const HexCorpusDecode decoded = DecodeHexCorpus(bytes.data(), bytes.size());
  if (!ReportHexCorpusDecode(name.c_str(), decoded)) return 1;

  if (LLVMFuzzerTestOneInput(bytes.data(), bytes.size()) != 0) return 1;

  const std::vector<uint8_t>& seed =
      decoded.status == HexCorpusStatus::kDecoded ? decoded.bytes : bytes;
  return SeedReachesNamedPath(name.c_str(), seed.data(), seed.size()) ? 0 : 1;
}
#endif
