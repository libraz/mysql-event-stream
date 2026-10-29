// ChecksumTracker: per-FDE algorithm derivation, trailer framing and the two
// consumer switches. Scenario rows come from models/checksum.json.

#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>
#include <system_error>
#include <vector>

#include "mes/codec/checksum.h"
#include "mes/codec/crc32.h"
#include "mes/codec/event_header.h"
#include "mes/codec/event_types.h"

namespace mes::codec {
namespace {

using Bytes = std::vector<uint8_t>;

constexpr size_t kPostHeaderArrayLength = 41;
constexpr size_t kLegacyPostHeaderArrayLength = 27;

void PutU32(uint8_t* out, uint32_t value) {
  for (int i = 0; i < 4; ++i) out[i] = static_cast<uint8_t>(value >> (8 * i));
}

void WriteTrailer(Bytes& frame) {
  PutU32(frame.data() + frame.size() - kChecksumSize,
         ComputeCrc32(frame.data(), frame.size() - kChecksumSize));
}

// Header + body, plus a correct CRC32 trailer when @p trailer is set.
Bytes Frame(EventType type, uint16_t flags, const Bytes& body, bool trailer) {
  Bytes frame(kEventHeaderSize, 0);
  frame[4] = static_cast<uint8_t>(type);
  PutU32(frame.data() + 5, 1);
  frame[17] = static_cast<uint8_t>(flags);
  frame[18] = static_cast<uint8_t>(flags >> 8);
  frame.insert(frame.end(), body.begin(), body.end());
  if (trailer) frame.resize(frame.size() + kChecksumSize, 0);
  PutU32(frame.data() + 9, static_cast<uint32_t>(frame.size()));
  if (trailer) WriteTrailer(frame);
  return frame;
}

// FDE as a server of @p version writes it; kUndefined omits the alg byte and trailer.
Bytes Fde(const std::string& version, ChecksumAlgorithm algorithm) {
  Bytes body = {4, 0};
  Bytes version_field(50, 0);
  std::memcpy(version_field.data(), version.data(), version.size());
  body.insert(body.end(), version_field.begin(), version_field.end());
  body.insert(body.end(), {0, 0, 0, 0, static_cast<uint8_t>(kEventHeaderSize)});
  const bool legacy = algorithm == ChecksumAlgorithm::kUndefined;
  body.insert(body.end(), legacy ? kLegacyPostHeaderArrayLength : kPostHeaderArrayLength, 0x0A);
  if (legacy) return Frame(EventType::kFormatDescriptionEvent, 0, body, false);
  body.push_back(static_cast<uint8_t>(algorithm));
  // A checksum-aware server always writes a real CRC on the FDE, even under OFF.
  return Frame(EventType::kFormatDescriptionEvent, 0, body, true);
}

Bytes RotateBody(const std::string& name) {
  Bytes body(8, 0);
  body[0] = 4;
  body.insert(body.end(), name.begin(), name.end());
  return body;
}

Status Feed(ChecksumTracker& tracker, const Bytes& frame, FrameLayout* layout) {
  EventHeader header;
  const Status parsed = ParseEventHeader(frame.data(), frame.size(), &header);
  if (parsed != Status::kOk) return parsed;
  return tracker.OnEvent(header, frame.data(), frame.size(), layout);
}

bool ReadFixture(const char* name, Bytes* out) {
  const auto path = std::filesystem::path(__FILE__).parent_path() / "fixtures" / name;
  std::error_code ec;
  if (!std::filesystem::is_regular_file(path, ec)) return false;
  std::ifstream in(path, std::ios::binary);
  if (!in) return false;
  out->assign(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
  return !out->empty();
}

// --- Version gate ---

TEST(CodecFraming, ServerVersionGate) {
  EXPECT_TRUE(ServerVersionPredatesChecksums("5.6.0"));
  EXPECT_TRUE(ServerVersionPredatesChecksums("5.5.62-log"));
  EXPECT_FALSE(ServerVersionPredatesChecksums("5.6.1"));
  EXPECT_FALSE(ServerVersionPredatesChecksums("8.4.13"));
  EXPECT_FALSE(ServerVersionPredatesChecksums("9.7.4"));
  EXPECT_TRUE(ServerVersionPredatesChecksums("5.2.14-MariaDB"));
  EXPECT_FALSE(ServerVersionPredatesChecksums("5.3.0-MariaDB"));
  EXPECT_FALSE(ServerVersionPredatesChecksums("5.5.68-MariaDB"));
  EXPECT_FALSE(ServerVersionPredatesChecksums("11.8.9-MariaDB-ubu2404-log"));
  EXPECT_TRUE(ServerVersionPredatesChecksums(""));
}

// --- Mandatory rows ---

// binlog_checksum=NONE on a real 8.4.13 server: alg byte 0 at len-5, real CRC in
// the last 4 bytes, and the following events carry no trailer.
TEST(CodecFraming, RealNoneChecksumFde) {
  Bytes file;
  ASSERT_TRUE(ReadFixture("mysql84_none_tagged.bin", &file));
  ASSERT_GT(file.size(), kBinlogMagicOffset + kEventHeaderSize);
  const uint8_t* fde = file.data() + kBinlogMagicOffset;
  EventHeader header;
  ASSERT_EQ(ParseEventHeader(fde, file.size() - kBinlogMagicOffset, &header), Status::kOk);
  ASSERT_EQ(header.type_code, static_cast<uint8_t>(EventType::kFormatDescriptionEvent));
  ASSERT_EQ(header.event_length, 123u);
  EXPECT_EQ(fde[header.event_length - 5], 0);
  EXPECT_NE(fde[header.event_length - 1], 0);  // Not an "alg byte last" layout.

  ChecksumAlgorithm declared = ChecksumAlgorithm::kCrc32;
  ASSERT_EQ(ReadFdeChecksumAlgorithm(fde, header.event_length, &declared), Status::kOk);
  EXPECT_EQ(declared, ChecksumAlgorithm::kOff);

  ChecksumTracker tracker;  // Default CRC32, as for a raw feed.
  FrameLayout layout;
  ASSERT_EQ(tracker.OnEvent(header, fde, header.event_length, &layout), Status::kOk);
  EXPECT_TRUE(layout.has_trailer);
  EXPECT_EQ(layout.payload_length, 119u);
  EXPECT_EQ(tracker.algorithm(), ChecksumAlgorithm::kOff);

  const uint8_t* next = fde + header.event_length;
  EventHeader next_header;
  ASSERT_EQ(ParseEventHeader(next, kEventHeaderSize, &next_header), Status::kOk);
  ASSERT_EQ(tracker.OnEvent(next_header, next, next_header.event_length, &layout), Status::kOk);
  EXPECT_FALSE(layout.has_trailer);
  EXPECT_EQ(layout.payload_length, next_header.event_length);
}

TEST(CodecFraming, ShortChecksummedFrame) {
  for (uint32_t length = kEventHeaderSize; length < kEventHeaderSize + kChecksumSize; ++length) {
    Bytes frame = Frame(EventType::kXidEvent, 0, {}, false);
    frame.resize(length, 0);
    PutU32(frame.data() + 9, length);
    ChecksumTracker tracker;
    FrameLayout layout;
    EXPECT_EQ(Feed(tracker, frame, &layout), Status::kChecksumMismatch) << length;
    tracker.SetVerify(false);
    EXPECT_EQ(Feed(tracker, frame, &layout), Status::kChecksumMismatch) << length;
  }
  // 23 bytes is the smallest checksummed frame: an empty body.
  ChecksumTracker tracker;
  FrameLayout layout;
  ASSERT_EQ(Feed(tracker, Frame(EventType::kXidEvent, 0, {}, true), &layout), Status::kOk);
  EXPECT_EQ(layout.payload_length, kEventHeaderSize);
  EXPECT_EQ(layout.body_length(), 0u);
}

TEST(CodecFraming, DefaultAlgorithmBeforeFde) {
  ChecksumTracker tracker;
  EXPECT_EQ(tracker.default_algorithm(), ChecksumAlgorithm::kCrc32);
  tracker.SetDefaultAlgorithm(ChecksumAlgorithm::kOff);
  EXPECT_EQ(tracker.algorithm(), ChecksumAlgorithm::kOff);

  FrameLayout layout;
  const Bytes plain = Frame(EventType::kXidEvent, 0, Bytes(8, 7), false);
  ASSERT_EQ(Feed(tracker, plain, &layout), Status::kOk);
  EXPECT_FALSE(layout.has_trailer);
  EXPECT_EQ(layout.body_length(), 8u);

  // An FDE overrides the default for the rest of its file.
  ASSERT_EQ(Feed(tracker, Fde("8.4.13", ChecksumAlgorithm::kCrc32), &layout), Status::kOk);
  EXPECT_EQ(tracker.algorithm(), ChecksumAlgorithm::kCrc32);
  EXPECT_EQ(Feed(tracker, plain, &layout), Status::kChecksumMismatch);
  ASSERT_EQ(Feed(tracker, Frame(EventType::kXidEvent, 0, Bytes(8, 7), true), &layout), Status::kOk);
  EXPECT_TRUE(layout.has_trailer);
  EXPECT_EQ(layout.body_length(), 8u);

  // A new stream starts from the default again.
  tracker.Reset();
  EXPECT_EQ(tracker.algorithm(), ChecksumAlgorithm::kOff);
  ASSERT_EQ(Feed(tracker, plain, &layout), Status::kOk);
  EXPECT_FALSE(layout.has_trailer);
}

TEST(CodecFraming, VerifyDisabledKeepsFraming) {
  Bytes corrupt = Frame(EventType::kXidEvent, 0, Bytes(8, 7), true);
  corrupt[corrupt.size() - 1] ^= 0xFF;
  Bytes corrupt_fde = Fde("8.4.13", ChecksumAlgorithm::kCrc32);
  corrupt_fde[corrupt_fde.size() - 1] ^= 0xFF;

  ChecksumTracker verifying;
  FrameLayout layout;
  EXPECT_EQ(Feed(verifying, corrupt, &layout), Status::kChecksumMismatch);
  EXPECT_EQ(Feed(verifying, corrupt_fde, &layout), Status::kChecksumMismatch);

  ChecksumTracker trusting;
  trusting.SetVerify(false);
  EXPECT_FALSE(trusting.verify());
  ASSERT_EQ(Feed(trusting, corrupt, &layout), Status::kOk);
  EXPECT_TRUE(layout.has_trailer);
  EXPECT_EQ(layout.payload_length, corrupt.size() - kChecksumSize);
  ASSERT_EQ(Feed(trusting, corrupt_fde, &layout), Status::kOk);
  EXPECT_TRUE(layout.has_trailer);
  EXPECT_EQ(trusting.algorithm(), ChecksumAlgorithm::kCrc32);

  // Framing still follows the FDE: under OFF no trailer is stripped.
  ASSERT_EQ(Feed(trusting, Fde("8.4.13", ChecksumAlgorithm::kOff), &layout), Status::kOk);
  const Bytes plain = Frame(EventType::kXidEvent, 0, Bytes(8, 7), false);
  ASSERT_EQ(Feed(trusting, plain, &layout), Status::kOk);
  EXPECT_FALSE(layout.has_trailer);
  EXPECT_EQ(layout.payload_length, plain.size());
}

// --- FDE derivation ---

// The old MES rule trusted the CRC over the alg byte (D4 withdraws it): an FDE
// whose byte at len-5 says CRC32 is CRC32, and a bad trailer is a mismatch.
TEST(CodecFraming, FdeAlgorithmByteIsAuthoritative) {
  Bytes fde = Fde("8.4.13", ChecksumAlgorithm::kCrc32);
  ChecksumTracker tracker;
  FrameLayout layout;
  ASSERT_EQ(Feed(tracker, fde, &layout), Status::kOk);
  EXPECT_EQ(tracker.algorithm(), ChecksumAlgorithm::kCrc32);

  fde[20] ^= 0x80;  // binlog_version byte; trailer no longer matches.
  EXPECT_EQ(Feed(tracker, fde, &layout), Status::kChecksumMismatch);

  // The layout the old rule read as OFF ("alg byte last, no trailer").
  Bytes legacy_off = Fde("8.4.13", ChecksumAlgorithm::kCrc32);
  legacy_off.back() = 0;
  ChecksumAlgorithm declared = ChecksumAlgorithm::kOff;
  ASSERT_EQ(ReadFdeChecksumAlgorithm(legacy_off.data(), legacy_off.size(), &declared), Status::kOk);
  EXPECT_EQ(declared, ChecksumAlgorithm::kCrc32);
  EXPECT_EQ(Feed(tracker, legacy_off, &layout), Status::kChecksumMismatch);
}

// Under OFF the FDE trailer is not recomputed by every source, so it is not checked.
TEST(CodecFraming, OffFdeTrailerIsNotVerified) {
  Bytes fde = Fde("8.4.13", ChecksumAlgorithm::kOff);
  fde[fde.size() - 1] ^= 0xFF;
  ChecksumTracker tracker;
  FrameLayout layout;
  ASSERT_EQ(Feed(tracker, fde, &layout), Status::kOk);
  EXPECT_TRUE(layout.has_trailer);
  EXPECT_EQ(layout.payload_length, fde.size() - kChecksumSize);
  EXPECT_EQ(tracker.algorithm(), ChecksumAlgorithm::kOff);
}

TEST(CodecFraming, FdeUnknownAlgorithmByte) {
  Bytes fde = Fde("8.4.13", ChecksumAlgorithm::kCrc32);
  fde[fde.size() - kChecksumSize - 1] = 2;
  WriteTrailer(fde);
  ChecksumTracker tracker;
  FrameLayout layout;
  EXPECT_EQ(Feed(tracker, fde, &layout), Status::kUnsupported);
  EXPECT_EQ(tracker.algorithm(), ChecksumAlgorithm::kCrc32);
}

TEST(CodecFraming, FdeTooShort) {
  Bytes fixed_only = Fde("8.4.13", ChecksumAlgorithm::kCrc32);
  fixed_only.resize(kEventHeaderSize + 57 + 4);  // Room for a trailer but not the alg byte.
  PutU32(fixed_only.data() + 9, static_cast<uint32_t>(fixed_only.size()));
  ChecksumTracker tracker;
  FrameLayout layout;
  EXPECT_EQ(Feed(tracker, fixed_only, &layout), Status::kTruncated);

  Bytes cut = Frame(EventType::kFormatDescriptionEvent, 0, Bytes(40, 0), false);
  EXPECT_EQ(Feed(tracker, cut, &layout), Status::kTruncated);
}

TEST(CodecFraming, FrameLengthMustMatchHeader) {
  const Bytes frame = Frame(EventType::kXidEvent, 0, Bytes(8, 7), true);
  EventHeader header;
  ASSERT_EQ(ParseEventHeader(frame.data(), frame.size(), &header), Status::kOk);
  ChecksumTracker tracker;
  FrameLayout layout;
  EXPECT_EQ(tracker.OnEvent(header, frame.data(), frame.size() - 1, &layout), Status::kTruncated);
  Bytes longer = frame;
  longer.push_back(0);
  EXPECT_EQ(tracker.OnEvent(header, longer.data(), longer.size(), &layout),
            Status::kMalformedValue);
}

// --- Artificial ROTATE ---

TEST(CodecFraming, ArtificialRotateNeedsMatchingCrc) {
  ChecksumTracker tracker;
  tracker.SetDefaultAlgorithm(ChecksumAlgorithm::kOff);
  FrameLayout layout;

  Bytes bad =
      Frame(EventType::kRotateEvent, kLogEventArtificialFlag, RotateBody("binlog.000002"), true);
  bad[kEventHeaderSize + 8] ^= 0x01;  // Body changed, trailer stale.
  ASSERT_EQ(Feed(tracker, bad, &layout), Status::kOk);
  EXPECT_FALSE(layout.has_trailer);
  EXPECT_EQ(layout.payload_length, bad.size());

  // Without the artificial flag a matching CRC is not evidence of a trailer.
  const Bytes real = Frame(EventType::kRotateEvent, 0, RotateBody("binlog.000002"), true);
  ASSERT_EQ(Feed(tracker, real, &layout), Status::kOk);
  EXPECT_FALSE(layout.has_trailer);

  // Detection is framing, so it still runs with verification disabled.
  tracker.SetVerify(false);
  const Bytes good =
      Frame(EventType::kRotateEvent, kLogEventArtificialFlag, RotateBody("binlog.000002"), true);
  ASSERT_EQ(Feed(tracker, good, &layout), Status::kOk);
  EXPECT_TRUE(layout.has_trailer);
  EXPECT_EQ(layout.body_length(), 8u + 13u);
  EXPECT_EQ(tracker.algorithm(), ChecksumAlgorithm::kOff);
}

// --- Pairwise scenarios (models/checksum.json) ---

enum class Alg { kOff, kCrc32, kUndef };
enum class Flavor { kMySQL, kMariaDB };
enum class Supply { kRawFeed, kClient };
enum class Boundary { kSameAlg, kRotateNewAlg, kArtificialRotate };

struct ScenarioRow {
  Alg alg;
  Flavor flavor;
  Supply supply;
  Boundary boundary;
};

ChecksumAlgorithm ToAlgorithm(Alg alg) {
  switch (alg) {
    case Alg::kOff:
      return ChecksumAlgorithm::kOff;
    case Alg::kCrc32:
      return ChecksumAlgorithm::kCrc32;
    case Alg::kUndef:
      return ChecksumAlgorithm::kUndefined;
  }
  return ChecksumAlgorithm::kUndefined;
}

std::string VersionFor(Flavor flavor, ChecksumAlgorithm algorithm) {
  const bool legacy = algorithm == ChecksumAlgorithm::kUndefined;
  if (flavor == Flavor::kMariaDB) return legacy ? "5.2.14-MariaDB" : "11.8.9-MariaDB-log";
  return legacy ? "5.5.62-log" : "8.4.13";
}

struct Step {
  Bytes frame;
  size_t payload_length;
  bool has_trailer;
  ChecksumAlgorithm algorithm_after;
};

class StreamBuilder {
 public:
  explicit StreamBuilder(Flavor flavor) : flavor_(flavor) {}

  void AddFde(ChecksumAlgorithm algorithm) {
    current_ = algorithm;
    Bytes frame = Fde(VersionFor(flavor_, algorithm), algorithm);
    const bool trailer = algorithm != ChecksumAlgorithm::kUndefined;
    Add(std::move(frame), trailer);
  }

  void AddEvent(EventType type, const Bytes& body) {
    const bool trailer = current_ == ChecksumAlgorithm::kCrc32;
    Add(Frame(type, 0, body, trailer), trailer);
  }

  // A checksum-aware source sends the dump-start ROTATE with CRC32 whatever the file uses.
  void AddArtificialRotate(bool server_knows_checksums) {
    Add(Frame(EventType::kRotateEvent, kLogEventArtificialFlag, RotateBody("binlog.000002"),
              server_knows_checksums),
        server_knows_checksums);
  }

  void set_current(ChecksumAlgorithm algorithm) { current_ = algorithm; }
  const std::vector<Step>& steps() const { return steps_; }

 private:
  void Add(Bytes frame, bool trailer) {
    const size_t payload = frame.size() - (trailer ? kChecksumSize : 0);
    steps_.push_back(Step{std::move(frame), payload, trailer, current_});
  }

  Flavor flavor_;
  ChecksumAlgorithm current_ = ChecksumAlgorithm::kCrc32;
  std::vector<Step> steps_;
};

ChecksumAlgorithm OtherAlgorithm(ChecksumAlgorithm algorithm) {
  return algorithm == ChecksumAlgorithm::kCrc32 ? ChecksumAlgorithm::kOff
                                                : ChecksumAlgorithm::kCrc32;
}

void RunScenario(const ScenarioRow& row) {
  const ChecksumAlgorithm algorithm = ToAlgorithm(row.alg);
  const bool aware = algorithm != ChecksumAlgorithm::kUndefined;
  StreamBuilder stream(row.flavor);
  ChecksumTracker tracker;

  if (row.supply == Supply::kClient) {
    // The client configures framing from the server's setting before the first FDE.
    const ChecksumAlgorithm initial = algorithm == ChecksumAlgorithm::kCrc32
                                          ? ChecksumAlgorithm::kCrc32
                                          : ChecksumAlgorithm::kOff;
    tracker.SetDefaultAlgorithm(initial);
    stream.set_current(initial);
    stream.AddArtificialRotate(aware);
  }
  stream.AddFde(algorithm);
  stream.AddEvent(EventType::kQueryEvent, Bytes(20, 'q'));
  stream.AddEvent(EventType::kXidEvent, Bytes(8, 1));

  ChecksumAlgorithm next = algorithm;
  switch (row.boundary) {
    case Boundary::kSameAlg:
      stream.AddEvent(EventType::kRotateEvent, RotateBody("binlog.000002"));
      break;
    case Boundary::kRotateNewAlg:
      stream.AddEvent(EventType::kRotateEvent, RotateBody("binlog.000002"));
      next = OtherAlgorithm(algorithm);
      break;
    case Boundary::kArtificialRotate:
      stream.AddArtificialRotate(aware);
      break;
  }
  stream.AddFde(next);
  stream.AddEvent(EventType::kXidEvent, Bytes(8, 2));

  size_t index = 0;
  for (const Step& step : stream.steps()) {
    SCOPED_TRACE(testing::Message() << "event " << index++);
    FrameLayout layout;
    ASSERT_EQ(Feed(tracker, step.frame, &layout), Status::kOk);
    EXPECT_EQ(layout.has_trailer, step.has_trailer);
    EXPECT_EQ(layout.payload_length, step.payload_length);
    EXPECT_EQ(tracker.algorithm(), step.algorithm_after);
  }
}

// Rows in models/checksum.json order.
constexpr ScenarioRow kScenarioRows[] = {
    {Alg::kUndef, Flavor::kMySQL, Supply::kRawFeed, Boundary::kSameAlg},
    {Alg::kOff, Flavor::kMariaDB, Supply::kRawFeed, Boundary::kArtificialRotate},
    {Alg::kUndef, Flavor::kMariaDB, Supply::kClient, Boundary::kArtificialRotate},
    {Alg::kCrc32, Flavor::kMariaDB, Supply::kClient, Boundary::kRotateNewAlg},
    {Alg::kCrc32, Flavor::kMySQL, Supply::kRawFeed, Boundary::kRotateNewAlg},
    {Alg::kOff, Flavor::kMySQL, Supply::kClient, Boundary::kArtificialRotate},
    {Alg::kUndef, Flavor::kMySQL, Supply::kRawFeed, Boundary::kRotateNewAlg},
    {Alg::kCrc32, Flavor::kMySQL, Supply::kRawFeed, Boundary::kSameAlg},
    {Alg::kOff, Flavor::kMySQL, Supply::kRawFeed, Boundary::kRotateNewAlg},
    {Alg::kOff, Flavor::kMariaDB, Supply::kClient, Boundary::kSameAlg},
    {Alg::kCrc32, Flavor::kMariaDB, Supply::kRawFeed, Boundary::kArtificialRotate},
};

TEST(CodecFraming, PairwiseChecksumScenarios) {
  size_t row_index = 0;
  for (const ScenarioRow& row : kScenarioRows) {
    SCOPED_TRACE(testing::Message() << "model row " << row_index++);
    RunScenario(row);
  }
}

}  // namespace
}  // namespace mes::codec
