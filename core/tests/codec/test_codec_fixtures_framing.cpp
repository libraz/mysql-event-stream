// Real server binlog files (fixtures/, see fixtures/README.md) walked through
// the header parser and ChecksumTracker.

#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <system_error>
#include <vector>

#include "mes/codec/checksum.h"
#include "mes/codec/event_header.h"
#include "mes/codec/event_types.h"
#include "mes/codec/rotate.h"

namespace mes::codec {
namespace {

using Bytes = std::vector<uint8_t>;

bool ReadFixture(const char* name, Bytes* out) {
  const auto path = std::filesystem::path(__FILE__).parent_path() / "fixtures" / name;
  std::error_code ec;
  if (!std::filesystem::is_regular_file(path, ec)) return false;
  std::ifstream in(path, std::ios::binary);
  if (!in) return false;
  out->assign(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
  return !out->empty();
}

struct FixtureCase {
  const char* file;
  ChecksumAlgorithm algorithm;
  bool mariadb;
  size_t events;
};

constexpr FixtureCase kFixtures[] = {
    {"mysql84_none_tagged.bin", ChecksumAlgorithm::kOff, false, 37},
    {"mysql84_crc32_tagged.bin", ChecksumAlgorithm::kCrc32, false, 37},
    {"mysql97_none_tagged.bin", ChecksumAlgorithm::kOff, false, 37},
    {"mysql97_crc32_tagged.bin", ChecksumAlgorithm::kCrc32, false, 37},
    {"mariadb118_none.bin", ChecksumAlgorithm::kOff, true, 29},
    {"mariadb118_crc32.bin", ChecksumAlgorithm::kCrc32, true, 29},
};

void WalkFixture(const FixtureCase& fixture, bool verify) {
  SCOPED_TRACE(fixture.file);
  Bytes file;
  ASSERT_TRUE(ReadFixture(fixture.file, &file));
  ASSERT_GE(file.size(), kBinlogMagicOffset);
  ASSERT_EQ(std::memcmp(file.data(), kBinlogMagic, sizeof(kBinlogMagic)), 0);

  ChecksumTracker tracker;  // Raw-file consumer: default framing until the FDE.
  tracker.SetVerify(verify);
  size_t pos = kBinlogMagicOffset;
  size_t events = 0;
  size_t tagged_gtids = 0;
  size_t annotate_rows = 0;
  EventHeader last;
  FrameLayout last_layout;
  while (pos < file.size()) {
    SCOPED_TRACE(testing::Message() << "event at " << pos);
    EventHeader header;
    ASSERT_EQ(ParseEventHeader(file.data() + pos, file.size() - pos, &header), Status::kOk);
    ASSERT_GE(header.event_length, kEventHeaderSize);
    ASSERT_LE(header.event_length, file.size() - pos);
    FrameLayout layout;
    ASSERT_EQ(tracker.OnEvent(header, file.data() + pos, header.event_length, &layout), Status::kOk)
        << EventTypeName(header.type_code);

    if (events == 0) {
      ASSERT_EQ(header.type_code, static_cast<uint8_t>(EventType::kFormatDescriptionEvent));
      EXPECT_EQ(tracker.algorithm(), fixture.algorithm);
      EXPECT_TRUE(layout.has_trailer);  // The FDE's own trailer, under either setting.
    } else {
      EXPECT_EQ(layout.has_trailer, fixture.algorithm == ChecksumAlgorithm::kCrc32);
    }
    // On disk next_position is the end of this event; MariaDB writes 0 inside a transaction.
    if (!(fixture.mariadb && header.next_position == 0)) {
      EXPECT_EQ(header.next_position, pos + header.event_length);
    }
    if (header.type_code == static_cast<uint8_t>(EventType::kGtidTaggedLogEvent)) ++tagged_gtids;
    if (header.type_code == static_cast<uint8_t>(EventType::kMariaDBAnnotateRowsEvent)) {
      ++annotate_rows;
    }
    last = header;
    last_layout = layout;
    pos += header.event_length;
    ++events;
  }
  EXPECT_EQ(pos, file.size());
  EXPECT_EQ(events, fixture.events);
  EXPECT_EQ(tracker.algorithm(), fixture.algorithm);

  ASSERT_EQ(last.type_code, static_cast<uint8_t>(EventType::kRotateEvent));
  RotateEvent rotate;
  const size_t rotate_offset = file.size() - last.event_length;
  ASSERT_EQ(ParseRotateEvent(file.data() + rotate_offset + kEventHeaderSize,
                             last_layout.body_length(), &rotate),
            Status::kOk);
  EXPECT_EQ(rotate.position, kBinlogMagicOffset);
  EXPECT_EQ(rotate.next_file.rfind("binlog.", 0), 0u) << rotate.next_file;

  if (fixture.mariadb) {
    EXPECT_EQ(tagged_gtids, 0u);
    EXPECT_GT(annotate_rows, 0u);
  } else {
    EXPECT_EQ(tagged_gtids, 2u);
  }
}

TEST(CodecFixtures, FramingWalksEveryFixture) {
  for (const FixtureCase& fixture : kFixtures) WalkFixture(fixture, true);
}

TEST(CodecFixtures, FramingWithoutVerification) {
  for (const FixtureCase& fixture : kFixtures) WalkFixture(fixture, false);
}

// The FDE algorithm matches the checksum named in the file name.
TEST(CodecFixtures, FdeAlgorithmMatchesFileName) {
  for (const FixtureCase& fixture : kFixtures) {
    SCOPED_TRACE(fixture.file);
    Bytes file;
    ASSERT_TRUE(ReadFixture(fixture.file, &file));
    const uint8_t* fde = file.data() + kBinlogMagicOffset;
    EventHeader header;
    ASSERT_EQ(ParseEventHeader(fde, file.size() - kBinlogMagicOffset, &header), Status::kOk);
    ChecksumAlgorithm declared = ChecksumAlgorithm::kUndefined;
    ASSERT_EQ(ReadFdeChecksumAlgorithm(fde, header.event_length, &declared), Status::kOk);
    const bool named_crc32 = std::strstr(fixture.file, "_crc32") != nullptr;
    EXPECT_EQ(declared, named_crc32 ? ChecksumAlgorithm::kCrc32 : ChecksumAlgorithm::kOff);
    EXPECT_EQ(declared, fixture.algorithm);
  }
}

}  // namespace
}  // namespace mes::codec
