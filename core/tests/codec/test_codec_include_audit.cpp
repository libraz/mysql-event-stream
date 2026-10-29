/**
 * @file test_codec_include_audit.cpp
 * @brief Keeps core/codec self-contained: quoted includes stay inside the codec
 *        tree and public headers declare no virtual members.
 */

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <string>
#include <system_error>

#include "source_scan.h"

namespace {

namespace fs = std::filesystem;
using mes::source_scan::CountLinesContaining;
using mes::source_scan::FilesUnder;

// source_scan::RepoRoot() derives from that header's __FILE__, which reads
// "codec/../source_scan.h" from this directory; anchor on this file instead.
fs::path RepoRoot() {
  return fs::path(__FILE__)
      .parent_path()
      .parent_path()
      .parent_path()
      .parent_path()
      .lexically_normal();
}

bool IsSourceFile(const fs::path& path) {
  const std::string ext = path.extension().string();
  return ext == ".h" || ext == ".cpp";
}

// Returns the target of a `#include "..."` line, or empty if the line is not one.
std::string QuotedInclude(const std::string& line) {
  size_t i = line.find_first_not_of(" \t");
  if (i == std::string::npos || line[i] != '#') {
    return {};
  }
  i = line.find_first_not_of(" \t", i + 1);
  if (i == std::string::npos || line.compare(i, 7, "include") != 0) {
    return {};
  }
  i = line.find_first_not_of(" \t", i + 7);
  if (i == std::string::npos || line[i] != '"') {
    return {};
  }
  const size_t end = line.find('"', i + 1);
  if (end == std::string::npos) {
    return {};
  }
  return line.substr(i + 1, end - i - 1);
}

bool IsFile(const fs::path& path) {
  std::error_code ec;
  return fs::is_regular_file(path, ec);
}

TEST(CodecIncludeAudit, QuotedIncludesResolveInsideCodecTree) {
  const fs::path codec = RepoRoot() / "core" / "codec";
  const fs::path include_root = codec / "include";
  const fs::path src_root = codec / "src";
  const fs::path core_include = RepoRoot() / "core" / "include";
  const fs::path core_src = RepoRoot() / "core" / "src";

  const auto files = FilesUnder(codec);
  ASSERT_FALSE(files.empty()) << "no files under " << codec;

  int scanned = 0;
  for (const fs::path& file : files) {
    if (!IsSourceFile(file)) {
      continue;
    }
    std::ifstream in(file);
    ASSERT_TRUE(in.is_open()) << file;
    ++scanned;
    std::string line;
    while (std::getline(in, line)) {
      const std::string target = QuotedInclude(line);
      if (target.empty()) {
        continue;
      }
      const bool in_tree = IsFile(file.parent_path() / target) || IsFile(include_root / target) ||
                           IsFile(src_root / target);
      EXPECT_TRUE(in_tree) << file << ": \"" << target << "\" does not resolve under core/codec";
      // A name that also exists in the host tree is only acceptable if it
      // resolved inside the codec tree first.
      if (!in_tree) {
        EXPECT_FALSE(IsFile(core_include / target) || IsFile(core_src / target))
            << file << ": \"" << target << "\" resolves into the host tree";
      }
    }
  }
  EXPECT_GT(scanned, 0);
}

TEST(CodecIncludeAudit, PublicHeadersDeclareNoVirtual) {
  const fs::path include_root = RepoRoot() / "core" / "codec" / "include";
  const auto files = FilesUnder(include_root);
  ASSERT_FALSE(files.empty()) << "no files under " << include_root;

  const std::string token = std::string("vir") + "tual";
  for (const fs::path& file : files) {
    if (file.extension() != ".h") {
      continue;
    }
    EXPECT_EQ(CountLinesContaining(file, token), 0) << file;
  }
}

}  // namespace
