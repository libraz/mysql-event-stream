// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <set>
#include <string>
#include <system_error>
#include <vector>

#include "secure_cleanse.h"

namespace {

const char kSecret[] = "correct horse battery staple";

/**
 * @brief Wipes @p buf through the scope guard, optionally returning early.
 *
 * The buffer is owned by the caller, so its bytes are still live storage after
 * the guard's scope ends and reading them back is well defined.
 */
void GuardedScope(uint8_t* buf, size_t len, bool return_early) {
  mes::SecureCleanse guard{buf, len};
  if (return_early) {
    return;
  }
  // A path that reaches the end of the scope normally must wipe just the same.
  buf[0] = 0xFF;
}

/** @brief The repository root, located relative to this test's own source. */
std::filesystem::path RepoRoot() {
  return std::filesystem::path(__FILE__).parent_path().parent_path().parent_path();
}

/** @brief Every regular file under @p root, or an empty list if it is unreadable. */
std::vector<std::filesystem::path> FilesUnder(const std::filesystem::path& root) {
  std::vector<std::filesystem::path> files;
  std::error_code ec;
  // The error_code overloads are used throughout: this translation unit is
  // built without exceptions, so a throwing filesystem call would abort.
  for (std::filesystem::recursive_directory_iterator it(root, ec), end; !ec && it != end;
       it.increment(ec)) {
    if (it->is_regular_file(ec)) {
      files.push_back(it->path());
    }
  }
  return files;
}

/** @brief Number of lines in @p file containing @p needle; -1 if unreadable. */
int CountLinesContaining(const std::filesystem::path& file, const std::string& needle) {
  std::ifstream in(file);
  if (!in.is_open()) {
    return -1;
  }
  int hits = 0;
  std::string line;
  while (std::getline(in, line)) {
    if (line.find(needle) != std::string::npos) {
      ++hits;
    }
  }
  return hits;
}

}  // namespace

TEST(SecureWipeTest, ZeroesALiveStringInPlace) {
  std::string secret = kSecret;
  const size_t size = secret.size();
  const size_t capacity = secret.capacity();
  ASSERT_NE(secret[0], '\0');

  mes::SecureWipe(secret);

  EXPECT_EQ(secret.size(), size) << "the wipe must not resize the string";
  EXPECT_EQ(secret.capacity(), capacity) << "the wipe must not release the storage it scrubbed";
  for (size_t i = 0; i < size; ++i) {
    EXPECT_EQ(secret[i], '\0') << "byte " << i << " survived the wipe";
  }
}

TEST(SecureWipeTest, ZeroesALiveBuffer) {
  uint8_t buf[] = {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08};

  mes::SecureWipe(buf, sizeof(buf));

  for (size_t i = 0; i < sizeof(buf); ++i) {
    EXPECT_EQ(buf[i], 0) << "byte " << i << " survived the wipe";
  }
}

TEST(SecureWipeTest, LeavesNothingToDoForEmptyTargets) {
  // A credential that was never populated reaches the wipe on the same paths as
  // one that was, so an empty or absent target is accepted rather than guarded
  // against at every call site.
  uint8_t buf[] = {0x11, 0x22};
  mes::SecureWipe(buf, 0);
  EXPECT_EQ(buf[0], 0x11);
  EXPECT_EQ(buf[1], 0x22);

  mes::SecureWipe(nullptr, sizeof(buf));

  std::string empty;
  mes::SecureWipe(empty);
  EXPECT_TRUE(empty.empty());
}

TEST(SecureCleanseTest, WipesABufferOnEveryExitPathOfTheGuardedScope) {
  const bool return_early[] = {true, false};
  for (bool early : return_early) {
    SCOPED_TRACE(early ? "early return" : "fallthrough");
    uint8_t buf[] = {0x01, 0x02, 0x03, 0x04};
    ASSERT_NE(buf[0], 0);

    GuardedScope(buf, sizeof(buf), early);

    for (size_t i = 0; i < sizeof(buf); ++i) {
      EXPECT_EQ(buf[i], 0) << "byte " << i << " survived the guard";
    }
  }
}

TEST(SecureCleanseTest, WipesStringBytesAssignedAfterTheGuardWasDeclared) {
  // The C ABI entry points declare the guard against a config field and fill
  // that field afterwards, so the guard is constructed over an empty string. A
  // guard that captured data() and size() at construction would latch that
  // empty string and wipe nothing, while still being visibly present at the
  // site, so this ordering is the one the guard has to survive.
  std::string password;
  ASSERT_TRUE(password.empty()) << "the guard has to be constructed over an empty string";
  {
    mes::SecureCleanse guard{password};
    password = kSecret;
    ASSERT_NE(password[0], '\0');
  }

  ASSERT_EQ(password.size(), sizeof(kSecret) - 1);
  for (size_t i = 0; i < password.size(); ++i) {
    EXPECT_EQ(password[i], '\0') << "byte " << i << " survived the guard";
  }
}

TEST(SecureCleanseTest, WipesTheStringBytesPresentWhenTheScopeEnds) {
  std::string password = kSecret;
  {
    mes::SecureCleanse guard{password};
    // Growing past the small-string threshold reallocates, which is exactly the
    // case a captured pointer would get wrong.
    password.append(64, 'x');
  }

  for (size_t i = 0; i < password.size(); ++i) {
    EXPECT_EQ(password[i], '\0') << "byte " << i << " survived the guard";
  }
}

/**
 * @brief The non-elidable wipe primitive is named in one file only.
 *
 * A translation unit that names the OpenSSL primitive itself, or declares its
 * own cleanse guard, is how several same-purpose copies of this helper came to
 * exist side by side. Scanning the sources is what keeps that from recurring
 * silently: it proves the absence of duplicates, which no unit test can.
 *
 * Every pattern below is assembled from fragments rather than written out, so
 * that this file does not become a hit in the very searches it exists to keep
 * empty.
 */
TEST(SecureCleanseTest, WipePrimitiveIsNamedInTheSharedHeaderAlone) {
  const std::string primitive = std::string("OPENSSL_") + "cleanse";
  const std::string guard = std::string("struct ") + "SecureCleanse";
  const std::filesystem::path root = RepoRoot();
  const std::filesystem::path shared_header = root / "core" / "src" / "secure_cleanse.h";
  const std::filesystem::path scanned_roots[] = {
      root / "core" / "src",
      root / "core" / "include",
      root / "bindings" / "node" / "src",
  };

  std::vector<std::filesystem::path> files;
  for (const std::filesystem::path& scanned : scanned_roots) {
    const std::vector<std::filesystem::path> found = FilesUnder(scanned);
    ASSERT_FALSE(found.empty()) << "no files found under " << scanned;
    files.insert(files.end(), found.begin(), found.end());
  }

  std::set<std::string> primitive_files;
  std::set<std::string> guard_files;
  int guard_definitions = 0;
  for (const std::filesystem::path& file : files) {
    const int primitive_hits = CountLinesContaining(file, primitive);
    const int guard_hits = CountLinesContaining(file, guard);
    ASSERT_GE(primitive_hits, 0) << "cannot read " << file;
    if (primitive_hits > 0) {
      primitive_files.insert(file.string());
    }
    if (guard_hits > 0) {
      guard_files.insert(file.string());
      guard_definitions += guard_hits;
    }
  }

  // Reading the shared header is what proves the scan is looking at the sources
  // it is meant to: without this the assertions below would pass on an empty or
  // mislocated file list.
  ASSERT_GT(CountLinesContaining(shared_header, primitive), 0)
      << "the shared helper was not found at " << shared_header;

  const std::set<std::string> expected = {shared_header.string()};
  EXPECT_EQ(primitive_files, expected);
  EXPECT_EQ(guard_files, expected);
  EXPECT_EQ(guard_definitions, 1);

  // A wipe spelled with one of these primitives would be the same duplication
  // under a different name. None of them has a use in this tree.
  const std::string alternatives[] = {
      std::string("explicit_") + "bzero",
      std::string("Secure") + "ZeroMemory",
      std::string("memset") + "_s",
      std::string("bzero") + "(",
  };
  for (const std::filesystem::path& file : files) {
    for (const std::string& alternative : alternatives) {
      EXPECT_EQ(CountLinesContaining(file, alternative), 0)
          << file << " wipes with " << alternative << " instead of the shared helper";
    }
  }
}
