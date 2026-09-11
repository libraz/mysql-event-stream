// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @file source_scan.h
 * @brief Read the tree's own sources from a test, to assert a shape is unique
 *
 * Some invariants are about the absence of a second copy: two constants holding
 * the same value change no behaviour until one of them is edited, so no test
 * that drives the code can observe the duplicate. Scanning the sources is what
 * proves the absence.
 *
 * A scan that finds nothing passes for the same reason a scan that finds
 * everything does, so every caller owes the same guards: assert each scanned
 * root yielded files, treat an unreadable file as a failure rather than a zero
 * count (which is why the counters return -1 instead of 0), and assert the file
 * expected to define the shape does contain it. Assemble search patterns from
 * fragments so the scanning test does not become a hit in its own search.
 */

#ifndef MES_TESTS_SOURCE_SCAN_H_
#define MES_TESTS_SOURCE_SCAN_H_

#include <filesystem>
#include <fstream>
#include <string>
#include <system_error>
#include <vector>

// Deliberately not named `testing`: these helpers are used from tests that sit
// inside `namespace mes`, where a nested `mes::testing` would shadow gtest's
// own `::testing` for every unqualified reference in the file.
namespace mes::source_scan {

/** @brief The repository root, located relative to this header's own source. */
inline std::filesystem::path RepoRoot() {
  return std::filesystem::path(__FILE__).parent_path().parent_path().parent_path();
}

/** @brief Every regular file under @p root, or an empty list if it is unreadable. */
inline std::vector<std::filesystem::path> FilesUnder(const std::filesystem::path& root) {
  std::vector<std::filesystem::path> files;
  std::error_code ec;
  // The error_code overloads are used throughout: these translation units are
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
inline int CountLinesContaining(const std::filesystem::path& file, const std::string& needle) {
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

/** @brief Number of lines in @p file containing both needles; -1 if unreadable. */
inline int CountLinesContainingBoth(const std::filesystem::path& file, const std::string& first,
                                    const std::string& second) {
  std::ifstream in(file);
  if (!in.is_open()) {
    return -1;
  }
  int hits = 0;
  std::string line;
  while (std::getline(in, line)) {
    if (line.find(first) != std::string::npos && line.find(second) != std::string::npos) {
      ++hits;
    }
  }
  return hits;
}

/**
 * @brief Whole file with every run of whitespace collapsed to one space.
 *
 * Lets a scan assert an expression that the formatter is free to wrap across
 * lines, which a line-oriented count cannot see. Empty if @p file is unreadable,
 * which every caller must distinguish from a file that simply lacks the shape.
 */
inline std::string ReadCollapsed(const std::filesystem::path& file) {
  std::ifstream in(file);
  if (!in.is_open()) {
    return std::string();
  }
  std::string text;
  std::string line;
  const auto append_space = [&text] {
    if (!text.empty() && text.back() != ' ') {
      text.push_back(' ');
    }
  };
  while (std::getline(in, line)) {
    for (char c : line) {
      if (c == ' ' || c == '\t' || c == '\r') {
        append_space();
        continue;
      }
      text.push_back(c);
    }
    append_space();
  }
  return text;
}

/** @brief Number of non-overlapping occurrences of @p needle in @p text. */
inline int CountOccurrences(const std::string& text, const std::string& needle) {
  if (needle.empty()) {
    return 0;
  }
  int hits = 0;
  for (size_t at = text.find(needle); at != std::string::npos;
       at = text.find(needle, at + needle.size())) {
    ++hits;
  }
  return hits;
}

}  // namespace mes::source_scan

#endif  // MES_TESTS_SOURCE_SCAN_H_
