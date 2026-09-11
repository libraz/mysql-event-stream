// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>

#include <filesystem>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "client/connection_validator.h"
#include "source_scan.h"

namespace mes::detail {
namespace {

struct FakeVariables {
  std::map<std::string, std::string> values = {
      {"log_bin", "ON"},
      {"gtid_mode", "ON"},
      {"binlog_format", "ROW"},
      {"binlog_row_image", "FULL"},
      {"binlog_transaction_compression", "OFF"},
      {"binlog_row_value_options", ""},
      {"log_bin_compress", "OFF"},
  };

  static VariableValue Lookup(void* context, const char* variable_name) {
    const auto& values = static_cast<FakeVariables*>(context)->values;
    const auto it = values.find(variable_name);
    if (it == values.end()) return {.status = VariableQueryStatus::kNotFound};
    return {.status = VariableQueryStatus::kFound, .value = it->second};
  }
};

TEST(ConnectionValidatorTest, RejectsEveryRequiredMySqlSetting) {
  const std::vector<std::pair<std::string, std::string>> rejected = {
      {"log_bin", "OFF"},
      {"gtid_mode", "OFF"},
      {"binlog_format", "STATEMENT"},
      {"binlog_row_image", "MINIMAL"},
      {"binlog_transaction_compression", "ON"},
      {"binlog_row_value_options", "PARTIAL_JSON"},
  };
  for (const auto& [name, value] : rejected) {
    FakeVariables variables;
    variables.values[name] = value;
    const ValidationResult result =
        ValidateServerConfiguration(FakeVariables::Lookup, &variables, ServerFlavor::kMySQL);
    EXPECT_EQ(result.error, MES_ERR_VALIDATION) << name;
    EXPECT_NE(std::string(result.message).find(name), std::string::npos);
  }
}

TEST(ConnectionValidatorTest, AppliesMariaDbFlavorGates) {
  FakeVariables variables;
  variables.values["gtid_mode"] = "OFF";
  variables.values.erase("binlog_transaction_compression");
  variables.values.erase("binlog_row_value_options");
  EXPECT_EQ(
      ValidateServerConfiguration(FakeVariables::Lookup, &variables, ServerFlavor::kMariaDB).error,
      MES_OK);

  variables.values["log_bin_compress"] = "ON";
  const ValidationResult result =
      ValidateServerConfiguration(FakeVariables::Lookup, &variables, ServerFlavor::kMariaDB);
  EXPECT_EQ(result.error, MES_ERR_VALIDATION);
  EXPECT_NE(std::string(result.message).find("log_bin_compress"), std::string::npos);
}

TEST(ConnectionValidatorTest, UnknownOptionalVariableIsDistinctFromQueryFailure) {
  protocol::QueryResult empty;
  EXPECT_EQ(ClassifyVariableQueryResult(MES_OK, empty), VariableQueryStatus::kNotFound);

  protocol::QueryResult ignored;
  EXPECT_EQ(ClassifyVariableQueryResult(MES_ERR_VALIDATION, ignored),
            VariableQueryStatus::kQueryError);
  EXPECT_EQ(ClassifyVariableQueryResult(MES_ERR_DISCONNECTED, ignored),
            VariableQueryStatus::kQueryError);
}

TEST(ConnectionValidatorTest, ValidVariableIsFound) {
  protocol::QueryResult result;
  protocol::QueryResultRow row;
  row.values = {"binlog_transaction_compression", "OFF"};
  row.is_null = {false, false};
  result.rows.push_back(std::move(row));

  EXPECT_EQ(ClassifyVariableQueryResult(MES_OK, result), VariableQueryStatus::kFound);
}

// --- The documented check list is the implemented one ---

using source_scan::CountLinesContaining;
using source_scan::CountLinesContainingBoth;
using source_scan::RepoRoot;

/** @brief A lookup that records what it was asked for, then answers as valid. */
struct RecordingVariables {
  FakeVariables passing;
  std::vector<std::string> queried;

  static VariableValue Lookup(void* context, const char* variable_name) {
    auto* self = static_cast<RecordingVariables*>(context);
    self->queried.emplace_back(variable_name);
    return FakeVariables::Lookup(&self->passing, variable_name);
  }
};

/**
 * @brief Position @p variable holds in the header's numbered check list, or 0.
 *
 * Matched on the name followed by its verb so that the entry for a setting
 * whose name is a prefix of another's cannot be claimed by the longer one.
 */
inline int DocumentedItemNumber(const std::filesystem::path& header, const std::string& variable) {
  for (int number = 1; number <= 9; ++number) {
    const std::string prefix = " * " + std::to_string(number) + ". ";
    if (CountLinesContainingBoth(header, prefix, variable + " must") == 1) return number;
  }
  return 0;
}

/**
 * @brief The validator queries exactly the settings its header lists, in order.
 *
 * The class-level list is what a reader consults to learn which settings gate
 * Connect(), and nothing about the implementation forces the two to agree: a
 * check added to ValidateServerConfiguration takes effect whether or not the
 * list gains an entry, which is how the list came to describe five checks while
 * seven ran. Recording the lookups the validator performs and reading the
 * header back closes both directions -- a check that runs unlisted, and a
 * listed check that no longer runs on either flavor.
 */
TEST(ConnectionValidatorTest, QueriesExactlyTheSettingsItsHeaderLists) {
  const std::filesystem::path header =
      RepoRoot() / "core" / "src" / "client" / "connection_validator.h";

  // A scan of an unreadable file passes every count below, so establish that
  // this one was read and does carry the list before trusting any of them.
  ASSERT_EQ(CountLinesContaining(header, "log_bin must be ON"), 1);

  int listed = 0;
  for (int number = 1; number <= 9; ++number) {
    const int hits = CountLinesContaining(header, " * " + std::to_string(number) + ". ");
    ASSERT_GE(hits, 0);
    listed += hits;
  }
  ASSERT_GT(listed, 0);

  // ValidationResult carries an error and a message and nothing else, so no
  // doc comment on it may promise a field an embedder could try to read.
  EXPECT_EQ(CountLinesContaining(header, "uuid"), 0);

  std::set<int> claimed;
  for (const ServerFlavor flavor : {ServerFlavor::kMySQL, ServerFlavor::kMariaDB}) {
    RecordingVariables recording;
    // Every value passes, so no check short-circuits the ones after it.
    ASSERT_EQ(ValidateServerConfiguration(RecordingVariables::Lookup, &recording, flavor).error,
              MES_OK);
    ASSERT_FALSE(recording.queried.empty());

    int previous = 0;
    for (const std::string& variable : recording.queried) {
      const int number = DocumentedItemNumber(header, variable);
      EXPECT_GT(number, 0) << variable << " is queried but the header lists no check for it";
      // The header presents the list in the order the checks run, so the
      // numbering must ascend in the order the lookups actually arrive.
      EXPECT_GT(number, previous) << variable;
      previous = number;
      claimed.insert(number);
    }
  }

  // The two flavors together have to account for every listed item: a listed
  // check that runs on neither leaves its number unclaimed.
  EXPECT_EQ(static_cast<int>(claimed.size()), listed);
}

TEST(ConnectionValidatorTest, MalformedVariableRowIsNotTreatedAsAbsent) {
  protocol::QueryResult result;
  protocol::QueryResultRow row;
  row.values = {"binlog_transaction_compression"};
  row.is_null = {false};
  result.rows.push_back(std::move(row));
  EXPECT_EQ(ClassifyVariableQueryResult(MES_OK, result), VariableQueryStatus::kMalformed);

  result.rows[0].values.push_back("");
  result.rows[0].is_null.push_back(true);
  EXPECT_EQ(ClassifyVariableQueryResult(MES_OK, result), VariableQueryStatus::kMalformed);
}

}  // namespace
}  // namespace mes::detail
