// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>

#include <map>
#include <string>

#include "client/connection_validator.h"

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
