// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>

#include "client/connection_validator.h"

namespace mes::detail {
namespace {

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
