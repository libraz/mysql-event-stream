// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>

#include "protocol/mysql_query.h"

namespace mes::protocol {
namespace {

TEST(QueryResultTest, ConstructionAndAccess) {
  QueryResult result;
  result.column_names = {"id", "name"};

  QueryResultRow row;
  row.values = {"1", "alice"};
  row.is_null = {false, false};
  result.rows.push_back(row);

  EXPECT_EQ(result.column_names.size(), 2u);
  EXPECT_EQ(result.rows.size(), 1u);
  EXPECT_EQ(result.rows[0].values[0], "1");
  EXPECT_EQ(result.rows[0].values[1], "alice");
  EXPECT_FALSE(result.rows[0].is_null[0]);
}

TEST(QueryResultTest, NullValues) {
  QueryResultRow row;
  row.values = {"", ""};
  row.is_null = {true, false};

  EXPECT_TRUE(row.is_null[0]);
  EXPECT_FALSE(row.is_null[1]);
}

TEST(QueryResultTest, EmptyResult) {
  QueryResult result;
  EXPECT_TRUE(result.column_names.empty());
  EXPECT_TRUE(result.rows.empty());
}

}  // namespace
}  // namespace mes::protocol
