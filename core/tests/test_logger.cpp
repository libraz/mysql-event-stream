// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include "logger.h"

namespace mes {
namespace {

// Shared state for callback tests
struct LogCapture {
  std::atomic<int> call_count{0};
  mes_log_level_t last_level = MES_LOG_ERROR;
  std::string last_message;
  void* last_userdata = nullptr;
};

static LogCapture g_capture;

void TestCallback(mes_log_level_t level, const char* message, void* userdata) {
  g_capture.call_count.fetch_add(1, std::memory_order_relaxed);
  g_capture.last_level = level;
  g_capture.last_message = message;
  g_capture.last_userdata = userdata;
}

class LoggerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    g_capture.call_count.store(0);
    g_capture.last_level = MES_LOG_ERROR;
    g_capture.last_message.clear();
    g_capture.last_userdata = nullptr;
    LogConfig::SetCallback(nullptr, MES_LOG_ERROR, nullptr);
  }

  void TearDown() override { LogConfig::SetCallback(nullptr, MES_LOG_ERROR, nullptr); }
};

TEST_F(LoggerTest, NoCallbackDoesNotCrash) {
  StructuredLog().Event("test").Field("key", "value").Error();
  EXPECT_EQ(g_capture.call_count.load(), 0);
}

TEST_F(LoggerTest, CallbackReceivesMessage) {
  LogConfig::SetCallback(TestCallback, MES_LOG_DEBUG, nullptr);
  StructuredLog().Event("test_event").Field("key", "val").Error();
  EXPECT_EQ(g_capture.call_count.load(), 1);
  EXPECT_EQ(g_capture.last_level, MES_LOG_ERROR);
  EXPECT_NE(g_capture.last_message.find("event=test_event"), std::string::npos);
  EXPECT_NE(g_capture.last_message.find("key=val"), std::string::npos);
}

TEST_F(LoggerTest, LogLevelFilters) {
  LogConfig::SetCallback(TestCallback, MES_LOG_WARN, nullptr);
  // INFO should be filtered (INFO > WARN in verbosity)
  StructuredLog().Event("info_msg").Info();
  EXPECT_EQ(g_capture.call_count.load(), 0);

  // WARN should pass
  StructuredLog().Event("warn_msg").Warn();
  EXPECT_EQ(g_capture.call_count.load(), 1);

  // ERROR should pass
  StructuredLog().Event("err_msg").Error();
  EXPECT_EQ(g_capture.call_count.load(), 2);
}

TEST_F(LoggerTest, UserdataPassedThrough) {
  int userdata_value = 42;
  LogConfig::SetCallback(TestCallback, MES_LOG_DEBUG, &userdata_value);
  StructuredLog().Event("test").Error();
  EXPECT_EQ(g_capture.last_userdata, &userdata_value);
}

TEST_F(LoggerTest, SetCallbackUsesReleaseAcquireOrdering) {
  // Verify that stores done before SetCallback are visible after
  // GetCallback returns non-null. The release store in SetCallback
  // should synchronize with the acquire load in GetCallback, making
  // shared_data and log_level visible to the reader thread.
  //
  // Deterministic: write on main thread, then read on a separate thread.
  int shared_data = 0;

  shared_data = 12345;
  LogConfig::SetCallback(TestCallback, MES_LOG_DEBUG, nullptr);

  std::atomic<bool> reader_passed{false};
  std::thread reader([&] {
    auto* cb = LogConfig::GetCallback();
    ASSERT_NE(cb, nullptr);
    EXPECT_EQ(shared_data, 12345);
    EXPECT_EQ(LogConfig::GetLogLevel(), MES_LOG_DEBUG);
    reader_passed.store(true, std::memory_order_relaxed);
  });

  reader.join();
  EXPECT_TRUE(reader_passed.load());
}

// Stands in for a consumer that keeps an engine in a global and lets it log
// while the process tears down. Constructed during dynamic initialization, so
// it outlives the logging state that the first log call initializes and its
// destructor runs after that state would ordinarily have been destroyed.
struct StaticTeardownEmitter {
  ~StaticTeardownEmitter() {
    StructuredLog().Event("static_teardown").Field("stage", "destructor").Warn();
  }
};

StaticTeardownEmitter g_static_teardown_emitter;

void WriteMessageToStderr(mes_log_level_t, const char* message, void*) {
  std::fputs(message, stderr);
  std::fputc('\n', stderr);
  std::fflush(stderr);
}

TEST_F(LoggerTest, LoggingStaysUsableDuringStaticDestruction) {
  // The child installs a callback — initializing the logging state after the
  // emitter above was constructed — and then exits, which runs the emitter's
  // destructor during static teardown. The message reaching stderr shows the
  // snapshot and its mutex were both still alive at that point, and the exit
  // status shows teardown completed.
  EXPECT_EXIT(
      {
        LogConfig::SetCallback(WriteMessageToStderr, MES_LOG_WARN, nullptr);
        std::exit(0);
      },
      ::testing::ExitedWithCode(0), "event=static_teardown stage=destructor");
}

TEST_F(LoggerTest, ClearCallbackStopsLogging) {
  LogConfig::SetCallback(TestCallback, MES_LOG_DEBUG, nullptr);
  StructuredLog().Event("test1").Error();
  EXPECT_EQ(g_capture.call_count.load(), 1);

  LogConfig::SetCallback(nullptr, MES_LOG_ERROR, nullptr);
  StructuredLog().Event("test2").Error();
  EXPECT_EQ(g_capture.call_count.load(), 1);
}

}  // namespace
}  // namespace mes
