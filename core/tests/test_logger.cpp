// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
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

/// @brief A second registrable callback, so two configurations differ in all
///        three fields and a mix of them is recognizable.
void OtherTestCallback(mes_log_level_t, const char*, void*) {}

/// @brief Whether an observed configuration is one that was actually published.
bool SameConfiguration(const LogConfigSnapshot& observed, const LogConfigSnapshot& published) {
  return observed.callback == published.callback && observed.level == published.level &&
         observed.userdata == published.userdata;
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

TEST_F(LoggerTest, ConcurrentReaderNeverObservesAMixedConfiguration) {
  // The configuration is published as one immutable snapshot so that a reader
  // can never pair one generation's callback with another generation's level or
  // userdata. Showing that requires the reader to be polling while the writer
  // republishes: a reader that only looks after being joined is satisfied by the
  // happens-before edge of thread creation alone, whatever the publication
  // mechanism, and so would accept a torn read.
  static int first_userdata = 0;
  static int second_userdata = 0;
  const LogConfigSnapshot first{TestCallback, MES_LOG_DEBUG, &first_userdata};
  const LogConfigSnapshot second{OtherTestCallback, MES_LOG_WARN, &second_userdata};

  std::atomic<bool> reader_has_polled{false};
  std::atomic<bool> stop{false};
  std::atomic<int64_t> unset_observations{0};
  std::atomic<int64_t> published_observations{0};
  std::atomic<int64_t> mixed_observations{0};

  std::thread reader([&] {
    while (!stop.load(std::memory_order_relaxed)) {
      const std::shared_ptr<const LogConfigSnapshot> observed = LogConfig::GetSnapshot();
      if (observed->callback == nullptr && observed->userdata == nullptr) {
        unset_observations.fetch_add(1, std::memory_order_relaxed);
      } else if (SameConfiguration(*observed, first) || SameConfiguration(*observed, second)) {
        published_observations.fetch_add(1, std::memory_order_relaxed);
      } else {
        mixed_observations.fetch_add(1, std::memory_order_relaxed);
      }
      reader_has_polled.store(true, std::memory_order_relaxed);
    }
  });

  // Publishing only once the reader has classified something guarantees it was
  // already running for the unset-to-published transition.
  while (!reader_has_polled.load(std::memory_order_relaxed)) {
  }

  constexpr int kGenerations = 4000;
  for (int generation = 0; generation < kGenerations; generation++) {
    const LogConfigSnapshot& next = (generation % 2 == 0) ? first : second;
    LogConfig::SetCallback(next.callback, next.level, next.userdata);
  }

  // The last configuration stays published, so waiting for one published
  // observation terminates regardless of how the reader was scheduled.
  while (published_observations.load(std::memory_order_relaxed) == 0) {
  }
  stop.store(true, std::memory_order_relaxed);
  reader.join();

  EXPECT_EQ(mixed_observations.load(), 0)
      << "a reader observed a callback paired with a level or userdata from another generation";
  EXPECT_GT(unset_observations.load(), 0)
      << "the reader saw nothing before the first publication, so no transition was raced";
  EXPECT_GT(published_observations.load(), 0) << "the reader never observed a published callback";
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
