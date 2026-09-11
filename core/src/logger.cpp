// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include "logger.h"

#include <mutex>
#include <new>

namespace mes {

namespace {

/// The snapshot and the mutex serializing updates to it.
///
/// Reads take a local shared_ptr copy under the same lock, which is cheap (an
/// atomic refcount bump) and ensures the observed (callback, level, userdata)
/// triple is always internally consistent. std::atomic<std::shared_ptr> is
/// C++20-only; this lock is the portable C++17 equivalent.
struct LogState {
  std::mutex mutex;
  std::shared_ptr<const LogConfigSnapshot> snapshot = std::make_shared<const LogConfigSnapshot>();
};

/// @brief The process-wide logging state, constructed on first use and never
///        destroyed.
///
/// A consumer may own an engine with static storage duration, and an engine
/// emits log messages from its own destructor. Ordinary function-local statics
/// here are initialized on the first log call, hence after such an engine, and
/// are therefore destroyed before it: locking the mutex or reading the snapshot
/// during static teardown would then touch objects whose lifetime has already
/// ended. Constructing into static storage that is never reclaimed keeps both
/// valid for the whole process lifetime, and unlike a leaked heap allocation it
/// gives the leak sanitizers nothing to report.
LogState& State() {
  alignas(LogState) static unsigned char storage[sizeof(LogState)];
  static LogState* state = new (storage) LogState();
  return *state;
}

}  // namespace

void LogConfig::SetCallback(mes_log_callback_t callback, mes_log_level_t log_level,
                            void* userdata) {
  auto next = std::make_shared<LogConfigSnapshot>();
  next->callback = callback;
  next->level = log_level;
  next->userdata = userdata;
  LogState& state = State();
  std::lock_guard<std::mutex> lock(state.mutex);
  state.snapshot = std::move(next);
}

std::shared_ptr<const LogConfigSnapshot> LogConfig::GetSnapshot() {
  LogState& state = State();
  std::lock_guard<std::mutex> lock(state.mutex);
  return state.snapshot;
}

mes_log_callback_t LogConfig::GetCallback() { return GetSnapshot()->callback; }
mes_log_level_t LogConfig::GetLogLevel() { return GetSnapshot()->level; }
void* LogConfig::GetUserdata() { return GetSnapshot()->userdata; }

void StructuredLog::Emit(mes_log_level_t level) {
  // Reuse the snapshot captured in the StructuredLog constructor so that
  // every Field()/Emit() within this builder observes the same
  // configuration generation. This avoids an extra SnapshotMutex acquisition
  // per log line.
  if (!snap_ || snap_->callback == nullptr || level > snap_->level) {
    return;
  }

  // Build key=value format (matching mygram-db TEXT format)
  std::string message;
  if (!event_.empty()) {
    message += "event=";
    message += event_;
  }
  for (const auto& [key, value] : fields_) {
    if (!message.empty()) message += ' ';
    message += key;
    message += '=';
    message += value;
  }

  snap_->callback(level, message.c_str(), snap_->userdata);
}

}  // namespace mes
