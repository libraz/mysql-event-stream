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

/// A value is quoted when a reader splitting on spaces and '=' could not recover it unchanged.
bool NeedsQuoting(const std::string& value) {
  for (unsigned char ch : value) {
    if (ch <= 0x20 || ch == 0x7F || ch == '=' || ch == '"' || ch == '\\') return true;
  }
  return false;
}

/// Appends @p value as key=value text: verbatim when plain, otherwise quoted
/// with backslash escapes so it stays on one line and cannot introduce fields.
void AppendValue(std::string* out, const std::string& value) {
  if (!NeedsQuoting(value)) {
    *out += value;
    return;
  }
  static constexpr char kHex[] = "0123456789abcdef";
  *out += '"';
  for (unsigned char ch : value) {
    if (ch == '"' || ch == '\\') {
      *out += '\\';
      *out += static_cast<char>(ch);
    } else if (ch == '\n') {
      *out += "\\n";
    } else if (ch == '\r') {
      *out += "\\r";
    } else if (ch == '\t') {
      *out += "\\t";
    } else if (ch < 0x20 || ch == 0x7F) {
      *out += "\\x";
      *out += kHex[ch >> 4];
      *out += kHex[ch & 0x0F];
    } else {
      *out += static_cast<char>(ch);
    }
  }
  *out += '"';
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
    AppendValue(&message, value);
  }

  snap_->callback(level, message.c_str(), snap_->userdata);
}

}  // namespace mes
