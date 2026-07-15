// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include "client/event_queue.h"

#include <limits>
#include <utility>

namespace mes {

EventQueue::EventQueue(size_t max_size, size_t max_bytes)
    : max_size_(max_size == 0 ? 10000 : max_size),
      max_bytes_(max_bytes == 0 ? kDefaultEventQueueBytes : max_bytes) {}

bool EventQueue::Push(QueuedEvent event) {
  return PushWithStatus(std::move(event)) == PushResult::kPushed;
}

EventQueue::PushResult EventQueue::PushWithStatus(QueuedEvent event) {
  const size_t event_bytes = EventMemoryBytes(event);
  if (event_bytes > max_bytes_) return PushResult::kEventTooLarge;

  std::unique_lock<std::mutex> lock(mu_);
  not_full_cv_.wait(lock, [this, event_bytes] {
    return closed_ || (queue_.size() < max_size_ && event_bytes <= max_bytes_ - queued_bytes_);
  });
  if (closed_) return PushResult::kClosed;
  queued_bytes_ += event_bytes;
  queue_.push(std::move(event));
  not_empty_cv_.notify_one();
  return PushResult::kPushed;
}

bool EventQueue::Pop(QueuedEvent* event) {
  std::unique_lock<std::mutex> lock(mu_);
  not_empty_cv_.wait(lock, [this] { return closed_ || !queue_.empty(); });
  if (queue_.empty()) return false;  // closed and drained
  const size_t event_bytes = EventMemoryBytes(queue_.front());
  *event = std::move(queue_.front());
  queue_.pop();
  queued_bytes_ -= event_bytes;
  not_full_cv_.notify_one();
  return true;
}

void EventQueue::Close() {
  std::lock_guard<std::mutex> lock(mu_);
  closed_ = true;
  not_empty_cv_.notify_all();
  not_full_cv_.notify_all();
}

void EventQueue::Clear() {
  std::lock_guard<std::mutex> lock(mu_);
  std::queue<QueuedEvent> empty;
  queue_.swap(empty);
  queued_bytes_ = 0;
  not_full_cv_.notify_all();
}

size_t EventQueue::Size() const {
  std::lock_guard<std::mutex> lock(mu_);
  return queue_.size();
}

size_t EventQueue::QueuedBytes() const {
  std::lock_guard<std::mutex> lock(mu_);
  return queued_bytes_;
}

size_t EventQueue::MaxBytes() const { return max_bytes_; }

bool EventQueue::IsClosed() const {
  std::lock_guard<std::mutex> lock(mu_);
  return closed_;
}

size_t EventQueue::EventMemoryBytes(const QueuedEvent& event) {
  const size_t data_bytes = event.data.capacity();
  const size_t checkpoint_bytes =
      event.checkpoint_gtid.empty() ? 0 : event.checkpoint_gtid.capacity();
  if (data_bytes > std::numeric_limits<size_t>::max() - checkpoint_bytes) {
    return std::numeric_limits<size_t>::max();
  }
  return data_bytes + checkpoint_bytes;
}

}  // namespace mes
