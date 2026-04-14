// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include "client/event_queue.h"

#include <utility>

namespace mes {

EventQueue::EventQueue(size_t max_size) : max_size_(max_size == 0 ? 10000 : max_size) {}

bool EventQueue::Push(QueuedEvent event) {
  std::unique_lock<std::mutex> lock(mu_);
  not_full_cv_.wait(lock, [this] { return closed_ || queue_.size() < max_size_; });
  if (closed_) return false;
  queue_.push(std::move(event));
  not_empty_cv_.notify_one();
  return true;
}

bool EventQueue::Pop(QueuedEvent* event) {
  std::unique_lock<std::mutex> lock(mu_);
  not_empty_cv_.wait(lock, [this] { return closed_ || !queue_.empty(); });
  if (queue_.empty()) return false;  // closed and drained
  *event = std::move(queue_.front());
  queue_.pop();
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
  not_full_cv_.notify_all();
}

size_t EventQueue::Size() const {
  std::lock_guard<std::mutex> lock(mu_);
  return queue_.size();
}

bool EventQueue::IsClosed() const {
  std::lock_guard<std::mutex> lock(mu_);
  return closed_;
}

}  // namespace mes
