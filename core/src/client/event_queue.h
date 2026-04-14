// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#ifndef MES_CLIENT_EVENT_QUEUE_H_
#define MES_CLIENT_EVENT_QUEUE_H_

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <queue>
#include <vector>

#include "mes.h"

namespace mes {

// NOTE(review): this queue intentionally uses std::queue + mutex + condition
// variables rather than a lock-free SPSC ring buffer. Rationale:
//   * Each QueuedEvent owns a std::vector<uint8_t> payload; the per-event
//     heap allocation for that payload dominates any queue-structure cost,
//     so switching to a ring buffer would not meaningfully reduce latency
//     without also addressing the payload allocation (tracked separately as
//     a cross-cutting protocol-layer refactor).
//   * std::queue gives us bounded blocking Push/Pop and a single, simple
//     shutdown notification path (Close() + notify_all) that cleanly
//     unblocks both producer (reader thread) and consumer (Poll()).
//   * Lock-free SPSC adds complexity (memory ordering, shutdown handshake)
//     that is hard to audit. We will revisit if profiling shows the mutex
//     as a real bottleneck.

/** @brief An event buffered in the EventQueue. */
struct QueuedEvent {
  std::vector<uint8_t> data;   ///< Owned copy of binlog event bytes
  mes_error_t error = MES_OK;  ///< MES_OK for real events; error code for poison pill
};

/**
 * @brief Thread-safe bounded blocking queue for binlog events.
 *
 * Used between the reader thread (producer) and Poll() (consumer).
 * Push blocks when full (backpressure). Pop blocks when empty.
 * Close() unblocks all waiters for graceful shutdown.
 */
class EventQueue {
 public:
  explicit EventQueue(size_t max_size = 10000);
  ~EventQueue() = default;

  // Non-copyable, non-movable
  EventQueue(const EventQueue&) = delete;
  EventQueue& operator=(const EventQueue&) = delete;

  /**
   * @brief Push an event into the queue (producer side).
   * Blocks if queue is full. Returns false if queue is closed.
   */
  bool Push(QueuedEvent event);

  /**
   * @brief Pop an event from the queue (consumer side).
   * Blocks if queue is empty. Returns false if queue is closed AND empty.
   */
  bool Pop(QueuedEvent* event);

  /** @brief Close the queue, unblocking all waiters. Idempotent. */
  void Close();

  /** @brief Clear all pending events. Only call when no concurrent Push/Pop. */
  void Clear();

  /** @brief Current number of events in the queue (approximate under concurrency). */
  size_t Size() const;

  /** @brief Check if the queue has been closed. */
  bool IsClosed() const;

 private:
  mutable std::mutex mu_;
  std::condition_variable not_empty_cv_;
  std::condition_variable not_full_cv_;
  std::queue<QueuedEvent> queue_;
  size_t max_size_;
  bool closed_ = false;
};

}  // namespace mes

#endif  // MES_CLIENT_EVENT_QUEUE_H_
