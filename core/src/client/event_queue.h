// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#ifndef MES_CLIENT_EVENT_QUEUE_H_
#define MES_CLIENT_EVENT_QUEUE_H_

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <queue>
#include <string>
#include <vector>

#include "mes.h"

namespace mes {

constexpr size_t kDefaultEventQueueBytes = MES_DEFAULT_QUEUE_BYTES;

/// MySQL prefixes every streamed binlog event with a one-byte OK marker. The
/// reader keeps that byte in the queued buffer instead of copying the event to
/// strip it, so a wire event of N bytes is held as N + 1 bytes.
constexpr size_t kQueuedEventPrefixBytes = 1;

/**
 * @brief Bytes a queued buffer of @p buffer_bytes charges to the byte budget.
 *
 * This is the single definition of what `max_queue_bytes` counts. Only the
 * buffered wire payload is charged. A queued event also carries a checkpoint
 * GTID set and, for a sentinel, an error message; those are reader-side
 * bookkeeping whose length grows with the number of source UUIDs rather than
 * with the event, so charging them would make the admissible event size depend
 * on a quantity no caller can size a budget for -- and would let an event
 * within max_event_size be refused by a budget that claims to accommodate it.
 */
constexpr size_t QueuedEventCharge(size_t buffer_bytes) { return buffer_bytes; }

/**
 * @brief Smallest queue byte budget that always admits a wire event of
 *        @p max_event_size bytes.
 *
 * Derived from QueuedEventCharge() so BinlogClient's start-up guard and the
 * queue's admission test cannot drift apart.
 */
constexpr size_t MinQueueBytesForEvent(size_t max_event_size) {
  return QueuedEventCharge(max_event_size + kQueuedEventPrefixBytes);
}

// Note: this queue intentionally uses std::queue + mutex + condition
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
  std::vector<uint8_t> data;       ///< Owned binlog packet bytes (empty for heartbeat/error)
  size_t data_offset = 0;          ///< Offset of the event payload within `data`
                                   ///< (used to skip the MySQL OK byte without
                                   ///< copying). Consumers read from
                                   ///< `data.data() + data_offset` with size
                                   ///< `data.size() - data_offset`.
  mes_error_t error = MES_OK;      ///< MES_OK for real events; error code for poison pill
  uint16_t server_error_code = 0;  ///< MySQL ERR packet code for an error sentinel
  std::string error_message;       ///< Detailed error text for an error sentinel
  bool is_heartbeat = false;       ///< true for silent heartbeats from the server
  std::string checkpoint_gtid;     ///< Committed GTID promoted after consumer finishes this event
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
  enum class PushResult { kPushed, kClosed, kEventTooLarge };

  explicit EventQueue(size_t max_size = 10000, size_t max_bytes = kDefaultEventQueueBytes);
  ~EventQueue() = default;

  // Non-copyable, non-movable
  EventQueue(const EventQueue&) = delete;
  EventQueue& operator=(const EventQueue&) = delete;

  /**
   * @brief Push an event into the queue (producer side).
   * Blocks if queue is full. Returns false if queue is closed.
   */
  bool Push(QueuedEvent event);

  /** Push with a reason when the queue cannot accept the event. */
  PushResult PushWithStatus(QueuedEvent event);

  /**
   * @brief Pop an event from the queue (consumer side).
   * Blocks if queue is empty. Returns false if queue is closed AND empty.
   */
  bool Pop(QueuedEvent* event);

  /** Pop an already queued event without blocking. Returns false when empty. */
  bool TryPop(QueuedEvent* event);

  /** @brief Close the queue, unblocking all waiters. Idempotent. */
  void Close();

  /** @brief Clear all pending events. Only call when no concurrent Push/Pop. */
  void Clear();

  /** @brief Current number of events in the queue (approximate under concurrency). */
  size_t Size() const;

  /** @brief Current charged payload bytes in the queue (buffer size, not capacity). */
  size_t QueuedBytes() const;

  /** @brief Configured queue byte budget. */
  size_t MaxBytes() const;

  /** @brief Check if the queue has been closed. */
  bool IsClosed() const;

 private:
  mutable std::mutex mu_;
  std::condition_variable not_empty_cv_;
  std::condition_variable not_full_cv_;
  std::queue<QueuedEvent> queue_;
  size_t max_size_;
  size_t max_bytes_;
  size_t queued_bytes_ = 0;
  bool closed_ = false;

  static size_t EventMemoryBytes(const QueuedEvent& event);
};

}  // namespace mes

#endif  // MES_CLIENT_EVENT_QUEUE_H_
