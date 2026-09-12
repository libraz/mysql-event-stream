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
#include "types.h"

namespace mes {

constexpr size_t kDefaultEventQueueBytes = MES_DEFAULT_QUEUE_BYTES;

/// MySQL prefixes every streamed binlog event with a one-byte OK marker. The
/// reader keeps that byte in the queued buffer instead of copying the event to
/// strip it, so a wire event of N bytes is held as N + 1 bytes.
constexpr size_t kQueuedEventPrefixBytes = 1;

/**
 * @brief Bytes a queue budget reserves for one entry's bookkeeping.
 *
 * A queued event carries, besides the wire buffer, the committed GTID
 * checkpoint set it would promote and -- for a sentinel -- an error message.
 * Those stay resident for exactly as long as the buffer does, so
 * QueuedEventCharge() charges them; the budget that admits a maximum-sized
 * event therefore has to cover them too, which is what this reserve is.
 *
 * A MySQL GTID set costs roughly 50 bytes per distinct source UUID, so the
 * reserve covers a replication history of some twenty thousand sources -- far
 * past any real topology -- while remaining negligible next to the default
 * budget. A checkpoint larger still does not break the byte bound; it only
 * crowds out queued events sooner.
 */
constexpr size_t kQueuedCheckpointReserveBytes = 1024U * 1024U;

/**
 * @brief Smallest queue byte budget that always admits a wire event of
 *        @p max_event_size bytes.
 *
 * Derived from QueuedEventCharge() so BinlogClient's start-up guard and the
 * queue's admission test cannot drift apart, and it budgets for the same two
 * terms the charge counts: the buffer including its packet prefix, plus
 * @ref kQueuedCheckpointReserveBytes of bookkeeping.
 */
constexpr size_t MinQueueBytesForEvent(size_t max_event_size) {
  return QueuedEventCharge(max_event_size + kQueuedEventPrefixBytes, kQueuedCheckpointReserveBytes);
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
  /// Framing the reader applied to this event: true when its last four bytes
  /// are a CRC32 trailer. Recorded per event because a FORMAT_DESCRIPTION_EVENT
  /// moves the reader's framing while earlier events, read under the previous
  /// one, are still queued. False for a heartbeat or an error sentinel, neither
  /// of which carries an event.
  bool checksum_enabled = false;
  /// Committed GTID promoted after the consumer finishes this event. Its length
  /// follows the number of distinct GTID source UUIDs, not the event, which is
  /// why it is charged to the byte budget like the payload is.
  std::string checkpoint_gtid;
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

  /**
   * @brief Push with a reason when the queue cannot accept the event.
   *
   * An event is admitted once the queue holds fewer than max_size entries and
   * its QueuedEventCharge() fits in what is left of the byte budget; a single
   * event whose charge exceeds the whole budget is refused as kEventTooLarge
   * rather than blocking forever. A terminal error sentinel is exempt from the
   * byte budget: it is the only way the consumer learns why the stream ended,
   * so a full budget must not be able to swallow it.
   */
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

  /**
   * @brief Bytes currently charged to the queue: every resident byte of every
   *        queued entry, buffer sizes rather than allocator capacities.
   *
   * Bounded by MaxBytes(), except that an admitted terminal sentinel's message
   * may carry the total marginally past it.
   */
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
