// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @file test_event_queue.cpp
 * @brief Unit tests for the EventQueue bounded blocking queue
 */

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "client/event_queue.h"

namespace {

TEST(EventQueueTest, PushPopBasic) {
  mes::EventQueue q(100);
  mes::QueuedEvent ev;
  ev.data = {1, 2, 3};

  ASSERT_TRUE(q.Push(std::move(ev)));

  mes::QueuedEvent out;
  ASSERT_TRUE(q.Pop(&out));
  EXPECT_EQ(out.data, (std::vector<uint8_t>{1, 2, 3}));
  EXPECT_EQ(out.error, MES_OK);
}

TEST(EventQueueTest, PushPopMultiple) {
  mes::EventQueue q(100);

  for (int i = 0; i < 5; i++) {
    mes::QueuedEvent ev;
    ev.data = {static_cast<uint8_t>(i)};
    ASSERT_TRUE(q.Push(std::move(ev)));
  }

  for (int i = 0; i < 5; i++) {
    mes::QueuedEvent out;
    ASSERT_TRUE(q.Pop(&out));
    ASSERT_EQ(out.data.size(), 1u);
    EXPECT_EQ(out.data[0], static_cast<uint8_t>(i));
  }
}

TEST(EventQueueTest, PopBlocksOnEmpty) {
  mes::EventQueue q(100);
  std::atomic<bool> done{false};
  mes::QueuedEvent popped;

  std::thread t([&]() {
    q.Pop(&popped);
    done.store(true);
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  EXPECT_FALSE(done.load());

  mes::QueuedEvent ev;
  ev.data = {42};
  q.Push(std::move(ev));

  t.join();
  EXPECT_TRUE(done.load());
  EXPECT_EQ(popped.data, (std::vector<uint8_t>{42}));
}

TEST(EventQueueTest, PushBlocksOnFull) {
  mes::EventQueue q(2);

  mes::QueuedEvent e1;
  e1.data = {1};
  ASSERT_TRUE(q.Push(std::move(e1)));

  mes::QueuedEvent e2;
  e2.data = {2};
  ASSERT_TRUE(q.Push(std::move(e2)));

  std::atomic<bool> done{false};
  std::thread t([&]() {
    mes::QueuedEvent e3;
    e3.data = {3};
    q.Push(std::move(e3));
    done.store(true);
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  EXPECT_FALSE(done.load());

  mes::QueuedEvent out;
  q.Pop(&out);

  t.join();
  EXPECT_TRUE(done.load());
}

TEST(EventQueueTest, CloseUnblocksPop) {
  mes::EventQueue q(100);
  std::atomic<bool> done{false};
  bool pop_result = true;

  std::thread t([&]() {
    mes::QueuedEvent out;
    pop_result = q.Pop(&out);
    done.store(true);
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  EXPECT_FALSE(done.load());

  q.Close();

  t.join();
  EXPECT_TRUE(done.load());
  EXPECT_FALSE(pop_result);
}

TEST(EventQueueTest, CloseUnblocksPush) {
  mes::EventQueue q(1);

  mes::QueuedEvent e1;
  e1.data = {1};
  ASSERT_TRUE(q.Push(std::move(e1)));

  std::atomic<bool> done{false};
  bool push_result = true;

  std::thread t([&]() {
    mes::QueuedEvent e2;
    e2.data = {2};
    push_result = q.Push(std::move(e2));
    done.store(true);
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  EXPECT_FALSE(done.load());

  q.Close();

  t.join();
  EXPECT_TRUE(done.load());
  EXPECT_FALSE(push_result);
}

TEST(EventQueueTest, CloseIdempotent) {
  mes::EventQueue q(100);
  q.Close();
  q.Close();
  EXPECT_TRUE(q.IsClosed());
}

TEST(EventQueueTest, PushAfterClose) {
  mes::EventQueue q(100);
  q.Close();

  mes::QueuedEvent ev;
  ev.data = {1};
  EXPECT_FALSE(q.Push(std::move(ev)));
}

TEST(EventQueueTest, PopAfterCloseWithData) {
  mes::EventQueue q(100);

  mes::QueuedEvent e1;
  e1.data = {10};
  ASSERT_TRUE(q.Push(std::move(e1)));

  mes::QueuedEvent e2;
  e2.data = {20};
  ASSERT_TRUE(q.Push(std::move(e2)));

  q.Close();

  mes::QueuedEvent out;
  ASSERT_TRUE(q.Pop(&out));
  EXPECT_EQ(out.data, (std::vector<uint8_t>{10}));

  ASSERT_TRUE(q.Pop(&out));
  EXPECT_EQ(out.data, (std::vector<uint8_t>{20}));

  EXPECT_FALSE(q.Pop(&out));
}

TEST(EventQueueTest, ErrorSentinel) {
  mes::EventQueue q(100);

  mes::QueuedEvent ev;
  ev.error = MES_ERR_STREAM;
  ev.server_error_code = 1236;
  ev.error_message = "MySQL server error 1236: requested GTID has been purged";
  ASSERT_TRUE(q.Push(std::move(ev)));

  mes::QueuedEvent out;
  ASSERT_TRUE(q.Pop(&out));
  EXPECT_EQ(out.error, MES_ERR_STREAM);
  EXPECT_EQ(out.server_error_code, 1236u);
  EXPECT_EQ(out.error_message, "MySQL server error 1236: requested GTID has been purged");
  EXPECT_TRUE(out.data.empty());
}

TEST(EventQueueTest, ErrorThenClose) {
  mes::EventQueue q(100);

  mes::QueuedEvent ev;
  ev.error = MES_ERR_STREAM;
  ASSERT_TRUE(q.Push(std::move(ev)));

  q.Close();

  mes::QueuedEvent out;
  ASSERT_TRUE(q.Pop(&out));
  EXPECT_EQ(out.error, MES_ERR_STREAM);
}

TEST(EventQueueTest, ConcurrentProducerConsumer) {
  mes::EventQueue q(100);
  constexpr int kCount = 1000;

  std::vector<mes::QueuedEvent> received;
  received.reserve(kCount);

  std::thread consumer([&]() {
    for (int i = 0; i < kCount; i++) {
      mes::QueuedEvent out;
      if (!q.Pop(&out)) break;
      received.push_back(std::move(out));
    }
  });

  std::thread producer([&]() {
    for (int i = 0; i < kCount; i++) {
      mes::QueuedEvent ev;
      ev.data = {static_cast<uint8_t>(i & 0xFF)};
      q.Push(std::move(ev));
    }
  });

  producer.join();
  consumer.join();

  ASSERT_EQ(received.size(), static_cast<size_t>(kCount));
  for (int i = 0; i < kCount; i++) {
    EXPECT_EQ(received[i].data[0], static_cast<uint8_t>(i & 0xFF));
  }
}

TEST(EventQueueTest, MultiProducerSingleConsumer) {
  mes::EventQueue q(100);
  constexpr int kProducers = 4;
  constexpr int kPerProducer = 250;
  constexpr int kTotal = kProducers * kPerProducer;

  std::atomic<int> consumed{0};
  std::vector<mes::QueuedEvent> received;
  received.reserve(kTotal);

  std::thread consumer([&]() {
    for (int i = 0; i < kTotal; i++) {
      mes::QueuedEvent out;
      if (!q.Pop(&out)) break;
      received.push_back(std::move(out));
      consumed.fetch_add(1);
    }
  });

  std::vector<std::thread> producers;
  for (int p = 0; p < kProducers; p++) {
    producers.emplace_back([&q, p]() {
      for (int i = 0; i < kPerProducer; i++) {
        mes::QueuedEvent ev;
        ev.data = {static_cast<uint8_t>(p), static_cast<uint8_t>(i & 0xFF)};
        q.Push(std::move(ev));
      }
    });
  }

  for (auto& t : producers) t.join();
  consumer.join();

  ASSERT_EQ(received.size(), static_cast<size_t>(kTotal));

  // Cardinality alone also accepts a queue that delivered one producer's event
  // twice while dropping another's, so compare the delivered payloads against
  // every (producer, index) pair that was pushed. kPerProducer stays below 256,
  // so each pair is unique once the index is narrowed to a byte.
  std::vector<std::pair<uint8_t, uint8_t>> delivered;
  delivered.reserve(received.size());
  for (const auto& ev : received) {
    ASSERT_EQ(ev.data.size(), 2u);
    delivered.emplace_back(ev.data[0], ev.data[1]);
  }
  std::sort(delivered.begin(), delivered.end());

  std::vector<std::pair<uint8_t, uint8_t>> expected;
  expected.reserve(kTotal);
  for (int p = 0; p < kProducers; p++) {
    for (int i = 0; i < kPerProducer; i++) {
      expected.emplace_back(static_cast<uint8_t>(p), static_cast<uint8_t>(i & 0xFF));
    }
  }
  std::sort(expected.begin(), expected.end());

  EXPECT_EQ(delivered, expected);
}

TEST(EventQueueTest, StressTest) {
  mes::EventQueue q(10);
  constexpr int kCount = 10000;

  std::atomic<int> consumed{0};

  std::thread consumer([&]() {
    for (int i = 0; i < kCount; i++) {
      mes::QueuedEvent out;
      if (!q.Pop(&out)) break;
      consumed.fetch_add(1);
    }
  });

  std::thread producer([&]() {
    for (int i = 0; i < kCount; i++) {
      mes::QueuedEvent ev;
      ev.data = {static_cast<uint8_t>(i & 0xFF)};
      q.Push(std::move(ev));
    }
  });

  producer.join();
  consumer.join();

  EXPECT_EQ(consumed.load(), kCount);
}

TEST(EventQueueTest, SizeIsConsistent) {
  mes::EventQueue q(100);

  mes::QueuedEvent e1, e2, e3;
  e1.data = {1};
  e2.data = {2};
  e3.data = {3};
  q.Push(std::move(e1));
  q.Push(std::move(e2));
  q.Push(std::move(e3));
  EXPECT_EQ(q.Size(), 3u);

  mes::QueuedEvent out;
  q.Pop(&out);
  EXPECT_EQ(q.Size(), 2u);

  q.Clear();
  EXPECT_EQ(q.Size(), 0u);
}

TEST(EventQueueTest, ByteBudgetBlocksBeforeCountLimit) {
  mes::EventQueue q(100, 5);
  mes::QueuedEvent first;
  first.data = {1, 2, 3, 4};
  ASSERT_TRUE(q.Push(std::move(first)));
  EXPECT_EQ(q.QueuedBytes(), 4u);

  std::atomic<bool> pushed{false};
  std::thread producer([&]() {
    mes::QueuedEvent second;
    second.data = {5, 6};
    pushed.store(q.Push(std::move(second)));
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  EXPECT_FALSE(pushed.load());
  EXPECT_EQ(q.Size(), 1u);
  EXPECT_LE(q.QueuedBytes(), q.MaxBytes());

  mes::QueuedEvent out;
  ASSERT_TRUE(q.Pop(&out));
  producer.join();
  EXPECT_TRUE(pushed.load());
  EXPECT_EQ(q.QueuedBytes(), 2u);
}

TEST(EventQueueTest, OversizedSingleEventIsRejectedWithoutBlocking) {
  mes::EventQueue q(100, 4);
  mes::QueuedEvent event;
  event.data = {1, 2, 3, 4, 5};

  EXPECT_EQ(q.PushWithStatus(std::move(event)), mes::EventQueue::PushResult::kEventTooLarge);
  EXPECT_EQ(q.Size(), 0u);
  EXPECT_EQ(q.QueuedBytes(), 0u);
}

TEST(EventQueueTest, ChargesPayloadSizeRatherThanVectorCapacity) {
  mes::EventQueue q(100, 4);
  mes::QueuedEvent event;
  event.data.reserve(1024);
  event.data = {1, 2, 3, 4};
  ASSERT_GT(event.data.capacity(), event.data.size());

  EXPECT_EQ(q.PushWithStatus(std::move(event)), mes::EventQueue::PushResult::kPushed);
  EXPECT_EQ(q.QueuedBytes(), 4u);
}

TEST(EventQueueTest, EventAtMaxEventSizeFitsTheMinimumBudget) {
  // The budget a client is allowed to configure for a given max_event_size and
  // the charge the queue applies come from the same definition, so an event at
  // the ceiling must be admitted rather than terminating the stream -- together
  // with the checkpoint the reader attaches to it, which the minimum budget
  // reserves room for.
  constexpr size_t kMaxEventSize = 4096;
  mes::EventQueue q(100, mes::MinQueueBytesForEvent(kMaxEventSize));

  const std::string checkpoint = "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-1000000";
  mes::QueuedEvent event;
  event.data.assign(kMaxEventSize + mes::kQueuedEventPrefixBytes, 0x7F);
  event.data_offset = mes::kQueuedEventPrefixBytes;
  event.checkpoint_gtid = checkpoint;

  EXPECT_EQ(q.PushWithStatus(std::move(event)), mes::EventQueue::PushResult::kPushed);
  EXPECT_EQ(q.QueuedBytes(), kMaxEventSize + mes::kQueuedEventPrefixBytes + checkpoint.size());
  EXPECT_LE(q.QueuedBytes(), q.MaxBytes());

  mes::QueuedEvent out;
  ASSERT_TRUE(q.Pop(&out));
  EXPECT_EQ(out.data.size() - out.data_offset, kMaxEventSize);
  EXPECT_EQ(q.QueuedBytes(), 0u);
}

TEST(EventQueueTest, CheckpointAndErrorTextAreChargedToTheBudget) {
  mes::EventQueue q(100, 1024);

  mes::QueuedEvent event;
  event.data = {1, 2, 3, 4};
  event.checkpoint_gtid = std::string(512, 'a');
  ASSERT_EQ(q.PushWithStatus(std::move(event)), mes::EventQueue::PushResult::kPushed);
  EXPECT_EQ(q.QueuedBytes(), 4u + 512u);

  // A terminal error sentinel carries no payload and is the consumer's only
  // account of why the stream ended, so the byte budget does not gate it -- but
  // its message is charged like any other resident byte, so the reported total
  // stays an honest measure of what the queue holds.
  mes::QueuedEvent sentinel;
  sentinel.error = MES_ERR_STREAM;
  sentinel.error_message = std::string(4096, 'e');
  EXPECT_EQ(q.PushWithStatus(std::move(sentinel)), mes::EventQueue::PushResult::kPushed);
  EXPECT_EQ(q.QueuedBytes(), 4u + 512u + 4096u);
}

// A GTID set as the server formats it: one 36-character UUID plus an interval
// per source, comma separated. Its length follows how many distinct sources the
// replication history has accumulated and has nothing to do with event size.
std::string MakeWideGtidSet(size_t source_count) {
  std::string set;
  for (size_t i = 0; i < source_count; ++i) {
    if (!set.empty()) set.push_back(',');
    const std::string suffix = std::to_string(i);
    set += "3e11fa47-71ca-11e1-9e33-";
    set.append(12 - suffix.size(), '0');
    set += suffix;
    set += ":1-4294967296";
  }
  return set;
}

TEST(EventQueueTest, CheckpointBytesCannotCarryResidentMemoryPastTheBudget) {
  // Entries that are tiny on the wire but carry a wide GTID set: the checkpoint
  // outweighs the payload two hundred to one, so if it were excluded from the
  // charge the entry count -- not the byte budget -- would decide how much
  // memory the queue holds.
  constexpr size_t kMaxEntries = 1000;
  constexpr size_t kBudgetBytes = 256U * 1024U;
  constexpr size_t kPayloadBytes = 64;
  const std::string checkpoint = MakeWideGtidSet(256);
  ASSERT_GT(checkpoint.size(), 100u * kPayloadBytes);
  ASSERT_LT(kMaxEntries * kPayloadBytes, kBudgetBytes);  // payload alone never fills it

  mes::EventQueue q(kMaxEntries, kBudgetBytes);
  std::atomic<size_t> admitted{0};
  std::thread producer([&]() {
    for (size_t i = 0; i < kMaxEntries; ++i) {
      mes::QueuedEvent event;
      event.data.assign(kPayloadBytes, 0x11);
      event.checkpoint_gtid = checkpoint;
      if (!q.Push(std::move(event))) return;
      admitted.fetch_add(1);
    }
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  const size_t reported_bytes = q.QueuedBytes();
  q.Close();
  producer.join();

  // Recount from the entries themselves rather than trusting the accounting
  // under test: this is the memory the queue actually held.
  size_t resident_bytes = 0;
  size_t drained = 0;
  mes::QueuedEvent out;
  while (q.TryPop(&out)) {
    resident_bytes += out.data.size() + out.checkpoint_gtid.size() + out.error_message.size();
    ++drained;
  }

  EXPECT_LE(resident_bytes, kBudgetBytes);
  EXPECT_EQ(reported_bytes, resident_bytes);
  // The byte budget, not the entry count, is what stopped the producer.
  EXPECT_LT(drained, kMaxEntries);
  EXPECT_GT(drained, 0u);
  EXPECT_EQ(drained, admitted.load());
}

TEST(EventQueueTest, ClearResetsByteChargeAndUnblocksProducer) {
  mes::EventQueue q(100, 4);
  mes::QueuedEvent first;
  first.data = {1, 2, 3, 4};
  ASSERT_TRUE(q.Push(std::move(first)));

  std::atomic<bool> pushed{false};
  std::thread producer([&]() {
    mes::QueuedEvent second;
    second.data = {5};
    pushed.store(q.Push(std::move(second)));
  });
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  EXPECT_FALSE(pushed.load());

  q.Clear();
  producer.join();
  EXPECT_TRUE(pushed.load());
  EXPECT_EQ(q.QueuedBytes(), 1u);
}

}  // namespace
