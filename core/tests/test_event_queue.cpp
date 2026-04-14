// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @file test_event_queue.cpp
 * @brief Unit tests for the EventQueue bounded blocking queue
 */

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>
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
  ASSERT_TRUE(q.Push(std::move(ev)));

  mes::QueuedEvent out;
  ASSERT_TRUE(q.Pop(&out));
  EXPECT_EQ(out.error, MES_ERR_STREAM);
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

  EXPECT_EQ(received.size(), static_cast<size_t>(kTotal));
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

}  // namespace
