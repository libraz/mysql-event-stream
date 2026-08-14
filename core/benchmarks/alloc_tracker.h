// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @file alloc_tracker.h
 * @brief Process-wide allocation accounting for the benchmark binaries.
 *
 * Replaces the global operator new/delete so a workload can report how many
 * bytes a decoded event costs and how much stays resident while events sit in
 * the engine queue. Every allocation carries a 16-byte header holding its size
 * and the offset back to the malloc base, so both the plain and the
 * over-aligned forms free correctly.
 *
 * Single-threaded by construction: the counters are plain scalars and the
 * benchmarks never decode from more than one thread.
 */

#ifndef MES_CORE_BENCHMARKS_ALLOC_TRACKER_H_
#define MES_CORE_BENCHMARKS_ALLOC_TRACKER_H_

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <new>

namespace mes {
namespace bench {

struct AllocStats {
  size_t live_bytes = 0;
  size_t peak_live_bytes = 0;
  size_t total_bytes = 0;
  size_t alloc_count = 0;
};

/// Mutable process-wide counters. Defined in alloc_tracker.cpp.
AllocStats& Stats();

/// Zero the counters and re-seed the peak at the current live total.
inline void ResetStats() {
  AllocStats& s = Stats();
  s.peak_live_bytes = s.live_bytes;
  s.total_bytes = 0;
  s.alloc_count = 0;
}

/// Snapshot used to express a measurement as a delta.
struct AllocSnapshot {
  size_t live_bytes;
  size_t peak_live_bytes;
  size_t total_bytes;
  size_t alloc_count;
};

inline AllocSnapshot Snapshot() {
  const AllocStats& s = Stats();
  return AllocSnapshot{s.live_bytes, s.peak_live_bytes, s.total_bytes, s.alloc_count};
}

}  // namespace bench
}  // namespace mes

#endif  // MES_CORE_BENCHMARKS_ALLOC_TRACKER_H_
