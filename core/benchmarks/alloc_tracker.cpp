// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include "alloc_tracker.h"

#include <cstring>

namespace mes {
namespace bench {

AllocStats& Stats() {
  static AllocStats stats;
  return stats;
}

namespace {

// Sized so the payload stays 16-byte aligned for the unaligned forms.
constexpr size_t kHeaderSize = 16;

struct Header {
  size_t size;
  size_t offset;  ///< payload - malloc base
};

void Record(size_t size) {
  AllocStats& s = Stats();
  s.live_bytes += size;
  s.total_bytes += size;
  ++s.alloc_count;
  if (s.live_bytes > s.peak_live_bytes) s.peak_live_bytes = s.live_bytes;
}

void* Allocate(size_t size, size_t alignment) {
  const size_t slack = alignment > kHeaderSize ? alignment + kHeaderSize : kHeaderSize;
  void* base = std::malloc(size + slack);
  if (base == nullptr) std::abort();
  uintptr_t raw = reinterpret_cast<uintptr_t>(base) + kHeaderSize;
  if (alignment > kHeaderSize) {
    raw = (raw + alignment - 1) & ~(static_cast<uintptr_t>(alignment) - 1);
  }
  uint8_t* payload = reinterpret_cast<uint8_t*>(raw);
  Header header{size, static_cast<size_t>(payload - static_cast<uint8_t*>(base))};
  std::memcpy(payload - sizeof(Header), &header, sizeof(Header));
  Record(size);
  return payload;
}

void Release(void* payload) {
  if (payload == nullptr) return;
  Header header{};
  std::memcpy(&header, static_cast<uint8_t*>(payload) - sizeof(Header), sizeof(Header));
  AllocStats& s = Stats();
  s.live_bytes -= header.size;
  std::free(static_cast<uint8_t*>(payload) - header.offset);
}

}  // namespace
}  // namespace bench
}  // namespace mes

void* operator new(size_t size) { return mes::bench::Allocate(size, 0); }
void* operator new[](size_t size) { return mes::bench::Allocate(size, 0); }
void* operator new(size_t size, const std::nothrow_t&) noexcept {
  return mes::bench::Allocate(size, 0);
}
void* operator new[](size_t size, const std::nothrow_t&) noexcept {
  return mes::bench::Allocate(size, 0);
}
void* operator new(size_t size, std::align_val_t align) {
  return mes::bench::Allocate(size, static_cast<size_t>(align));
}
void* operator new[](size_t size, std::align_val_t align) {
  return mes::bench::Allocate(size, static_cast<size_t>(align));
}

void operator delete(void* p) noexcept { mes::bench::Release(p); }
void operator delete[](void* p) noexcept { mes::bench::Release(p); }
void operator delete(void* p, size_t) noexcept { mes::bench::Release(p); }
void operator delete[](void* p, size_t) noexcept { mes::bench::Release(p); }
void operator delete(void* p, const std::nothrow_t&) noexcept { mes::bench::Release(p); }
void operator delete[](void* p, const std::nothrow_t&) noexcept { mes::bench::Release(p); }
void operator delete(void* p, std::align_val_t) noexcept { mes::bench::Release(p); }
void operator delete[](void* p, std::align_val_t) noexcept { mes::bench::Release(p); }
void operator delete(void* p, size_t, std::align_val_t) noexcept { mes::bench::Release(p); }
void operator delete[](void* p, size_t, std::align_val_t) noexcept { mes::bench::Release(p); }
