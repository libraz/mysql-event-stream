// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include "table_map.h"

#include <cstring>
#include <utility>

#include "mybinlog/event_header.h"
#include "mybinlog/status.h"
#include "mybinlog/table_map.h"
#include "wire_util.h"

namespace mes {

size_t TableMapRegistry::EntryCharge(const Entry& entry) {
  return entry.raw_body.size() + TableMetadataContentBytes(*entry.metadata);
}

void TableMapRegistry::EvictUntilWithinBounds(std::vector<uint64_t>* evicted_table_ids) {
  while (entries_.size() > 1 &&
         (entries_.size() > kMaxEntries || retained_bytes_ > kMaxRetainedBytes)) {
    const uint64_t evicted = lru_.back();
    auto it = entries_.find(evicted);
    retained_bytes_ -= EntryCharge(it->second);
    entries_.erase(it);
    lru_.pop_back();
    if (evicted_table_ids != nullptr) evicted_table_ids->push_back(evicted);
  }
}

bool TableMapRegistry::ProcessTableMapEvent(const uint8_t* data, size_t len,
                                            mybinlog::ServerFlavor flavor,
                                            std::vector<uint64_t>* evicted_table_ids,
                                            bool* unchanged) {
  if (unchanged != nullptr) *unchanged = false;
  if (data == nullptr || len < 6) return false;
  const uint64_t table_id = wire::ReadU48Le(data);
  auto existing = entries_.find(table_id);
  if (existing != entries_.end() && existing->second.flavor == flavor &&
      existing->second.raw_body.size() == len &&
      std::memcmp(existing->second.raw_body.data(), data, len) == 0) {
    Touch(existing);
    if (unchanged != nullptr) *unchanged = true;
    return true;
  }

  TableMetadata metadata;
  if (mybinlog::ParseTableMapEvent(mybinlog::EventHeader{}, data, len, flavor, &metadata) !=
      mybinlog::Status::kOk) {
    return false;
  }
  auto it = entries_.find(table_id);
  if (it != entries_.end()) {
    retained_bytes_ -= EntryCharge(it->second);
    it->second.metadata = std::make_shared<TableMetadata>(std::move(metadata));
    it->second.raw_body.assign(data, data + len);
    it->second.flavor = flavor;
    retained_bytes_ += EntryCharge(it->second);
    Touch(it);
    EvictUntilWithinBounds(evicted_table_ids);
    return true;
  }
  lru_.push_front(table_id);
  auto inserted = entries_.emplace(
      table_id, Entry{std::make_shared<TableMetadata>(std::move(metadata)),
                      std::vector<uint8_t>(data, data + len), flavor, lru_.begin()});
  retained_bytes_ += EntryCharge(inserted.first->second);
  EvictUntilWithinBounds(evicted_table_ids);
  return true;
}

void TableMapRegistry::Touch(std::unordered_map<uint64_t, Entry>::iterator it) {
  lru_.splice(lru_.begin(), lru_, it->second.lru_position);
  it->second.lru_position = lru_.begin();
}

const TableMetadata* TableMapRegistry::Lookup(uint64_t table_id) {
  auto it = entries_.find(table_id);
  if (it == entries_.end()) {
    return nullptr;
  }
  Touch(it);
  return it->second.metadata.get();
}

std::shared_ptr<const TableMetadata> TableMapRegistry::SharedLookup(uint64_t table_id) {
  auto it = entries_.find(table_id);
  if (it == entries_.end()) return {};
  Touch(it);
  return it->second.metadata;
}

void TableMapRegistry::Clear() {
  entries_.clear();
  lru_.clear();
  retained_bytes_ = 0;
}

size_t TableMapRegistry::Size() const { return entries_.size(); }

size_t TableMapRegistry::RetainedBytes() const { return retained_bytes_; }

void TableMapRegistry::ForEach(
    const std::function<void(uint64_t, const TableMetadata&)>& visitor) const {
  for (const auto& [table_id, entry] : entries_) {
    visitor(table_id, *entry.metadata);
  }
}

bool TableMapRegistry::ReplaceMetadata(uint64_t table_id, TableMetadata metadata,
                                       std::vector<uint64_t>* evicted_table_ids) {
  auto it = entries_.find(table_id);
  if (it == entries_.end()) return false;
  retained_bytes_ -= EntryCharge(it->second);
  // A new object, never an assignment through the existing one: the current
  // object may still be shared with queued events that borrow its names.
  it->second.metadata = std::make_shared<TableMetadata>(std::move(metadata));
  retained_bytes_ += EntryCharge(it->second);
  Touch(it);
  EvictUntilWithinBounds(evicted_table_ids);
  return true;
}

}  // namespace mes
