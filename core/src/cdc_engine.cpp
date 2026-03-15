// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#include "cdc_engine.h"

#include "binary_util.h"
#include "rotate_event.h"

#ifdef MES_HAS_MYSQL
#include "client/metadata_fetcher.h"
#endif

namespace mes {

static void AttachColumnNames(RowData& row, const TableMetadata& meta) {
  for (size_t i = 0; i < row.columns.size() && i < meta.columns.size(); i++) {
    row.columns[i].name = meta.columns[i].name;
  }
}

size_t CdcEngine::Feed(const uint8_t* data, size_t len) {
  size_t total_consumed = 0;

  while (total_consumed < len) {
    size_t consumed = stream_parser_.Feed(data + total_consumed, len - total_consumed);
    if (consumed == 0 && !stream_parser_.HasEvent()) {
      break;
    }
    total_consumed += consumed;

    while (stream_parser_.HasEvent()) {
      const EventHeader& header = stream_parser_.CurrentHeader();
      const uint8_t* body = nullptr;
      size_t body_len = 0;
      stream_parser_.CurrentBody(&body, &body_len);
      ProcessEvent(header, body, body_len);
      stream_parser_.Advance();
    }
  }

  return total_consumed;
}

bool CdcEngine::NextEvent(ChangeEvent* event) {
  if (event_queue_.empty() || event == nullptr) {
    return false;
  }
  *event = std::move(event_queue_.front());
  event_queue_.pop();
  return true;
}

bool CdcEngine::HasEvents() const { return !event_queue_.empty(); }

const BinlogPosition& CdcEngine::CurrentPosition() const { return position_; }

void CdcEngine::Reset() {
  stream_parser_.Reset();
  table_registry_.Clear();
  position_ = BinlogPosition{};
  // Clear the queue
  std::queue<ChangeEvent> empty;
  event_queue_.swap(empty);
}

size_t CdcEngine::PendingEventCount() const { return event_queue_.size(); }

#ifdef MES_HAS_MYSQL
void CdcEngine::SetMetadataFetcher(MetadataFetcher* fetcher) { metadata_fetcher_ = fetcher; }
#endif

void CdcEngine::ProcessEvent(const EventHeader& header, const uint8_t* body, size_t body_len) {
  // Update position from next_position
  if (header.next_position > 0) {
    position_.offset = header.next_position;
  }

  switch (header.type_code) {
    case static_cast<uint8_t>(BinlogEventType::kTableMapEvent):
      table_registry_.ProcessTableMapEvent(body, body_len);
#ifdef MES_HAS_MYSQL
      if (metadata_fetcher_) {
        auto* meta = table_registry_.MutableLookup(binary::ReadU48Le(body));
        if (meta && !meta->columns.empty() && meta->columns[0].name.empty()) {
          auto infos = metadata_fetcher_->FetchColumnInfo(meta->database_name, meta->table_name,
                                                          meta->columns.size());
          for (size_t i = 0; i < infos.size() && i < meta->columns.size(); i++) {
            meta->columns[i].name = infos[i].name;
            meta->columns[i].is_unsigned = infos[i].is_unsigned;
          }
        }
      }
#endif
      break;

    case static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent):
    case static_cast<uint8_t>(BinlogEventType::kWriteRowsEventV1):
    case static_cast<uint8_t>(BinlogEventType::kUpdateRowsEvent):
    case static_cast<uint8_t>(BinlogEventType::kUpdateRowsEventV1):
    case static_cast<uint8_t>(BinlogEventType::kDeleteRowsEvent):
    case static_cast<uint8_t>(BinlogEventType::kDeleteRowsEventV1):
      ProcessRowEvent(header, body, body_len);
      break;

    case static_cast<uint8_t>(BinlogEventType::kRotateEvent): {
      RotateEventData rot;
      if (ParseRotateEvent(body, body_len, &rot)) {
        position_.binlog_file = std::move(rot.new_log_file);
        position_.offset = rot.position;
      }
      break;
    }

    default:
      // Skip unhandled event types
      break;
  }
}

void CdcEngine::ProcessRowEvent(const EventHeader& header, const uint8_t* body, size_t body_len) {
  if (body == nullptr || body_len < 6) return;

  // Extract table_id from the first 6 bytes of the body
  uint64_t table_id = binary::ReadU48Le(body);
  const TableMetadata* meta = table_registry_.Lookup(table_id);
  if (meta == nullptr) return;

  uint8_t type_code = header.type_code;
  bool is_v2 = (type_code == static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent) ||
                type_code == static_cast<uint8_t>(BinlogEventType::kUpdateRowsEvent) ||
                type_code == static_cast<uint8_t>(BinlogEventType::kDeleteRowsEvent));

  bool is_write = (type_code == static_cast<uint8_t>(BinlogEventType::kWriteRowsEvent) ||
                   type_code == static_cast<uint8_t>(BinlogEventType::kWriteRowsEventV1));
  bool is_update = (type_code == static_cast<uint8_t>(BinlogEventType::kUpdateRowsEvent) ||
                    type_code == static_cast<uint8_t>(BinlogEventType::kUpdateRowsEventV1));

  if (is_write) {
    std::vector<RowData> rows;
    if (DecodeWriteRows(body, body_len, *meta, is_v2, &rows)) {
      for (auto& row : rows) {
        ChangeEvent event;
        event.type = EventType::kInsert;
        event.database = meta->database_name;
        event.table = meta->table_name;
        event.after = std::move(row);
        AttachColumnNames(event.after, *meta);
        event.timestamp = header.timestamp;
        event.position = position_;
        event_queue_.push(std::move(event));
      }
    }
  } else if (is_update) {
    std::vector<UpdatePair> pairs;
    if (DecodeUpdateRows(body, body_len, *meta, is_v2, &pairs)) {
      for (auto& pair : pairs) {
        ChangeEvent event;
        event.type = EventType::kUpdate;
        event.database = meta->database_name;
        event.table = meta->table_name;
        event.before = std::move(pair.before);
        event.after = std::move(pair.after);
        AttachColumnNames(event.before, *meta);
        AttachColumnNames(event.after, *meta);
        event.timestamp = header.timestamp;
        event.position = position_;
        event_queue_.push(std::move(event));
      }
    }
  } else {
    // DELETE
    std::vector<RowData> rows;
    if (DecodeDeleteRows(body, body_len, *meta, is_v2, &rows)) {
      for (auto& row : rows) {
        ChangeEvent event;
        event.type = EventType::kDelete;
        event.database = meta->database_name;
        event.table = meta->table_name;
        event.before = std::move(row);
        AttachColumnNames(event.before, *meta);
        event.timestamp = header.timestamp;
        event.position = position_;
        event_queue_.push(std::move(event));
      }
    }
  }
}

}  // namespace mes
