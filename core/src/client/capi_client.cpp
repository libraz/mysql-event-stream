// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @file capi_client.cpp
 * @brief C ABI wrapper for BinlogClient
 */

#include <mutex>
#include <new>
#include <string>

#include "client/binlog_client.h"
#include "mes.h"
#include "secure_cleanse.h"

struct mes_client {
  mes::BinlogClient client;
  /**
   * Message for a configuration rejected at this boundary, before
   * BinlogClient::Connect() is reached. Such a rejection has nowhere else to
   * record itself, and mes_client_last_error() is documented to describe every
   * failed call. Cleared at the start of each connect attempt so a stale
   * rejection cannot be mistaken for the outcome of a later one.
   *
   * Written by entry points documented single-owner but read by
   * mes_client_last_error(), which any thread may call at any moment, so both
   * go through boundary_error_mutex.
   */
  std::string boundary_error;
  /**
   * Stable buffer mes_client_last_error() copies boundary_error into, so the
   * pointer it hands out is not the one a writer may resize. Mirrors what
   * BinlogClient does for its own message; protected by boundary_error_mutex.
   */
  std::string boundary_error_snapshot;
  std::mutex boundary_error_mutex;
};

namespace {

/**
 * @brief Drop a stale boundary rejection before an entry point records its own.
 *
 * mes_client_last_error() describes the call that most recently failed, so a
 * rejection decided here must not outlive it. Taken under
 * boundary_error_mutex, which every writer of the field and every reader of it
 * also take, so a reader on another thread sees either the stale message whole
 * or none -- never storage mid-resize.
 */
void ClearBoundaryError(mes_client_t* c) {
  std::lock_guard<std::mutex> lock(c->boundary_error_mutex);
  c->boundary_error.clear();
}

/** @brief Record why this boundary refused the call, under the field's mutex. */
void SetBoundaryError(mes_client_t* c, const char* message) {
  std::lock_guard<std::mutex> lock(c->boundary_error_mutex);
  c->boundary_error = message;
}

}  // namespace

extern "C" {

MES_API mes_client_t* mes_client_create(void) { return new (std::nothrow) mes_client(); }

MES_API void mes_client_destroy(mes_client_t* c) { delete c; }

MES_API mes_error_t mes_client_connect(mes_client_t* c, const mes_client_config_t* config) {
  if (c == nullptr) {
    return MES_ERR_NULL_ARG;
  }
  ClearBoundaryError(c);
  if (config == nullptr) {
    SetBoundaryError(c, "config must not be NULL");
    return MES_ERR_NULL_ARG;
  }

  // The start position mode is an enumerator of the C ABI alone, so this is the
  // only layer that can reject an out-of-range value. Everything the mode maps
  // onto -- including binlog_file and binlog_position -- is validated once, by
  // BinlogClient::Connect(), which records the message describing it. Checking
  // any of that a second time here would let a configuration be rejected by a
  // branch other than the one that reports why.
  if (config->start_position_mode != MES_START_AT_CURRENT &&
      config->start_position_mode != MES_START_AT_GTID &&
      config->start_position_mode != MES_START_AT_POSITION) {
    SetBoundaryError(c, "start_position_mode must be current, gtid or position");
    return MES_ERR_INVALID_ARG;
  }

  mes::BinlogClientConfig cfg;
  // Declared before the credential is assigned below, which is why the guard
  // re-reads the string's bytes at scope exit rather than capturing them here.
  mes::SecureCleanse password_cleanse{cfg.password};
  cfg.host = config->host != nullptr ? config->host : "127.0.0.1";
  cfg.port = config->port;
  cfg.user = config->user != nullptr ? config->user : "";
  cfg.password = config->password != nullptr ? config->password : "";
  cfg.server_id = config->server_id;
  cfg.start_gtid = config->start_gtid != nullptr ? config->start_gtid : "";
  // Preserve the established C ABI behavior for callers compiled before the
  // explicit mode was added: a non-empty start_gtid has always meant resume
  // from that GTID. Empty sets require MES_START_AT_GTID to be unambiguous.
  cfg.start_at_current =
      config->start_position_mode == MES_START_AT_CURRENT && cfg.start_gtid.empty();
  cfg.start_at_file_position = config->start_position_mode == MES_START_AT_POSITION;
  cfg.binlog_file = config->binlog_file != nullptr ? config->binlog_file : "";
  cfg.binlog_position = config->binlog_position;
  // A zero timeout field means "unset" at this boundary, not "unbounded": the
  // zero-initialized config the header documents as a supported construction
  // must still bound every blocking read, otherwise a peer that completes the
  // handshake and then goes silent holds the call forever. Both C ABI entry
  // points that accept this struct resolve the two fields the same way, so one
  // field cannot mean different things depending on which one was called.
  cfg.connect_timeout_s =
      config->connect_timeout_s != 0 ? config->connect_timeout_s : MES_DEFAULT_CONNECT_TIMEOUT_S;
  cfg.read_timeout_s =
      config->read_timeout_s != 0 ? config->read_timeout_s : MES_DEFAULT_READ_TIMEOUT_S;
  cfg.ssl_mode = config->ssl_mode;
  cfg.ssl_ca = config->ssl_ca != nullptr ? config->ssl_ca : "";
  cfg.ssl_cert = config->ssl_cert != nullptr ? config->ssl_cert : "";
  cfg.ssl_key = config->ssl_key != nullptr ? config->ssl_key : "";
  cfg.max_queue_size = config->max_queue_size;
  cfg.allow_public_key_retrieval = config->allow_public_key_retrieval != 0;

  // BinlogClient wipes its own copy once authentication is done and never keeps
  // the credential for a self-initiated reconnection, so after this call the
  // staged copy above is the last plaintext in the process; the scope guard
  // wipes it on whichever path leaves this function.
  return c->client.Connect(cfg);
}

MES_API mes_error_t mes_client_start(mes_client_t* c) {
  if (c == nullptr) {
    return MES_ERR_NULL_ARG;
  }
  ClearBoundaryError(c);
  return c->client.StartStream();
}

MES_API mes_poll_result_t mes_client_poll(mes_client_t* c) {
  mes_poll_result_t out = {};
  if (c == nullptr) {
    out.error = MES_ERR_NULL_ARG;
    return out;
  }
  ClearBoundaryError(c);

  auto result = c->client.Poll();
  out.error = result.error;
  out.data = result.data;
  out.size = result.size;
  out.is_heartbeat = result.is_heartbeat ? 1 : 0;
  out.checksum_enabled = result.checksum_enabled ? 1 : 0;
  return out;
}

MES_API mes_error_t mes_client_poll_batch(mes_client_t* c, mes_poll_result_t* results,
                                          size_t capacity, size_t* result_count) {
  if (c == nullptr) return MES_ERR_NULL_ARG;
  ClearBoundaryError(c);
  if (results == nullptr || result_count == nullptr) {
    SetBoundaryError(c, "results and result_count must not be NULL");
    return MES_ERR_NULL_ARG;
  }
  if (capacity == 0) {
    SetBoundaryError(c, "capacity must be at least 1");
    return MES_ERR_INVALID_ARG;
  }

  std::vector<mes::PollResult> batch;
  c->client.PollBatch(capacity, &batch);
  for (size_t i = 0; i < batch.size(); ++i) {
    results[i].error = batch[i].error;
    results[i].data = batch[i].data;
    results[i].size = batch[i].size;
    results[i].is_heartbeat = batch[i].is_heartbeat ? 1 : 0;
    results[i].checksum_enabled = batch[i].checksum_enabled ? 1 : 0;
  }
  *result_count = batch.size();
  return MES_OK;
}

MES_API void mes_client_stop(mes_client_t* c) {
  if (c != nullptr) {
    c->client.Stop();
  }
}

MES_API void mes_client_disconnect(mes_client_t* c) {
  if (c != nullptr) {
    c->client.Disconnect();
  }
}

MES_API int mes_client_is_connected(mes_client_t* c) {
  if (c == nullptr) {
    return 0;
  }
  return c->client.IsConnected() ? 1 : 0;
}

MES_API int mes_client_is_streaming(mes_client_t* c) {
  return c != nullptr && c->client.IsStreaming() ? 1 : 0;
}

MES_API mes_server_flavor_t mes_client_flavor(mes_client_t* c) {
  if (c == nullptr) return MES_SERVER_FLAVOR_MYSQL;
  return c->client.GetServerFlavor() == mes::ServerFlavor::kMariaDB ? MES_SERVER_FLAVOR_MARIADB
                                                                    : MES_SERVER_FLAVOR_MYSQL;
}

MES_API const char* mes_client_last_error(mes_client_t* c) {
  if (c == nullptr) {
    return "";
  }
  {
    // Snapshot under boundary_error_mutex so the returned c_str() is not
    // invalidated by an entry point resizing boundary_error, and so the
    // pointer is valid until the next mes_client_last_error() call on this
    // client whichever branch produced it.
    std::lock_guard<std::mutex> lock(c->boundary_error_mutex);
    if (!c->boundary_error.empty()) {
      c->boundary_error_snapshot = c->boundary_error;
      return c->boundary_error_snapshot.c_str();
    }
  }
  // The boundary lock is released before delegating: BinlogClient serialises
  // its own message on its own mutex, and the two are never held at once.
  return c->client.GetLastError();
}

MES_API const char* mes_client_current_gtid(mes_client_t* c) {
  if (c == nullptr) {
    return "";
  }
  return c->client.GetCurrentGtid();
}

MES_API int mes_client_checksum_enabled(mes_client_t* c) {
  if (c == nullptr) return 0;
  return c->client.ChecksumEnabled() ? 1 : 0;
}

MES_API mes_error_t mes_client_set_max_event_size(mes_client_t* c, uint32_t max_event_size) {
  if (c == nullptr) return MES_ERR_NULL_ARG;
  c->client.SetMaxEventSize(max_event_size);
  return MES_OK;
}

MES_API uint32_t mes_client_get_max_event_size(mes_client_t* c) {
  return c == nullptr ? 0 : c->client.MaxEventSize();
}

MES_API mes_error_t mes_client_set_max_queue_bytes(mes_client_t* c, size_t max_queue_bytes) {
  if (c == nullptr) return MES_ERR_NULL_ARG;
  c->client.SetMaxQueueBytes(max_queue_bytes);
  return MES_OK;
}

MES_API size_t mes_client_get_max_queue_bytes(mes_client_t* c) {
  return c == nullptr ? 0 : c->client.MaxQueueBytes();
}

MES_API size_t mes_client_queued_bytes(mes_client_t* c) {
  return c == nullptr ? 0 : c->client.QueuedBytes();
}

MES_API uint64_t mes_client_crc_errors(mes_client_t* c) {
  return c == nullptr ? 0 : c->client.GetCRCErrors();
}

}  // extern "C"
