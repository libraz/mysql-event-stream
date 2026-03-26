// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @file capi_client.cpp
 * @brief C ABI wrapper for BinlogClient
 */

#include <new>

#include "client/binlog_client.h"
#include "mes.h"

struct mes_client {
  mes::BinlogClient client;
};

extern "C" {

MES_API mes_client_t* mes_client_create(void) {
  return new (std::nothrow) mes_client();
}

MES_API void mes_client_destroy(mes_client_t* c) { delete c; }

MES_API mes_error_t mes_client_connect(
    mes_client_t* c, const mes_client_config_t* config) {
  if (c == nullptr || config == nullptr) {
    return MES_ERR_NULL_ARG;
  }

  mes::BinlogClientConfig cfg;
  cfg.host = config->host != nullptr ? config->host : "127.0.0.1";
  cfg.port = config->port;
  cfg.user = config->user != nullptr ? config->user : "";
  cfg.password = config->password != nullptr ? config->password : "";
  cfg.server_id = config->server_id;
  cfg.start_gtid = config->start_gtid != nullptr ? config->start_gtid : "";
  cfg.connect_timeout_s = config->connect_timeout_s;
  cfg.read_timeout_s = config->read_timeout_s;
  cfg.ssl_mode = config->ssl_mode;
  cfg.ssl_ca = config->ssl_ca != nullptr ? config->ssl_ca : "";
  cfg.ssl_cert = config->ssl_cert != nullptr ? config->ssl_cert : "";
  cfg.ssl_key = config->ssl_key != nullptr ? config->ssl_key : "";
  cfg.max_queue_size = config->max_queue_size;

  return c->client.Connect(cfg);
}

MES_API mes_error_t mes_client_start(mes_client_t* c) {
  if (c == nullptr) {
    return MES_ERR_NULL_ARG;
  }
  return c->client.StartStream();
}

MES_API mes_poll_result_t mes_client_poll(mes_client_t* c) {
  mes_poll_result_t out = {};
  if (c == nullptr) {
    out.error = MES_ERR_NULL_ARG;
    return out;
  }

  auto result = c->client.Poll();
  out.error = result.error;
  out.data = result.data;
  out.size = result.size;
  out.is_heartbeat = result.is_heartbeat ? 1 : 0;
  return out;
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

MES_API const char* mes_client_last_error(mes_client_t* c) {
  if (c == nullptr) {
    return "";
  }
  return c->client.GetLastError();
}

MES_API const char* mes_client_current_gtid(mes_client_t* c) {
  if (c == nullptr) {
    return "";
  }
  return c->client.GetCurrentGtid();
}

}  // extern "C"
