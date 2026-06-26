// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

#ifndef MES_NODE_MES_ERROR_UTIL_H_
#define MES_NODE_MES_ERROR_UTIL_H_

#include <napi.h>

#include <string>

#include "mes.h"

namespace mes_node {

/**
 * @brief Stable category name for a C-ABI error code.
 *
 * Exposed as the thrown Error's `name` so JavaScript callers can branch on a
 * category without substring-matching the message.
 */
inline const char* MesErrorName(mes_error_t code) {
  switch (code) {
    case MES_ERR_CHECKSUM:
      return "MesChecksumError";
    case MES_ERR_DECODE:
    case MES_ERR_DECODE_COLUMN:
    case MES_ERR_DECODE_ROW:
      return "MesDecodeError";
    case MES_ERR_PARSE:
      return "MesParseError";
    case MES_ERR_CONNECT:
      return "MesConnectError";
    case MES_ERR_AUTH:
      return "MesAuthError";
    case MES_ERR_VALIDATION:
      return "MesValidationError";
    case MES_ERR_STREAM:
      return "MesStreamError";
    case MES_ERR_DISCONNECTED:
      return "MesDisconnectedError";
    case MES_ERR_QUEUE_FULL:
      return "MesQueueFullError";
    default:
      return "MesError";
  }
}

/**
 * @brief Build a Napi::Error carrying the numeric `code` and category `name`.
 *
 * Lets JavaScript distinguish error categories programmatically
 * (`err.code === 401`, `err.name === 'MesAuthError'`) instead of relying on
 * brittle message-string matching. The numeric `code` mirrors the C ABI
 * mes_error_t value.
 */
inline Napi::Error MakeMesError(Napi::Env env, const std::string& message, mes_error_t code) {
  Napi::Error err = Napi::Error::New(env, message);
  err.Set("code", Napi::Number::New(env, static_cast<double>(code)));
  err.Set("name", Napi::String::New(env, MesErrorName(code)));
  return err;
}

}  // namespace mes_node

#endif  // MES_NODE_MES_ERROR_UTIL_H_
