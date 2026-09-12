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
    case MES_ERR_GTID_PURGED:
      return "MesGtidPurgedError";
    case MES_ERR_GTID_TAGGED_UNSUPPORTED:
      return "MesGtidTaggedUnsupportedError";
    case MES_ERR_QUEUE_FULL:
      return "MesQueueFullError";
    default:
      return "MesError";
  }
}

/**
 * @brief JavaScript class a thrown Mes error is constructed as.
 *
 * A refusal of an argument the caller just supplied keeps the built-in subclass
 * JavaScript uses for that kind of refusal, so a caller's `instanceof TypeError`
 * or `instanceof RangeError` branch holds whichever entry point received the
 * value: the TypeScript validator in `src/validation.ts` raises the same two
 * classes for the options it checks itself. Everything else — a failed
 * connection, a decode failure, an operation on a destroyed handle — is not an
 * argument refusal and stays a plain Error distinguished by `code` and `name`.
 */
enum class MesErrorClass {
  kPlain,
  /** A value whose runtime type is not the one the option or parameter takes. */
  kType,
  /** A value of the right type outside the window the option accepts. */
  kRange,
};

/** @brief Construct an empty error object of the requested JavaScript class. */
inline Napi::Error NewErrorOfClass(Napi::Env env, const std::string& message,
                                   MesErrorClass error_class) {
  switch (error_class) {
    case MesErrorClass::kType:
      return Napi::TypeError::New(env, message);
    case MesErrorClass::kRange:
      return Napi::RangeError::New(env, message);
    case MesErrorClass::kPlain:
      break;
  }
  return Napi::Error::New(env, message);
}

/**
 * @brief Build a Napi::Error carrying the numeric `code` and category `name`.
 *
 * Lets JavaScript distinguish error categories programmatically
 * (`err.code === 401`, `err.name === 'MesAuthError'`) instead of relying on
 * brittle message-string matching. The numeric `code` mirrors the C ABI
 * mes_error_t value.
 *
 * @param error_class Class to construct; see MesErrorClass. The default suits
 *   an outcome rather than a refused argument, so a new argument check has to
 *   name the subclass its refusal is presented as.
 */
inline Napi::Error MakeMesError(Napi::Env env, const std::string& message, mes_error_t code,
                                MesErrorClass error_class = MesErrorClass::kPlain) {
  Napi::Error err = NewErrorOfClass(env, message, error_class);
  err.Set("code", Napi::Number::New(env, static_cast<double>(code)));
  err.Set("name", Napi::String::New(env, MesErrorName(code)));
  return err;
}

}  // namespace mes_node

#endif  // MES_NODE_MES_ERROR_UTIL_H_
