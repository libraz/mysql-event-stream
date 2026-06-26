// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

/**
 * @file server_flavor.h
 * @brief MySQL/MariaDB server flavor detection
 *
 * Provides enumeration and detection of database server flavor
 * from the VERSION() string returned by the server.
 */

#ifndef MES_SERVER_FLAVOR_H_
#define MES_SERVER_FLAVOR_H_

#include <cstdint>
#include <string>

namespace mes {

/** @brief Database server flavor. */
enum class ServerFlavor : uint8_t {
  kMySQL,    ///< MySQL (including Percona Server)
  kMariaDB,  ///< MariaDB
};

/**
 * @brief Get human-readable name for server flavor.
 * @param flavor Server flavor enum value.
 * @return Static string with the flavor name.
 */
inline const char* GetServerFlavorName(ServerFlavor flavor) {
  switch (flavor) {
    case ServerFlavor::kMySQL:
      return "MySQL";
    case ServerFlavor::kMariaDB:
      return "MariaDB";
  }
  return "Unknown";  // Unreachable, satisfies compiler warning
}

/**
 * @brief Detect server flavor from VERSION() string.
 *
 * MariaDB's VERSION() contains "MariaDB"
 * (e.g., "10.11.6-MariaDB", "11.4.0-MariaDB-1:11.4.0+maria~ubu2404").
 * MySQL's VERSION() is purely numeric (e.g., "8.4.7", "9.0.1").
 *
 * @note Fallback behavior: when @p version_string is empty or does not contain
 *       a recognizable "MariaDB" marker, this returns ServerFlavor::kMySQL. The
 *       version string is normally never empty (it comes from the handshake
 *       packet's NUL-terminated server-version field), but a malformed or
 *       stripped handshake could leave it empty; in that ambiguous case we
 *       default to MySQL, which uses the more strict GTID/checksum negotiation
 *       and the wider-deployed wire dialect. Callers that record the detection
 *       should log the ambiguity when the input is empty.
 *
 * @param version_string Result of SELECT VERSION() (or the handshake
 *                       server-version field).
 * @return Detected server flavor; ServerFlavor::kMySQL when undetectable.
 */
inline ServerFlavor DetectServerFlavor(const std::string& version_string) {
  if (version_string.find("MariaDB") != std::string::npos ||
      version_string.find("mariadb") != std::string::npos) {
    return ServerFlavor::kMariaDB;
  }
  return ServerFlavor::kMySQL;
}

}  // namespace mes

#endif  // MES_SERVER_FLAVOR_H_
