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
 * @param version_string Result of SELECT VERSION().
 * @return Detected server flavor.
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
