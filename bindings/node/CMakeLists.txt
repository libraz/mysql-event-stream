# Copyright 2024 mysql-event-stream Authors
# SPDX-License-Identifier: Apache-2.0

cmake_minimum_required(VERSION 3.20)
project(mes-node LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Release prebuilds bundle these libraries so an installed addon never relies
# on a package-manager OpenSSL/ZLIB or a particular Linux C++ runtime.
option(MES_BUNDLE_DEPENDENCIES "Statically link OpenSSL, ZLIB, and the GNU C++ runtime" OFF)

# node-addon-api include path
execute_process(
  COMMAND node -p "require('node-addon-api').include"
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
  OUTPUT_VARIABLE NAPI_INCLUDE_DIR
  OUTPUT_STRIP_TRAILING_WHITESPACE
)
# Strip quotes from the output
string(REPLACE "\"" "" NAPI_INCLUDE_DIR ${NAPI_INCLUDE_DIR})

# Core library sources (build inline as static)
set(MES_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../..)
set(CORE_SOURCES
  ${MES_ROOT}/core/src/binary_util.cpp
  ${MES_ROOT}/core/src/event_header.cpp
  ${MES_ROOT}/core/src/table_map.cpp
  ${MES_ROOT}/core/src/rotate_event.cpp
  ${MES_ROOT}/core/src/state_machine.cpp
  ${MES_ROOT}/core/src/row_decoder.cpp
  ${MES_ROOT}/core/src/cdc_engine.cpp
  ${MES_ROOT}/core/src/capi.cpp
  ${MES_ROOT}/core/src/logger.cpp
  ${MES_ROOT}/core/src/mariadb_gtid.cpp
  ${MES_ROOT}/core/src/mariadb_event_parser.cpp
  ${MES_ROOT}/core/src/client/gtid_encoder.cpp
  ${MES_ROOT}/core/src/client/connection_validator.cpp
  ${MES_ROOT}/core/src/client/binlog_client.cpp
  ${MES_ROOT}/core/src/client/capi_client.cpp
  ${MES_ROOT}/core/src/client/metadata_fetcher.cpp
  ${MES_ROOT}/core/src/client/event_queue.cpp
  ${MES_ROOT}/core/src/client/transaction_gtid_tracker.cpp
  ${MES_ROOT}/core/src/protocol/mysql_socket.cpp
  ${MES_ROOT}/core/src/protocol/mysql_packet.cpp
  ${MES_ROOT}/core/src/protocol/mysql_auth.cpp
  ${MES_ROOT}/core/src/protocol/mysql_connection.cpp
  ${MES_ROOT}/core/src/protocol/mysql_query.cpp
  ${MES_ROOT}/core/src/protocol/mysql_binlog_stream.cpp
)

# Addon sources
set(ADDON_SOURCES
  src/addon/addon.cpp
  src/addon/engine_wrap.cpp
  src/addon/client_wrap.cpp
  src/addon/log_callback.cpp
)

# OpenSSL (required for protocol layer)
if(MES_BUNDLE_DEPENDENCIES)
  set(OPENSSL_USE_STATIC_LIBS TRUE)
  set(ZLIB_USE_STATIC_LIBS TRUE)
  # ZLIB_USE_STATIC_LIBS was added in CMake 3.24. Restrict suffixes as well so
  # older supported CMake versions cannot silently select a shared libz.
  set(_MES_SAVED_LIBRARY_SUFFIXES ${CMAKE_FIND_LIBRARY_SUFFIXES})
  set(CMAKE_FIND_LIBRARY_SUFFIXES .a)
endif()
find_package(OpenSSL REQUIRED)
find_package(ZLIB REQUIRED)
if(MES_BUNDLE_DEPENDENCIES)
  set(CMAKE_FIND_LIBRARY_SUFFIXES ${_MES_SAVED_LIBRARY_SUFFIXES})
endif()

# Build the addon
add_library(${PROJECT_NAME} SHARED
  ${CORE_SOURCES}
  ${ADDON_SOURCES}
  ${CMAKE_JS_SRC}
)

set_target_properties(${PROJECT_NAME} PROPERTIES
  PREFIX ""
  SUFFIX ".node"
)

target_include_directories(${PROJECT_NAME} PRIVATE
  ${CMAKE_JS_INC}
  ${NAPI_INCLUDE_DIR}
  ${MES_ROOT}/core/include
  ${MES_ROOT}/core/src
)

target_compile_definitions(${PROJECT_NAME} PRIVATE
  NAPI_DISABLE_CPP_EXCEPTIONS
  MES_BUILDING
)

target_compile_options(${PROJECT_NAME} PRIVATE
  -fno-exceptions
  -fno-rtti
  -Wall
  -Wextra
  -Wpedantic
)

target_link_libraries(${PROJECT_NAME} PRIVATE ${CMAKE_JS_LIB} OpenSSL::SSL OpenSSL::Crypto ZLIB::ZLIB)

if(MES_BUNDLE_DEPENDENCIES AND UNIX AND NOT APPLE)
  target_link_options(${PROJECT_NAME} PRIVATE -static-libgcc -static-libstdc++)
endif()

if(WIN32)
  target_link_libraries(${PROJECT_NAME} PRIVATE ws2_32)
endif()
