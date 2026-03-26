# Copyright 2024 mysql-event-stream Authors
# SPDX-License-Identifier: Apache-2.0

cmake_minimum_required(VERSION 3.20)
project(mes-node LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

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
  ${MES_ROOT}/core/src/client/gtid_encoder.cpp
  ${MES_ROOT}/core/src/client/connection_validator.cpp
  ${MES_ROOT}/core/src/client/binlog_client.cpp
  ${MES_ROOT}/core/src/client/capi_client.cpp
  ${MES_ROOT}/core/src/client/metadata_fetcher.cpp
  ${MES_ROOT}/core/src/client/event_queue.cpp
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
)

# OpenSSL (required for protocol layer)
find_package(OpenSSL REQUIRED)

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
)

target_link_libraries(${PROJECT_NAME} PRIVATE ${CMAKE_JS_LIB} OpenSSL::SSL OpenSSL::Crypto)

if(WIN32)
  target_link_libraries(${PROJECT_NAME} PRIVATE ws2_32)
endif()
