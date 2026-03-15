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
  ${MES_ROOT}/core/src/client/gtid_encoder.cpp
)

# Addon sources
set(ADDON_SOURCES
  src/addon/addon.cpp
  src/addon/engine_wrap.cpp
)

# Auto-detect MySQL client library for BinlogClient
find_package(PkgConfig QUIET)
if(PkgConfig_FOUND)
  pkg_check_modules(MYSQL QUIET mysqlclient)
endif()

if(NOT MYSQL_FOUND)
  execute_process(
    COMMAND brew --prefix mysql-client@8.4
    OUTPUT_VARIABLE MYSQL_BREW_PREFIX
    OUTPUT_STRIP_TRAILING_WHITESPACE
    ERROR_QUIET
    RESULT_VARIABLE BREW_RESULT
  )
  if(BREW_RESULT EQUAL 0)
    set(MYSQL_BREW_INCLUDE "${MYSQL_BREW_PREFIX}/include/mysql")
    set(MYSQL_BREW_LIB "${MYSQL_BREW_PREFIX}/lib")
  endif()

  find_path(MYSQL_INCLUDE_DIRS mysql.h
    HINTS
      ${MYSQL_BREW_INCLUDE}
      /opt/homebrew/opt/mysql-client@8.4/include/mysql
      /opt/homebrew/opt/mysql-client/include/mysql
      /usr/local/opt/mysql-client/include/mysql
      /opt/homebrew/include/mysql
      /usr/local/include/mysql
      /usr/include/mysql
  )
  find_library(MYSQL_LIBRARY NAMES mysqlclient
    HINTS
      ${MYSQL_BREW_LIB}
      /opt/homebrew/opt/mysql-client@8.4/lib
      /opt/homebrew/opt/mysql-client/lib
      /usr/local/opt/mysql-client/lib
      /opt/homebrew/lib
      /usr/local/lib
      /usr/lib
      /usr/lib64
      /usr/lib/x86_64-linux-gnu
  )
  if(MYSQL_INCLUDE_DIRS AND MYSQL_LIBRARY)
    set(MYSQL_FOUND TRUE)
    set(MYSQL_LIBRARIES ${MYSQL_LIBRARY})
  endif()
endif()

if(MYSQL_FOUND)
  message(STATUS "MySQL client found: ${MYSQL_INCLUDE_DIRS}")

  list(APPEND CORE_SOURCES
    ${MES_ROOT}/core/src/client/connection_validator.cpp
    ${MES_ROOT}/core/src/client/binlog_client.cpp
    ${MES_ROOT}/core/src/client/capi_client.cpp
    ${MES_ROOT}/core/src/client/metadata_fetcher.cpp
  )
  list(APPEND ADDON_SOURCES
    src/addon/client_wrap.cpp
  )
else()
  message(WARNING
    "libmysqlclient not found — BinlogClient will not be available.\n"
    "Install: brew install mysql-client@8.4 (macOS) or apt install libmysqlclient-dev (Linux)")
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
)

target_link_libraries(${PROJECT_NAME} PRIVATE ${CMAKE_JS_LIB})

if(MYSQL_FOUND)
  target_compile_definitions(${PROJECT_NAME} PRIVATE MES_HAS_MYSQL)
  target_include_directories(${PROJECT_NAME} SYSTEM PRIVATE ${MYSQL_INCLUDE_DIRS})
  target_link_libraries(${PROJECT_NAME} PRIVATE ${MYSQL_LIBRARIES})
endif()
