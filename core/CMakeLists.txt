cmake_minimum_required(VERSION 3.20)

# Options
option(MES_ENABLE_ASAN "Enable AddressSanitizer" OFF)
option(MES_ENABLE_TSAN "Enable ThreadSanitizer" OFF)
option(MES_ENABLE_COVERAGE "Enable code coverage" OFF)

# Core static library
add_library(mes-core STATIC
  src/binary_util.cpp
  src/event_header.cpp
  src/table_map.cpp
  src/rotate_event.cpp
  src/state_machine.cpp
  src/row_decoder.cpp
  src/cdc_engine.cpp
  src/capi.cpp
  src/logger.cpp
  src/client/gtid_encoder.cpp
)

target_include_directories(mes-core PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/include
  ${CMAKE_CURRENT_SOURCE_DIR}/src
)

target_compile_options(mes-core PRIVATE
  -fno-exceptions
  -fno-rtti
  -Wall
  -Wextra
  -Wpedantic
)

# Shared library for C ABI
add_library(mes-shared SHARED
  src/binary_util.cpp
  src/event_header.cpp
  src/table_map.cpp
  src/rotate_event.cpp
  src/state_machine.cpp
  src/row_decoder.cpp
  src/cdc_engine.cpp
  src/capi.cpp
  src/logger.cpp
  src/client/gtid_encoder.cpp
)

target_include_directories(mes-shared PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/include
  ${CMAKE_CURRENT_SOURCE_DIR}/src
)

target_compile_options(mes-shared PRIVATE
  -fno-exceptions
  -fno-rtti
  -Wall
  -Wextra
  -Wpedantic
)

target_compile_definitions(mes-shared PRIVATE MES_BUILDING)

set_target_properties(mes-shared PROPERTIES
  OUTPUT_NAME mes
  C_VISIBILITY_PRESET hidden
  CXX_VISIBILITY_PRESET hidden
  VISIBILITY_INLINES_HIDDEN ON
)

# Sanitizers
if(MES_ENABLE_ASAN)
  target_compile_options(mes-core PUBLIC -fsanitize=address -fno-omit-frame-pointer)
  target_link_options(mes-core PUBLIC -fsanitize=address)
  target_compile_options(mes-shared PUBLIC -fsanitize=address -fno-omit-frame-pointer)
  target_link_options(mes-shared PUBLIC -fsanitize=address)
endif()

if(MES_ENABLE_TSAN)
  target_compile_options(mes-core PUBLIC -fsanitize=thread)
  target_link_options(mes-core PUBLIC -fsanitize=thread)
  target_compile_options(mes-shared PUBLIC -fsanitize=thread)
  target_link_options(mes-shared PUBLIC -fsanitize=thread)
endif()

if(MES_ENABLE_COVERAGE)
  target_compile_options(mes-core PUBLIC --coverage)
  target_link_options(mes-core PUBLIC --coverage)
  target_compile_options(mes-shared PUBLIC --coverage)
  target_link_options(mes-shared PUBLIC --coverage)
endif()

# OpenSSL (required for protocol layer)
# Static linking for self-contained wheel/binary distribution
option(MES_OPENSSL_STATIC "Link OpenSSL statically" OFF)
if(MES_OPENSSL_STATIC)
  set(OPENSSL_USE_STATIC_LIBS TRUE)
endif()
find_package(OpenSSL REQUIRED)
message(STATUS "OpenSSL found: ${OPENSSL_VERSION} (static=${MES_OPENSSL_STATIC})")

# Protocol layer (MySQL wire protocol implementation)
set(PROTOCOL_SOURCES
  src/protocol/mysql_socket.cpp
  src/protocol/mysql_packet.cpp
  src/protocol/mysql_auth.cpp
  src/protocol/mysql_connection.cpp
  src/protocol/mysql_query.cpp
  src/protocol/mysql_binlog_stream.cpp
)

# BinlogClient and related client code
set(CLIENT_SOURCES
  src/client/connection_validator.cpp
  src/client/binlog_client.cpp
  src/client/capi_client.cpp
  src/client/metadata_fetcher.cpp
  src/client/event_queue.cpp
)

# Add to both targets unconditionally
target_sources(mes-core PRIVATE ${PROTOCOL_SOURCES} ${CLIENT_SOURCES})
target_link_libraries(mes-core PRIVATE OpenSSL::SSL OpenSSL::Crypto)

target_sources(mes-shared PRIVATE ${PROTOCOL_SOURCES} ${CLIENT_SOURCES})
target_link_libraries(mes-shared PRIVATE OpenSSL::SSL OpenSSL::Crypto)

if(WIN32)
  target_link_libraries(mes-core PRIVATE ws2_32)
  target_link_libraries(mes-shared PRIVATE ws2_32)
endif()

# Install
include(GNUInstallDirs)
install(TARGETS mes-shared
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
)
install(FILES include/mes.h DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

# Tests
enable_testing()
add_subdirectory(tests)
