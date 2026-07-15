cmake_minimum_required(VERSION 3.20)

# Options
option(MES_ENABLE_ASAN "Enable AddressSanitizer" OFF)
option(MES_ENABLE_TSAN "Enable ThreadSanitizer" OFF)
option(MES_ENABLE_UBSAN "Enable UndefinedBehaviorSanitizer" OFF)
option(MES_ENABLE_COVERAGE "Enable code coverage" OFF)
option(MES_ENABLE_FUZZ "Build libFuzzer harnesses (requires Clang)" OFF)

# OpenSSL (required for protocol layer)
# Static linking for self-contained wheel/binary distribution
option(MES_OPENSSL_STATIC "Link OpenSSL statically" OFF)
if(MES_OPENSSL_STATIC)
  set(OPENSSL_USE_STATIC_LIBS TRUE)
endif()
find_package(OpenSSL REQUIRED)
message(STATUS "OpenSSL found: ${OPENSSL_VERSION} (static=${MES_OPENSSL_STATIC})")

# ZLIB (required for CRC32 checksum verification)
option(MES_ZLIB_STATIC "Link ZLIB statically" OFF)
if(MES_ZLIB_STATIC)
  set(ZLIB_USE_STATIC_LIBS TRUE)
  # ZLIB_USE_STATIC_LIBS was added in CMake 3.24. Restrict suffixes as well so
  # older supported CMake versions cannot silently select a shared libz.
  set(_MES_SAVED_LIBRARY_SUFFIXES ${CMAKE_FIND_LIBRARY_SUFFIXES})
  set(CMAKE_FIND_LIBRARY_SUFFIXES .a)
endif()
find_package(ZLIB REQUIRED)
if(MES_ZLIB_STATIC)
  set(CMAKE_FIND_LIBRARY_SUFFIXES ${_MES_SAVED_LIBRARY_SUFFIXES})
endif()
message(STATUS "ZLIB found: ${ZLIB_VERSION_STRING} (static=${MES_ZLIB_STATIC})")

# Linux wheels use this to avoid a host libstdc++/libgcc runtime requirement.
option(MES_PORTABLE_RUNTIME "Statically link the GNU C++ runtime into shared artifacts" OFF)

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
  src/client/transaction_gtid_tracker.cpp
)

# OBJECT library: compile sources once, reuse in static and shared targets
add_library(mes-objects OBJECT
  src/binary_util.cpp
  src/event_header.cpp
  src/mariadb_gtid.cpp
  src/mariadb_event_parser.cpp
  src/table_map.cpp
  src/rotate_event.cpp
  src/state_machine.cpp
  src/row_decoder.cpp
  src/cdc_engine.cpp
  src/capi.cpp
  src/logger.cpp
  src/client/gtid_encoder.cpp
  ${PROTOCOL_SOURCES}
  ${CLIENT_SOURCES}
)

set_target_properties(mes-objects PROPERTIES
  POSITION_INDEPENDENT_CODE ON
  C_VISIBILITY_PRESET hidden
  CXX_VISIBILITY_PRESET hidden
  VISIBILITY_INLINES_HIDDEN ON
)

target_compile_definitions(mes-objects PRIVATE MES_BUILDING)

target_include_directories(mes-objects PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/include
  ${CMAKE_CURRENT_SOURCE_DIR}/src
)

target_compile_options(mes-objects PRIVATE
  -fno-exceptions
  -fno-rtti
  -Wall
  -Wextra
  -Wpedantic
)

# OpenSSL include directories needed at compile time
target_link_libraries(mes-objects PRIVATE OpenSSL::SSL OpenSSL::Crypto ZLIB::ZLIB)

# Sanitizers (applied to OBJECT library so both static and shared inherit them)
if(MES_ENABLE_ASAN)
  target_compile_options(mes-objects PRIVATE -fsanitize=address -fno-omit-frame-pointer)
endif()

if(MES_ENABLE_TSAN)
  target_compile_options(mes-objects PRIVATE -fsanitize=thread)
endif()

if(MES_ENABLE_UBSAN)
  target_compile_options(mes-objects PRIVATE -fsanitize=undefined -fno-omit-frame-pointer)
endif()

if(MES_ENABLE_COVERAGE)
  target_compile_options(mes-objects PRIVATE --coverage)
endif()

# Core static library
add_library(mes-core STATIC $<TARGET_OBJECTS:mes-objects>)

target_include_directories(mes-core PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/include
  ${CMAKE_CURRENT_SOURCE_DIR}/src
)

target_link_libraries(mes-core PRIVATE OpenSSL::SSL OpenSSL::Crypto ZLIB::ZLIB)

# Propagate sanitizer/coverage link flags to consumers (PUBLIC required for static lib)
if(MES_ENABLE_ASAN)
  target_link_options(mes-core PUBLIC -fsanitize=address)
endif()
if(MES_ENABLE_TSAN)
  target_link_options(mes-core PUBLIC -fsanitize=thread)
endif()
if(MES_ENABLE_UBSAN)
  target_link_options(mes-core PUBLIC -fsanitize=undefined)
endif()
if(MES_ENABLE_COVERAGE)
  target_link_options(mes-core PUBLIC --coverage)
endif()

# Shared library for C ABI
add_library(mes-shared SHARED $<TARGET_OBJECTS:mes-objects>)

target_include_directories(mes-shared PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/include
  ${CMAKE_CURRENT_SOURCE_DIR}/src
)

set_target_properties(mes-shared PROPERTIES
  OUTPUT_NAME mes
  VERSION 1.5.0
  SOVERSION 1
)

target_link_libraries(mes-shared PRIVATE OpenSSL::SSL OpenSSL::Crypto ZLIB::ZLIB)

if(MES_PORTABLE_RUNTIME AND UNIX AND NOT APPLE)
  target_link_options(mes-shared PRIVATE -static-libgcc -static-libstdc++)
endif()

# Propagate sanitizer/coverage link flags to final targets
if(MES_ENABLE_ASAN)
  target_link_options(mes-shared PRIVATE -fsanitize=address)
endif()
if(MES_ENABLE_TSAN)
  target_link_options(mes-shared PRIVATE -fsanitize=thread)
endif()
if(MES_ENABLE_UBSAN)
  target_link_options(mes-shared PRIVATE -fsanitize=undefined)
endif()
if(MES_ENABLE_COVERAGE)
  target_link_options(mes-shared PRIVATE --coverage)
endif()

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

# Fuzzing harnesses (libFuzzer; Clang only). Combine with ASan/UBSan.
if(MES_ENABLE_FUZZ)
  if(NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    message(FATAL_ERROR "MES_ENABLE_FUZZ requires Clang (libFuzzer support)")
  endif()
  add_executable(fuzz_feed tests/fuzz/fuzz_feed.cpp)
  target_link_libraries(fuzz_feed PRIVATE mes-core)
  target_compile_options(fuzz_feed PRIVATE -fsanitize=fuzzer)
  target_link_options(fuzz_feed PRIVATE -fsanitize=fuzzer)
endif()

# Tests are disabled for wheel/native artifact builds with BUILD_TESTING=OFF.
if(BUILD_TESTING)
  enable_testing()
  add_subdirectory(tests)
endif()
