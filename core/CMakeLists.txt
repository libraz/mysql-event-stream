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

# BinlogClient (auto-detected, requires libmysqlclient)
find_package(PkgConfig QUIET)
if(PkgConfig_FOUND)
  pkg_check_modules(MYSQL QUIET mysqlclient)
endif()

if(NOT MYSQL_FOUND)
  # macOS Homebrew fallback
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
  message(STATUS "MySQL client library: ${MYSQL_LIBRARIES}")

  set(CLIENT_SOURCES
    src/client/connection_validator.cpp
    src/client/binlog_client.cpp
    src/client/capi_client.cpp
    src/client/metadata_fetcher.cpp
  )

  target_sources(mes-core PRIVATE ${CLIENT_SOURCES})
  target_compile_definitions(mes-core PUBLIC MES_HAS_MYSQL)
  target_include_directories(mes-core SYSTEM PRIVATE ${MYSQL_INCLUDE_DIRS})
  target_link_libraries(mes-core PRIVATE ${MYSQL_LIBRARIES})

  target_sources(mes-shared PRIVATE ${CLIENT_SOURCES})
  target_compile_definitions(mes-shared PUBLIC MES_HAS_MYSQL)
  target_include_directories(mes-shared SYSTEM PRIVATE ${MYSQL_INCLUDE_DIRS})
  target_link_libraries(mes-shared PRIVATE ${MYSQL_LIBRARIES})
else()
  message(WARNING
    "libmysqlclient not found — BinlogClient will not be available.\n"
    "Install: brew install mysql-client@8.4 (macOS) or apt install libmysqlclient-dev (Linux)")
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
