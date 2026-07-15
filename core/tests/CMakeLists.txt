include(FetchContent)
FetchContent_Declare(
  googletest
  GIT_REPOSITORY https://github.com/google/googletest.git
  GIT_TAG v1.14.0
)
FetchContent_MakeAvailable(googletest)

include(GoogleTest)

function(mes_add_gtest target source)
  add_executable(${target} ${source})
  target_link_libraries(${target} PRIVATE mes-core GTest::gtest_main ${ARGN})
  target_compile_options(${target} PRIVATE -fno-exceptions -fno-rtti)
  gtest_discover_tests(${target} DISCOVERY_TIMEOUT 30)
endfunction()

# test_types
mes_add_gtest(test_types test_types.cpp)

# test_binary_util
mes_add_gtest(test_binary_util test_binary_util.cpp)

# test_event_header
mes_add_gtest(test_event_header test_event_header.cpp)

# test_table_map
mes_add_gtest(test_table_map test_table_map.cpp)

# test_rotate_event
mes_add_gtest(test_rotate_event test_rotate_event.cpp)

# test_state_machine
mes_add_gtest(test_state_machine test_state_machine.cpp)

# test_row_decoder
mes_add_gtest(test_row_decoder test_row_decoder.cpp ZLIB::ZLIB)

# test_cdc_engine
mes_add_gtest(test_cdc_engine test_cdc_engine.cpp)

# test_capi
mes_add_gtest(test_capi test_capi.cpp)

# test_gtid_encoder
mes_add_gtest(test_gtid_encoder test_gtid_encoder.cpp)

# test_mysql_packet
mes_add_gtest(test_mysql_packet test_mysql_packet.cpp)

# test_mysql_auth
mes_add_gtest(test_mysql_auth test_mysql_auth.cpp)

# test_mysql_query
mes_add_gtest(test_mysql_query test_mysql_query.cpp)

# test_mysql_binlog_stream
mes_add_gtest(test_mysql_binlog_stream test_mysql_binlog_stream.cpp)

# test_event_queue
mes_add_gtest(test_event_queue test_event_queue.cpp)

# test_transaction_gtid_tracker
mes_add_gtest(test_transaction_gtid_tracker test_transaction_gtid_tracker.cpp)

# test_connection_validator
mes_add_gtest(test_connection_validator test_connection_validator.cpp)

# test_crc32
mes_add_gtest(test_crc32 test_crc32.cpp ZLIB::ZLIB)

# test_logger
mes_add_gtest(test_logger test_logger.cpp)

# test_mariadb_gtid
mes_add_gtest(test_mariadb_gtid test_mariadb_gtid.cpp)

# test_mariadb_event_parser
mes_add_gtest(test_mariadb_event_parser test_mariadb_event_parser.cpp)

add_subdirectory(e2e)
