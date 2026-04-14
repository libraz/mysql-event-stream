include(FetchContent)
FetchContent_Declare(
  googletest
  GIT_REPOSITORY https://github.com/google/googletest.git
  GIT_TAG v1.14.0
)
FetchContent_MakeAvailable(googletest)

include(GoogleTest)

# test_types
add_executable(test_types test_types.cpp)
target_link_libraries(test_types PRIVATE mes-core GTest::gtest_main)
target_compile_options(test_types PRIVATE -fno-exceptions -fno-rtti)
gtest_discover_tests(test_types)

# test_binary_util
add_executable(test_binary_util test_binary_util.cpp)
target_link_libraries(test_binary_util PRIVATE mes-core GTest::gtest_main)
target_compile_options(test_binary_util PRIVATE -fno-exceptions -fno-rtti)
gtest_discover_tests(test_binary_util)

# test_event_header
add_executable(test_event_header test_event_header.cpp)
target_link_libraries(test_event_header PRIVATE mes-core GTest::gtest_main)
target_compile_options(test_event_header PRIVATE -fno-exceptions -fno-rtti)
gtest_discover_tests(test_event_header)

# test_table_map
add_executable(test_table_map test_table_map.cpp)
target_link_libraries(test_table_map PRIVATE mes-core GTest::gtest_main)
target_compile_options(test_table_map PRIVATE -fno-exceptions -fno-rtti)
gtest_discover_tests(test_table_map)

# test_rotate_event
add_executable(test_rotate_event test_rotate_event.cpp)
target_link_libraries(test_rotate_event PRIVATE mes-core GTest::gtest_main)
target_compile_options(test_rotate_event PRIVATE -fno-exceptions -fno-rtti)
gtest_discover_tests(test_rotate_event)

# test_state_machine
add_executable(test_state_machine test_state_machine.cpp)
target_link_libraries(test_state_machine PRIVATE mes-core GTest::gtest_main)
target_compile_options(test_state_machine PRIVATE -fno-exceptions -fno-rtti)
gtest_discover_tests(test_state_machine)

# test_row_decoder
add_executable(test_row_decoder test_row_decoder.cpp)
target_link_libraries(test_row_decoder PRIVATE mes-core GTest::gtest_main)
target_compile_options(test_row_decoder PRIVATE -fno-exceptions -fno-rtti)
gtest_discover_tests(test_row_decoder)

# test_cdc_engine
add_executable(test_cdc_engine test_cdc_engine.cpp)
target_link_libraries(test_cdc_engine PRIVATE mes-core GTest::gtest_main)
target_compile_options(test_cdc_engine PRIVATE -fno-exceptions -fno-rtti)
gtest_discover_tests(test_cdc_engine)

# test_capi
add_executable(test_capi test_capi.cpp)
target_link_libraries(test_capi PRIVATE mes-core GTest::gtest_main)
target_compile_options(test_capi PRIVATE -fno-exceptions -fno-rtti)
gtest_discover_tests(test_capi)

# test_gtid_encoder
add_executable(test_gtid_encoder test_gtid_encoder.cpp)
target_link_libraries(test_gtid_encoder PRIVATE mes-core GTest::gtest_main)
target_compile_options(test_gtid_encoder PRIVATE -fno-exceptions -fno-rtti)
gtest_discover_tests(test_gtid_encoder)

# test_mysql_packet
add_executable(test_mysql_packet test_mysql_packet.cpp)
target_link_libraries(test_mysql_packet PRIVATE mes-core GTest::gtest_main)
target_compile_options(test_mysql_packet PRIVATE -fno-exceptions -fno-rtti)
gtest_discover_tests(test_mysql_packet)

# test_mysql_auth
add_executable(test_mysql_auth test_mysql_auth.cpp)
target_link_libraries(test_mysql_auth PRIVATE mes-core GTest::gtest_main)
target_compile_options(test_mysql_auth PRIVATE -fno-exceptions -fno-rtti)
gtest_discover_tests(test_mysql_auth)

# test_mysql_query
add_executable(test_mysql_query test_mysql_query.cpp)
target_link_libraries(test_mysql_query PRIVATE mes-core GTest::gtest_main)
target_compile_options(test_mysql_query PRIVATE -fno-exceptions -fno-rtti)
gtest_discover_tests(test_mysql_query)

# test_mysql_binlog_stream
add_executable(test_mysql_binlog_stream test_mysql_binlog_stream.cpp)
target_link_libraries(test_mysql_binlog_stream PRIVATE mes-core GTest::gtest_main)
target_compile_options(test_mysql_binlog_stream PRIVATE -fno-exceptions -fno-rtti)
gtest_discover_tests(test_mysql_binlog_stream)

# test_event_queue
add_executable(test_event_queue test_event_queue.cpp)
target_link_libraries(test_event_queue PRIVATE mes-core GTest::gtest_main)
target_compile_options(test_event_queue PRIVATE -fno-exceptions -fno-rtti)
gtest_discover_tests(test_event_queue)

# test_crc32
add_executable(test_crc32 test_crc32.cpp)
target_link_libraries(test_crc32 PRIVATE mes-core ZLIB::ZLIB GTest::gtest_main)
target_compile_options(test_crc32 PRIVATE -fno-exceptions -fno-rtti)
gtest_discover_tests(test_crc32)

# test_logger
add_executable(test_logger test_logger.cpp)
target_link_libraries(test_logger PRIVATE mes-core GTest::gtest_main)
target_compile_options(test_logger PRIVATE -fno-exceptions -fno-rtti)
gtest_discover_tests(test_logger)

# test_mariadb_gtid
add_executable(test_mariadb_gtid test_mariadb_gtid.cpp)
target_link_libraries(test_mariadb_gtid PRIVATE mes-core GTest::gtest_main)
target_compile_options(test_mariadb_gtid PRIVATE -fno-exceptions -fno-rtti)
gtest_discover_tests(test_mariadb_gtid)

# test_mariadb_event_parser
add_executable(test_mariadb_event_parser test_mariadb_event_parser.cpp)
target_link_libraries(test_mariadb_event_parser PRIVATE mes-core GTest::gtest_main)
target_compile_options(test_mariadb_event_parser PRIVATE -fno-exceptions -fno-rtti)
gtest_discover_tests(test_mariadb_event_parser)

add_subdirectory(e2e)
