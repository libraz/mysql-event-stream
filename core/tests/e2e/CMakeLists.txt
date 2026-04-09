# E2E protocol tests (requires running MySQL 8.4+ or 9.x at localhost:13307)

add_executable(test_e2e_protocol test_e2e_protocol.cpp)
target_link_libraries(test_e2e_protocol PRIVATE mes-core GTest::gtest_main)
target_compile_options(test_e2e_protocol PRIVATE -fno-exceptions -fno-rtti)
target_compile_definitions(test_e2e_protocol PRIVATE MES_PROJECT_ROOT="${CMAKE_SOURCE_DIR}")
gtest_discover_tests(test_e2e_protocol)

add_executable(test_e2e_auth test_e2e_auth.cpp)
target_link_libraries(test_e2e_auth PRIVATE mes-core GTest::gtest_main)
target_compile_options(test_e2e_auth PRIVATE -fno-exceptions -fno-rtti)
target_compile_definitions(test_e2e_auth PRIVATE MES_PROJECT_ROOT="${CMAKE_SOURCE_DIR}")
gtest_discover_tests(test_e2e_auth)

add_executable(test_e2e_ssl test_e2e_ssl.cpp)
target_link_libraries(test_e2e_ssl PRIVATE mes-core GTest::gtest_main)
target_compile_options(test_e2e_ssl PRIVATE -fno-exceptions -fno-rtti)
target_compile_definitions(test_e2e_ssl PRIVATE MES_PROJECT_ROOT="${CMAKE_SOURCE_DIR}")
gtest_discover_tests(test_e2e_ssl)

add_executable(test_e2e_binlog_dml test_e2e_binlog_dml.cpp)
target_link_libraries(test_e2e_binlog_dml PRIVATE mes-core GTest::gtest_main)
target_compile_options(test_e2e_binlog_dml PRIVATE -fno-exceptions -fno-rtti)
target_compile_definitions(test_e2e_binlog_dml PRIVATE MES_PROJECT_ROOT="${CMAKE_SOURCE_DIR}")
gtest_discover_tests(test_e2e_binlog_dml)

add_executable(test_e2e_binlog_ddl test_e2e_binlog_ddl.cpp)
target_link_libraries(test_e2e_binlog_ddl PRIVATE mes-core GTest::gtest_main)
target_compile_options(test_e2e_binlog_ddl PRIVATE -fno-exceptions -fno-rtti)
target_compile_definitions(test_e2e_binlog_ddl PRIVATE MES_PROJECT_ROOT="${CMAKE_SOURCE_DIR}")
gtest_discover_tests(test_e2e_binlog_ddl)

add_executable(test_e2e_engine test_e2e_engine.cpp)
target_link_libraries(test_e2e_engine PRIVATE mes-core GTest::gtest_main)
target_compile_options(test_e2e_engine PRIVATE -fno-exceptions -fno-rtti)
target_compile_definitions(test_e2e_engine PRIVATE MES_PROJECT_ROOT="${CMAKE_SOURCE_DIR}")
gtest_discover_tests(test_e2e_engine)

# test_e2e_buffer
add_executable(test_e2e_buffer test_e2e_buffer.cpp)
target_link_libraries(test_e2e_buffer PRIVATE mes-core GTest::gtest_main)
target_compile_options(test_e2e_buffer PRIVATE -fno-exceptions -fno-rtti)
target_compile_definitions(test_e2e_buffer PRIVATE MES_PROJECT_ROOT="${CMAKE_SOURCE_DIR}")
gtest_discover_tests(test_e2e_buffer)
