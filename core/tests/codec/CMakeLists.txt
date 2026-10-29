# Codec unit tests: every test_codec_*.cpp joins one executable.
file(GLOB MES_CODEC_TEST_SOURCES CONFIGURE_DEPENDS ${CMAKE_CURRENT_LIST_DIR}/test_codec_*.cpp)

add_executable(test_codec ${MES_CODEC_TEST_SOURCES})
target_link_libraries(test_codec PRIVATE mes::codec GTest::gtest_main)
target_include_directories(test_codec PRIVATE ${CMAKE_CURRENT_LIST_DIR}/..)
target_compile_options(test_codec PRIVATE -fno-exceptions -fno-rtti)
gtest_discover_tests(test_codec DISCOVERY_TIMEOUT 30 PROPERTIES TIMEOUT 10)
