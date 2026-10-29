cmake_minimum_required(VERSION 3.18)
project(mes_codec CXX)

include(${CMAKE_CURRENT_LIST_DIR}/sources.cmake)

add_library(mes_codec STATIC ${MES_CODEC_SOURCES})
add_library(mes::codec ALIAS mes_codec)

target_include_directories(mes_codec
  PUBLIC ${CMAKE_CURRENT_LIST_DIR}/include
  PRIVATE ${CMAKE_CURRENT_LIST_DIR}/src
)
target_compile_features(mes_codec PUBLIC cxx_std_17)
target_compile_options(mes_codec PRIVATE
  -fno-exceptions
  -fno-rtti
  -Wall
  -Wextra
  -Wpedantic
  -Werror=switch
)
set_target_properties(mes_codec PROPERTIES POSITION_INDEPENDENT_CODE ON)

if(NOT TARGET ZLIB::ZLIB)
  find_package(ZLIB REQUIRED)
endif()
target_link_libraries(mes_codec PRIVATE ZLIB::ZLIB)
