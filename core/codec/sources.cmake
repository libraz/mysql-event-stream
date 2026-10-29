# Sets MES_CODEC_SOURCES; shared by core/codec/CMakeLists.txt and the Node addon build.
file(GLOB MES_CODEC_SOURCES CONFIGURE_DEPENDS ${CMAKE_CURRENT_LIST_DIR}/src/*.cpp)
