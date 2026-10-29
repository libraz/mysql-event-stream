# The GIT_TAG pin below is the single source of truth for the shared codec version.
include(FetchContent)

if(NOT TARGET mybinlog::mybinlog)
  if(NOT "$ENV{MYBINLOG_SOURCE_DIR}" STREQUAL "" AND NOT DEFINED FETCHCONTENT_SOURCE_DIR_LIBMYBINLOG)
    set(FETCHCONTENT_SOURCE_DIR_LIBMYBINLOG "$ENV{MYBINLOG_SOURCE_DIR}" CACHE PATH
        "Local libmybinlog checkout")
  endif()
  FetchContent_Declare(libmybinlog
    GIT_REPOSITORY https://github.com/libraz/libmybinlog.git
    GIT_TAG 717db77389ced67bd18bd6263a1f059fc196a981
  )
  FetchContent_MakeAvailable(libmybinlog)
endif()
