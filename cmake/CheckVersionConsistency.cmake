# Fail configuration when the release version drifts between the files that declare it.
#
# Four files state the version independently and nothing else compares them, so a
# forgotten bump used to ship a library whose mes_version() reported the previous
# release. The C header is the reference for the compiled library — mes_version()
# is built from its macros — and this check ties it to the CMake project version
# and to both binding manifests.
#
# Binding manifests are checked only when present, so a source tree containing
# just the core still configures.

set(_mes_header "${PROJECT_SOURCE_DIR}/core/include/mes.h")
file(READ "${_mes_header}" _mes_header_text)

foreach(_component MAJOR MINOR PATCH)
  string(REGEX MATCH "#define MES_VERSION_${_component}[ \t]+([0-9]+)" _mes_matched
         "${_mes_header_text}")
  if(_mes_matched STREQUAL "")
    message(FATAL_ERROR "MES_VERSION_${_component} not found in ${_mes_header}")
  endif()
  set(_mes_${_component} "${CMAKE_MATCH_1}")
endforeach()

set(_mes_header_version "${_mes_MAJOR}.${_mes_MINOR}.${_mes_PATCH}")
if(NOT _mes_header_version STREQUAL PROJECT_VERSION)
  message(FATAL_ERROR
    "Version mismatch: core/include/mes.h declares ${_mes_header_version} but the "
    "CMake project declares ${PROJECT_VERSION}")
endif()

set(_mes_node_manifest "${PROJECT_SOURCE_DIR}/bindings/node/package.json")
if(EXISTS "${_mes_node_manifest}")
  file(READ "${_mes_node_manifest}" _mes_node_text)
  string(JSON _mes_node_version GET "${_mes_node_text}" version)
  if(NOT _mes_node_version STREQUAL PROJECT_VERSION)
    message(FATAL_ERROR
      "Version mismatch: bindings/node/package.json declares ${_mes_node_version} "
      "but the CMake project declares ${PROJECT_VERSION}")
  endif()
endif()

set(_mes_python_manifest "${PROJECT_SOURCE_DIR}/bindings/python/pyproject.toml")
if(EXISTS "${_mes_python_manifest}")
  file(READ "${_mes_python_manifest}" _mes_python_text)
  # Anchored at line start so target-version and python_version cannot match.
  string(REGEX MATCH "(^|\n)version[ \t]*=[ \t]*\"([^\"]+)\"" _mes_matched
         "${_mes_python_text}")
  if(_mes_matched STREQUAL "")
    message(FATAL_ERROR "version not found in ${_mes_python_manifest}")
  endif()
  if(NOT CMAKE_MATCH_2 STREQUAL PROJECT_VERSION)
    message(FATAL_ERROR
      "Version mismatch: bindings/python/pyproject.toml declares ${CMAKE_MATCH_2} "
      "but the CMake project declares ${PROJECT_VERSION}")
  endif()
endif()

message(STATUS "Version consistent across header and binding manifests: ${PROJECT_VERSION}")
