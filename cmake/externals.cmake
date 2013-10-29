# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

include(ExternalProject)
set_property(DIRECTORY PROPERTY EP_BASE "${CMAKE_BINARY_DIR}/ep_base")
find_program(PATCH_EXECUTABLE patch)
if (NOT PATCH_EXECUTABLE)
   message(FATAL_ERROR "patch not found")
endif()

externalproject_add(
    rapidjson-0_11
    URL http://rapidjson.googlecode.com/files/rapidjson-0.11.zip
    URL_MD5 96a4b1b57ece8bc6a807ceb14ccaaf94
    PATCH_COMMAND ${PATCH_EXECUTABLE} -p1 < ${CMAKE_CURRENT_LIST_DIR}/rapidjson-0_11.patch
    CONFIGURE_COMMAND ""
    BUILD_COMMAND ""
    INSTALL_COMMAND ""
)

set(RAPIDJSON_INCLUDE_DIRS "${CMAKE_BINARY_DIR}/ep_base/Source/rapidjson-0_11/include")
include_directories(${RAPIDJSON_INCLUDE_DIRS})
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -isystem ${RAPIDJSON_INCLUDE_DIRS}")

