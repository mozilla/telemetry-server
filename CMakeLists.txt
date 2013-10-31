# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

cmake_minimum_required(VERSION 2.8 FATAL_ERROR)
project(telemetry)
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "telemetry-server")
set(CPACK_PACKAGE_VERSION_MAJOR 0)
set(CPACK_PACKAGE_VERSION_MINOR 1)
set(CPACK_PACKAGE_VERSION_PATCH 0)

set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake")
include(mozsvc)
include(externals)

find_package (Threads)
find_package(ZLIB REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(Protobuf 2.3 REQUIRED)
find_package(Boost 1.51.0 REQUIRED 
filesystem
system
thread
unit_test_framework)

include_directories(${Boost_INCLUDE_DIRS} ${OPENSSL_INCLUDE_DIR} "${CMAKE_SOURCE_DIR}/process_incoming/worker/common")

add_subdirectory(process_incoming/worker)
