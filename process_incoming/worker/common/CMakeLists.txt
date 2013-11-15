# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

set(TELEMETRY_SRC
TelemetryConstants.cpp
HistogramSpecification.cpp
HistogramCache.cpp
HistogramConverter.cpp
TelemetryRecord.cpp
TelemetrySchema.cpp
RecordWriter.cpp
CompressedFileWriter.cpp
Utils.cpp
Metric.cpp
message.pb.cc
HekaLogger.cpp)

add_library(telemetry STATIC ${TELEMETRY_SRC})
add_dependencies(telemetry rapidjson-0_11)
target_link_libraries(telemetry 
${Boost_LIBRARIES}
${PROTOBUF_LIBRARIES}
${ZLIB_LIBRARIES}
${OPENSSL_LIBRARIES}
${CMAKE_THREAD_LIBS_INIT}
${LZMA_LIBRARIES})

configure_file(TelemetryConstants.in.cpp ${CMAKE_CURRENT_BINARY_DIR}/TelemetryConstants.cpp)

add_subdirectory(test)
