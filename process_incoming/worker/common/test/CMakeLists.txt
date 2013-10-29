# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

add_executable(TestHistogramSpecification TestHistogramSpecification.cpp)
target_link_libraries(TestHistogramSpecification telemetry ${Boost_UNIT_TEST_FRAMEWORK_LIBRARY})
ADD_TEST(TestHistogramSpecification TestHistogramSpecification)

add_executable(TestHistogramCache TestHistogramCache.cpp)
target_link_libraries(TestHistogramCache telemetry ${Boost_UNIT_TEST_FRAMEWORK_LIBRARY})
ADD_TEST(TestHistogramCache TestHistogramCache)

add_executable(TestHistogramConverter TestHistogramConverter.cpp)
target_link_libraries(TestHistogramConverter telemetry ${Boost_UNIT_TEST_FRAMEWORK_LIBRARY})
ADD_TEST(TestHistogramConverter TestHistogramConverter)

add_executable(TestTelemetryRecord TestTelemetryRecord.cpp)
target_link_libraries(TestTelemetryRecord telemetry ${Boost_UNIT_TEST_FRAMEWORK_LIBRARY})
ADD_TEST(TestTelemetryRecord TestTelemetryRecord)

add_executable(TestTelemetrySchema TestTelemetrySchema.cpp)
target_link_libraries(TestTelemetrySchema telemetry ${Boost_UNIT_TEST_FRAMEWORK_LIBRARY})
ADD_TEST(TestTelemetrySchema TestTelemetrySchema)

configure_file (${CMAKE_CURRENT_SOURCE_DIR}/TestConfig.in.h ${CMAKE_CURRENT_BINARY_DIR}/TestConfig.h)
include_directories(${CMAKE_CURRENT_BINARY_DIR})
