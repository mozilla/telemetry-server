/* -*- Mode: C++; tab-width: 8; indent-tabs-mode: nil; c-basic-offset: 2 -*- */
/* vim: set ts=2 et sw=2 tw=80: */
/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

/// @brief Prevent duplication of string constants between compilation units @file

#include "@CMAKE_CURRENT_SOURCE_DIR@/TelemetryConstants.h"

namespace mozilla {
namespace telemetry {

const unsigned kVersionMajor = @CPACK_PACKAGE_VERSION_MAJOR@;
const unsigned kVersionMinor = @CPACK_PACKAGE_VERSION_MINOR@;
const unsigned kVersionPatch = @CPACK_PACKAGE_VERSION_PATCH@;

const std::string kProgramName("@PROJECT_NAME@");
const std::string kProgramDescription("@CPACK_PACKAGE_DESCRIPTION_SUMMARY@");

const size_t kMaxTelemetryPath = 10 * 1024;
const size_t kMaxTelemetryData = 200 * 1024;

const char kRecordSeparator = 0x1e;
const char kUnitSeparator = 0x1f;

const size_t kExtraBucketsSize = 5;
const char* kExtraBuckets[] = { "sum", "log_sum", "log_sum_squares",
  "sum_squares_lo", "sum_squares_hi", nullptr };

}
}
