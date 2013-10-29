/* -*- Mode: C++; tab-width: 8; indent-tabs-mode: nil; c-basic-offset: 2 -*- */
/* vim: set ts=2 et sw=2 tw=80: */
/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

/// Mozilla Telemetry global constants @file

#ifndef mozilla_telemetry_Telemetry_Constants_h
#define mozilla_telemetry_Telemetry_Constants_h

#include <string>

namespace mozilla {
namespace telemetry { 
extern const unsigned kVersionMajor;
extern const unsigned kVersionMinor;
extern const unsigned kVersionPatch;

extern const std::string kProgramName;
extern const std::string kProgramDescription;

extern const size_t kMaxTelemetryPath;
extern const size_t kMaxTelemetryData;

extern const char kRecordSeparator;
extern const char kUnitSeparator;

extern const size_t kExtraBucketsSize;
extern const char* kExtraBuckets[];
}
}

#endif // mozilla_telemetry_Telemetry_Constants_h
