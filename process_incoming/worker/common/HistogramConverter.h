/* -*- Mode: C++; tab-width: 8; indent-tabs-mode: nil; c-basic-offset: 2 -*- */
/* vim: set ts=2 et sw=2 tw=80: */
/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

/// Histogram data converter. @file

#ifndef mozilla_telemetry_Histogram_Converter_h
#define mozilla_telemetry_Histogram_Converter_h

#include "HistogramCache.h"

#include <rapidjson/document.h>

namespace mozilla {
namespace telemetry {

bool ConvertHistogramData(HistogramCache& aCache, RapidjsonDocument& aDoc);

}
}

#endif // mozilla_telemetry_Histogram_Converter_h
