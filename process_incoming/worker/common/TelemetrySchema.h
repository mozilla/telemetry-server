/* -*- Mode: C++; tab-width: 8; indent-tabs-mode: nil; c-basic-offset: 2 -*- */
/* vim: set ts=2 et sw=2 tw=80: */
/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

/** @file 
Accessor and utility functions functions for the telemetry_schema.json data 
structure. 
*/


#ifndef mozilla_telemetry_Telemetry_Schema_h
#define mozilla_telemetry_Telemetry_Schema_h

#include "Common.h"
#include "Metric.h"

#include <boost/filesystem.hpp>
#include <memory>
#include <rapidjson/document.h>
#include <set>
#include <string>
#include <vector>

namespace mozilla {
namespace telemetry {

class TelemetrySchema
{
public:
  /**
   * Loads the specified telemetry schema json file from disk
   * 
   * @param fileName Fully qualified name of telemetry file.
   * 
   */
  TelemetrySchema(const boost::filesystem::path& fileName);

  /**
   * Constructs the storage layout path based on the configured schema and 
   * the histogram info object values. 
   * 
   * @param aDoc Histogram object.
   * 
   * @return boost::filesystem::path 
   */
  boost::filesystem::path GetDimensionPath(const RapidjsonDocument& aDoc);

  /**
   * Rolls up the internal metric data into the fields element of the provided 
   * message. The metrics are reset after each call. 
   * 
   * @param aMsg The message fields element will be cleared and then populated 
   *             with the TelemetrySchema metrics.
   */
  void GetMetrics(message::Message& aMsg);

private:

  struct Metrics {
      Metrics() :
        mInvalidStringDimension("Invalid String Dimension"),
        mInvalidNumericDimension("Invalid Numeric Dimension") { }

    Metric mInvalidStringDimension;
    Metric mInvalidNumericDimension;
  };

  struct TelemetryDimension
  {
    TelemetryDimension(const RapidjsonValue& aValue);

    enum Type {
      kValue,
      kSet,
      kRange
    };

    Type        mType;
    std::string mName;

    std::string mValue;
    std::set<std::string> mSet;
    std::pair<double, double> mRange;
  };

  /**
   * Loads the histogram definitions/verifies the schema
   * 
   * @param aValue "histograms" object from the JSON document.
   * 
   */
  void LoadDimensions(const RapidjsonDocument& aDoc);

  static std::string SafePath(const std::string& s);

  int mVersion;
  std::vector<std::shared_ptr<TelemetryDimension>> mDimensions;

  Metrics mMetrics;
};

}
}

#endif // mozilla_telemetry_Telemetry_Schema_h
