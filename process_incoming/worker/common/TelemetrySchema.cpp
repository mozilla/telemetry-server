/* -*- Mode: C++; tab-width: 8; indent-tabs-mode: nil; c-basic-offset: 2 -*- */
/* vim: set ts=2 et sw=2 tw=80: */
/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

/// @brief Telemetry schema implementation @file

#include "TelemetrySchema.h"

#include <boost/lexical_cast.hpp>
#include <boost/xpressive/xpressive.hpp>
#include <fstream>
#include <rapidjson/document.h>
#include <sstream>

using namespace std;
namespace mozilla {
namespace telemetry {

const string TelemetrySchema::kOther("other");

////////////////////////////////////////////////////////////////////////////////
TelemetrySchema::TelemetryDimension::TelemetryDimension(const RapidjsonValue& aValue)
{
  namespace bx = boost::xpressive;
  const RapidjsonValue& fn = aValue["field_name"];
  if (!fn.IsString()) {
    throw runtime_error("missing field_name element");
  }
  mName = fn.GetString();

  const RapidjsonValue& av = aValue["allowed_values"];
  switch (av.GetType()) {
  case rapidjson::kStringType:
    mType = kValue;
    mValue = av.GetString();
    break;
  case rapidjson::kArrayType:
    mType = kSet;
    for (RapidjsonValue::ConstValueIterator it = av.Begin(); it != av.End();
         ++it) {
      if (!it->IsString()) {
        throw runtime_error("allowed_values must be strings");
      }
      mSet.insert(it->GetString());
    }
    break;
  case rapidjson::kObjectType:
    {
      mType = kRange;
      const RapidjsonValue& mn = av["min"];
      if (!mn.IsNumber()) {
        throw runtime_error("allowed_values range is missing min element");
      }
      const RapidjsonValue& mx = av["max"];
      if (!mx.IsNumber()) {
        throw runtime_error("allowed_values range is missing max element");
      }
      mRange.first = mn.GetDouble();
      mRange.second = mx.GetDouble();
    }
    break;
  default:
    throw runtime_error("invalid allowed_values element");
    break;
  }
}

////////////////////////////////////////////////////////////////////////////////
TelemetrySchema::TelemetrySchema(const boost::filesystem::path& aFilename)
{
  ifstream ifs(aFilename.c_str());
  if (!ifs) {
    stringstream ss;
    ss << "file open failed: " << aFilename.string();
    throw runtime_error(ss.str());
  }
  string json((istream_iterator<char>(ifs)), istream_iterator<char>());

  RapidjsonDocument doc;
  if (doc.Parse<0>(json.c_str()).HasParseError()) {
    stringstream ss;
    ss << "json parse failed: " << doc.GetParseError();
    throw runtime_error(ss.str());
  }

  const RapidjsonValue& version = doc["version"];
  if (!version.IsInt()) {
    throw runtime_error("version element is missing");
  }
  mVersion = version.GetInt();
  LoadDimensions(doc);
}

////////////////////////////////////////////////////////////////////////////////
std::string
TelemetrySchema::GetDimensionPath(const RapidjsonDocument& aDoc,
                                  uint64_t aTimestamp)
{
  const RapidjsonValue& info = aDoc["info"];
  if (!info.IsObject()) {
    throw runtime_error("info element must be an object");
  }
  string p;
  auto end = mDimensions.end();

  string separator = "";
  for (auto it = mDimensions.begin(); it != end; ++it){
    if (it + 1 == end) {
      separator = ".";
    } else if (!p.empty()) {
      separator = "/";
    }
    if (0 == (*it)->mName.compare("submission_date")) {
      char buf[9];
      time_t t = aTimestamp / 1000;
      struct tm* ptm = gmtime(&t);
      if (0 == strftime(buf, sizeof(buf), "%Y%m%d", ptm)) {
        buf[0] = 0;
      }
      ProcessStringDimension(*it, buf, separator, p);
      continue;
    }
    const RapidjsonValue& v = info[(*it)->mName.c_str()];
    if (v.IsString()) {
      ProcessStringDimension(*it, v.GetString(), separator, p);
    } else if (v.IsNumber()) {
      double dim = v.GetDouble();
      if ((*it)->mType == TelemetryDimension::kRange) {
        if (dim >= (*it)->mRange.first && dim <= (*it)->mRange.second) {
          p += separator + boost::lexical_cast<string>(dim);
        } else {
          p += separator + kOther;
        }
      } else {
        // string comparison not allowed on numbers
        ++mMetrics.mInvalidNumericDimension.mValue;
      }
    }
  }

  return p;
}

////////////////////////////////////////////////////////////////////////////////
void
TelemetrySchema::GetMetrics(message::Message& aMsg)
{
  aMsg.clear_fields();
  ConstructField(aMsg, mMetrics.mInvalidStringDimension);
  ConstructField(aMsg, mMetrics.mInvalidNumericDimension);

  mMetrics.mInvalidStringDimension.mValue = 0;
  mMetrics.mInvalidNumericDimension.mValue = 0;
}


////////////////////////////////////////////////////////////////////////////////
/// Private Member Functions
////////////////////////////////////////////////////////////////////////////////
void
TelemetrySchema::LoadDimensions(const RapidjsonDocument& aDoc)
{
  const RapidjsonValue& dimensions = aDoc["dimensions"];
  if (!dimensions.IsArray()) {
    throw runtime_error("dimensions element must be an array");
  }
  for (RapidjsonValue::ConstValueIterator it = dimensions.Begin();
       it != dimensions.End(); ++it) {
    if (!it->IsObject()) {
      throw runtime_error("dimension elemenst must be objects");
    }
    try {
      shared_ptr<TelemetryDimension> dim(new TelemetryDimension(*it));
      mDimensions.push_back(dim);
    }
    catch (exception& e) {
      stringstream ss;
      ss << "invalid dimension schema: " << e.what();
      throw runtime_error(ss.str());
    }
  }
}

////////////////////////////////////////////////////////////////////////////////
void
TelemetrySchema::ProcessStringDimension(const std::shared_ptr<TelemetryDimension>& aTd,
                                        const std::string& aDim,
                                        const std::string& aSeperator,
                                        std::string& aPath)
{
  switch (aTd->mType) {
  case TelemetryDimension::kValue:
    if (aTd->mValue == "*" || aTd->mValue == aDim) {
      aPath += aSeperator + SafePath(aDim);
    } else {
      aPath += aSeperator + kOther;
    }
    break;
  case TelemetryDimension::kSet:
    if (aTd->mSet.find(aDim) != aTd->mSet.end()) {
      aPath += aSeperator + SafePath(aDim);
    } else {
      aPath += aSeperator + kOther;
    }
    break;
  default:
    // range comparison not allowed on a string
    ++mMetrics.mInvalidStringDimension.mValue;
    break;
  }
}

////////////////////////////////////////////////////////////////////////////////
std::string TelemetrySchema::SafePath(const std::string& aStr)
{
  namespace bx = boost::xpressive;
  static bx::sregex clean_re = ~bx::set[bx::range('a', 'z') |
                                          bx::range('A', 'Z') |
                                          bx::range('0', '9') |
                                          '_' | '/' | '.'];
  return bx::regex_replace(aStr, clean_re, "_");
}

}
}
