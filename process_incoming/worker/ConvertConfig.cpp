/* -*- Mode: C++; tab-width: 8; indent-tabs-mode: nil; c-basic-offset: 2 -*- */
/* vim: set ts=2 et sw=2 tw=80: */
/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

/// @brief Converter configuration implementation @file

#include "Common.h"
#include "ConvertConfig.h"

#include <fstream>
#include <istream>
#include <sstream>
#include <rapidjson/document.h>

using namespace std;

namespace mozilla {
namespace telemetry {

///////////////////////////////////////////////////////////////////////////////
void ReadConfig(const char* aFile, ConvertConfig& aConfig)
{
  ifstream ifs(aFile);
  if (!ifs) {
    stringstream ss;
    ss << "file open failed: " << aFile;
    throw runtime_error(ss.str());
  }
  string json((istream_iterator<char>(ifs)), istream_iterator<char>());

  RapidjsonDocument doc;
  if (doc.Parse<0>(json.c_str()).HasParseError()) {
    stringstream ss;
    ss << "json parse failed: " << doc.GetParseError();
    throw runtime_error(ss.str());
  }

  RapidjsonValue& heka = doc["heka_server"];
  if (!heka.IsString()) {
    throw runtime_error("heka_server not specified");
  }
  aConfig.mHekaServer = heka.GetString();

  RapidjsonValue& ts = doc["telemetry_schema"];
  if (!ts.IsString()) {
    throw runtime_error("telemetry_schema not specified");
  }
  aConfig.mTelemetrySchema = ts.GetString();

  RapidjsonValue& hs = doc["histogram_server"];
  if (!hs.IsString()) {
    throw runtime_error("histogram_server not specified");
  }
  aConfig.mHistogramServer = hs.GetString();

  RapidjsonValue& sp = doc["storage_path"];
  if (!sp.IsString()) {
    throw runtime_error("storage_path not specified");
  }
  aConfig.mStoragePath = sp.GetString();
  if (!exists(aConfig.mStoragePath)) {
    create_directories(aConfig.mStoragePath);
  }

  RapidjsonValue& up = doc["upload_path"];
  if (!up.IsString()) {
    throw runtime_error("upload_path not specified");
  }
  aConfig.mUploadPath = up.GetString();
  if (!exists(aConfig.mUploadPath)) {
    create_directories(aConfig.mUploadPath);
  }

  RapidjsonValue& mu = doc["max_uncompressed"];
  if (!mu.IsUint64()) {
    throw runtime_error("max_uncompressed not specified");
  }
  aConfig.mMaxUncompressed = mu.GetUint64();

  RapidjsonValue& mc = doc["memory_constraint"];
  if (!mc.IsUint()) {
    throw runtime_error("memory_constraint not specified");
  }
  aConfig.mMemoryConstraint = mc.GetUint();

  RapidjsonValue& cpr = doc["compression_preset"];
  if (!cpr.IsInt()) {
    throw runtime_error("compression_preset not specified");
  }
  aConfig.mCompressionPreset = cpr.GetInt();
}

}
}
