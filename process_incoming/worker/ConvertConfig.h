/* -*- Mode: C++; tab-width: 8; indent-tabs-mode: nil; c-basic-offset: 2 -*- */
/* vim: set ts=2 et sw=2 tw=80: */
/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

#ifndef mozilla_telemetry_Convert_Config_h
#define mozilla_telemetry_Convert_Config_h

#include <boost/filesystem.hpp>

namespace mozilla {
namespace telemetry {

struct ConvertConfig
{
  std::string             mHekaServer;
  std::string             mHistogramServer;
  boost::filesystem::path mTelemetrySchema;
  boost::filesystem::path mStoragePath;
  boost::filesystem::path mUploadPath;
  uint64_t                mMaxUncompressed;
  size_t                  mMemoryConstraint;
  int                     mCompressionPreset;
};

/** 
 * Loads the converter configuration from disk.
 * 
 * @param aFile Filename containing the JSON configuration.
 * @param aConfig Structure to populate with the configuration.
 */
void ReadConfig(const char* aFile, ConvertConfig& aConfig);

}
}

#endif // mozilla_telemetry_Convert_Config_h
