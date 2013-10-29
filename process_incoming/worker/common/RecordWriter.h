/* -*- Mode: C++; tab-width: 8; indent-tabs-mode: nil; c-basic-offset: 2 -*- */
/* vim: set ts=2 et sw=2 tw=80: */
/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

/** @file 
The record writer partition and compresses the data preparing it for upload to 
the data warehouse. 
 */

#ifndef mozilla_telemetry_Record_Writer_h
#define mozilla_telemetry_Record_Writer_h

#include <boost/filesystem.hpp>
#include <cstdint>

namespace mozilla {
namespace telemetry {

/**
 * 
 * Class that manages compression and moves to aUploadFolder when large enough
 * 
 */
class RecordWriter
{
public:
  /**
   * Constructor
   * 
   * @param aWorkFolder Path where the data is partitioned and comperessed.
   * @param aUploadFolder Path containing the fully processed data waiting to be
   *                      pushed to S3.
   * @param aMaxUncompressedSize Maximum size of the record before compression.
   * @param aMemoryConstraint Approximate contraint, determines number of
   *                         contexts kept alive.
   * @param aCompressionPreset The level of compression to apply to each record.
   * 
   */
  RecordWriter(boost::filesystem::path aWorkFolder,
               boost::filesystem::path aUploadFolder,
               uint64_t aMaxUncompressedSize,
               size_t aMemoryConstraint,
               int aCompressionPreset);

  /**
   * Write aRecord to file in aFilterPath subfolder of aWorkFolder
   * 
   * @param aFilterPath Path computed from the telemetry schema and histogram
   *                    data.
   * @param aRecord Converted JSON histogram record.
   * @param aLength Number of bytes in the record.
   */
  void Write(const boost::filesystem::path& aFilterPath, 
             const char* aRecord, size_t aLength);

  /**
   * Compress all files and move them to aUploadFolder
   */
  void Finalize();

private:
  boost::filesystem::path mWorkFolder;
  boost::filesystem::path mUploadFolder;
  uint64_t mMaxUncompressedSize;
  size_t mMemoryConstraint;
  int mCompressionPreset;
};

}
}

#endif // mozilla_telemetry_RecordWriter_h
