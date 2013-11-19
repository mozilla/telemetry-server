/* -*- Mode: C++; tab-width: 8; indent-tabs-mode: nil; c-basic-offset: 2 -*- */
/* vim: set ts=2 et sw=2 tw=80: */
/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

#ifndef RecordWriter_h
#define RecordWriter_h

#include <string>
#include <unordered_map>

#include "Utils.h"

namespace mozilla {

namespace telemetry {

/**
 * RecordWriter is responsible for writing records into temporary files stored
 * in mWorkFolder, compressing and moving them to mUploadFolder, when the files
 * grow large enough or RecordWriter::Finalize() is called.
 * The RecordWrite will attempt to do on-the-fly compression of as many files
 * especially commonly used files as much as possible. Whilst providing the
 * desired compression preset and respecting soft memory limits,
 */
class RecordWriter
{
  class OutputFile;
  typedef std::unordered_map<std::string, OutputFile*> FileMap;
public:
  /**
   * Create a RecordWriter
   * @param aWorkFolder           work folder for temporary files
   * @param aUploadFolder         destination folder for files to be uploaded
   * @param aMaxUncompressedSize  maximum number of bytes before rotating file
   * @param aSoftMemoryLimit      a soft limit on memory usage
   * @param aCompressionPreset    desired compression level ranges from 0 to 9
   *
   * Remark: aSoftMemoryLimit will be used to determine the number of files that
   * can be compressed on-the-fly, it maybe violated.
   */
  RecordWriter(const std::string& aWorkFolder, const std::string& aUploadFolder,
               uint64_t aMaxUncompressedSize, size_t aSoftMemoryLimit,
               uint32_t aCompressionPreset);

  /** Write record with given filter path */
  bool Write(const std::string& aPath, const char* aRecord, size_t aLength);

  /** Close all open files, compress and move to upload folder */
  bool Finalize();

  /** Get absolute path to work folder, ending with a slash */
  const std::string& WorkFolder() const
  {
    return mWorkFolder;
  }

  /** Get absolute path to upload folder, ending with a slash */
  const std::string& UploadFolder() const
  {
    return mUploadFolder;
  }

  /** Get compression preset */
  uint32_t CompressionPreset() const
  {
    return mCompressionPreset;
  }

  /** Get max uncompressed size allowed */
  uint64_t MaxUncompressedSize() const
  {
    return mMaxUncompressedSize;
  }

  /** Get a new UUID */
  const std::string& GetUUID()
  {
    return mUUIDSource.GetUUID();
  }

private:
  std::string mWorkFolder;
  std::string mUploadFolder;
  uint64_t    mMaxUncompressedSize;
  size_t      mSoftMemoryLimit;
  uint32_t    mCompressionPreset;
  FileMap     mFileMap;
  size_t      mRecordsSinceLastReprioritization;
  Utils::UUIDGenerator mUUIDSource;

  /** Reprioritize on-the-fly compression */
  bool ReprioritizeCompression();
};

} // namespace Telemetry
} // namespace mozilla

#endif // RecordWriter_h
