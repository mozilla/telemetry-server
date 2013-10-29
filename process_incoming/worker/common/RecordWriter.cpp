/* -*- Mode: C++; tab-width: 8; indent-tabs-mode: nil; c-basic-offset: 2 -*- */
/* vim: set ts=2 et sw=2 tw=80: */
/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

/// @brief RecordWriter implementation @file

#include "RecordWriter.h"

using namespace std;
namespace fs = boost::filesystem;

#include <iostream>

namespace mozilla {
namespace telemetry {


////////////////////////////////////////////////////////////////////////////////
RecordWriter::RecordWriter(boost::filesystem::path aWorkFolder,
                           boost::filesystem::path aUploadFolder,
                           uint64_t aMaxUncompressedSize,
                           size_t aMemoryConstraint,
                           int aCompressionPreset) :
  mWorkFolder(aWorkFolder),
  mUploadFolder(aUploadFolder),
  mMaxUncompressedSize(aMaxUncompressedSize),
  mMemoryConstraint(aMemoryConstraint),
  mCompressionPreset(aCompressionPreset)
{

}

////////////////////////////////////////////////////////////////////////////////
//void RecordWriter::Write(const boost::filesystem::path& aFilterPath,
//           const char* aRecord, size_t aLength)
//{
//  cout << (mWorkFolder / aFilterPath) << " [" << aLength << "]: " << aRecord;
//}

////////////////////////////////////////////////////////////////////////////////
void RecordWriter::Write(const boost::filesystem::path&, const char*, size_t)
{

}

////////////////////////////////////////////////////////////////////////////////
void RecordWriter::Finalize()
{

}


}
}

