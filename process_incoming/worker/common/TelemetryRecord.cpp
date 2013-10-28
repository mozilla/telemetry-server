/* -*- Mode: C++; tab-width: 8; indent-tabs-mode: nil; c-basic-offset: 2 -*- */
/* vim: set ts=2 et sw=2 tw=80: */
/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

/// @brief Telemetry record implementation @file

#include "HistogramSpecification.h"
#include "TelemetryConstants.h"
#include "TelemetryRecord.h"

#include <boost/lexical_cast.hpp>
#include <cstring>
#include <iostream>
#include <string>
#include <zlib.h>

using namespace std;

namespace mozilla {
namespace telemetry {

///////////////////////////////////////////////////////////////////////////////
TelemetryRecord::TelemetryRecord() :
  mPathLength(0),
  mPathSize(kMaxTelemetryPath),
  mPath(nullptr),

  mDataLength(0),
  mDataSize(kMaxTelemetryData),
  mData(nullptr),

  mTimestamp(0),

  mInflateLength(0),
  mInflateSize(kMaxTelemetryData),
  mInflate(nullptr)
{
  mPath = new char[mPathSize + 1];
  mData = new char[mDataSize + 1];
  mInflate = new char[mInflateSize];
}

////////////////////////////////////////////////////////////////////////////////
TelemetryRecord::~TelemetryRecord()
{
  delete[] mPath;
  delete[] mData;
  delete[] mInflate;
}

////////////////////////////////////////////////////////////////////////////////
bool TelemetryRecord::Read(std::istream& aInput)
{
  while (aInput) {
    if (FindRecord(aInput)) {
      if (!aInput.read(mPath, mPathLength).good()) {
        return false;
      }
      mPath[mPathLength] = 0;

      if (!aInput.read(mData, mDataLength).good()) {
        return false;
      }
      mData[mDataLength] = 0;
      if (ProcessRecord()) return true;
    }
  }
  return false;
}

////////////////////////////////////////////////////////////////////////////////
const char* TelemetryRecord::GetPath()
{
  return mPath;
}

////////////////////////////////////////////////////////////////////////////////
uint64_t TelemetryRecord::GetTimestamp()
{
  return mTimestamp;
}

////////////////////////////////////////////////////////////////////////////////
RapidjsonDocument& TelemetryRecord::GetDocument()
{
  return mDocument;
}

////////////////////////////////////////////////////////////////////////////////
void
TelemetryRecord::GetMetrics(message::Message& aMsg)
{
  aMsg.clear_fields();
  ConstructField(aMsg, mMetrics.mInvalidPathLength);
  ConstructField(aMsg, mMetrics.mInvalidDataLength);
  ConstructField(aMsg, mMetrics.mInflateFailures);
  ConstructField(aMsg, mMetrics.mParseFailures);
  ConstructField(aMsg, mMetrics.mCorruptData);

  mMetrics.mInvalidPathLength.mValue = 0;
  mMetrics.mInvalidDataLength.mValue = 0;
  mMetrics.mInflateFailures.mValue = 0;
  mMetrics.mParseFailures.mValue = 0;
  mMetrics.mCorruptData.mValue = 0;
}

////////////////////////////////////////////////////////////////////////////////
/// Private Members
////////////////////////////////////////////////////////////////////////////////
bool TelemetryRecord::FindRecord(std::istream& aInput)
{
  while (aInput.good()) {
    char ch;
    aInput.get(ch);
    if (ch == kRecordSeparator) {
      streampos pos = aInput.tellg();
      if (ReadHeader(aInput)) {
        return true;
      }
      if (!aInput) {
        return false;
      }
      aInput.seekg(pos); // reset back to where the bad header starts
    } else {
      ++mMetrics.mCorruptData.mValue;
    }
  }
  return false;
}

////////////////////////////////////////////////////////////////////////////////
bool TelemetryRecord::ReadHeader(std::istream& aInput)
{
  // todo support conversion to big endian if necessary
  if (!read_value(aInput, mPathLength).good()) return false;
  if (mPathLength > kMaxTelemetryPath) {
    ++mMetrics.mInvalidPathLength.mValue;
    return false;
  }

  if (!read_value(aInput, mDataLength).good()) return false;
  if (mDataLength > kMaxTelemetryData) {
    ++mMetrics.mInvalidDataLength.mValue;
    return false;
  }

  if (!read_value(aInput, mTimestamp).good()) return false;
  return true;
}

////////////////////////////////////////////////////////////////////////////////
bool TelemetryRecord::ProcessRecord()
{
  if (mDataLength > 2 && mData[0] == 0x1f
      && static_cast<unsigned char>(mData[1]) == 0x8b) {
    int ret = Inflate();
    if (ret != Z_OK) {
      ++mMetrics.mInflateFailures.mValue;
      return false;
    } else {
      if (mInflateLength < mInflateSize) {
        mInflate[mInflateLength] = 0;
      } else {
        size_t required = mInflateLength + 1; // make room for the null
        char* tmp = new char[required];
        if (tmp) {
          memcpy(tmp, mInflate, mInflateLength);
          delete[] mInflate;
          mInflate = tmp;
          mInflateSize = required;
          mInflate[mInflateLength] = 0;
        }
      }
      mDocument.ParseInsitu<0>(mInflate); // destructively parse
    }
  } else {
    mDocument.ParseInsitu<0>(mData); // destructively parse
  }
  if (mDocument.HasParseError()) {
    ++mMetrics.mParseFailures.mValue;
    return false;
  }
  return true;
}

////////////////////////////////////////////////////////////////////////////////
int TelemetryRecord::Inflate()
{
  z_stream strm;
  strm.zalloc = Z_NULL;
  strm.zfree = Z_NULL;
  strm.opaque = Z_NULL;
  strm.avail_in = mDataLength;
  strm.next_in = reinterpret_cast<unsigned char*>(mData);
  strm.avail_out = mInflateSize;
  strm.next_out = reinterpret_cast<unsigned char*>(mInflate);

  int ret = inflateInit2(&strm, 16 + MAX_WBITS);
  if (ret != Z_OK) {
    return ret;
  }

  do {
    if (ret == Z_BUF_ERROR) {
      size_t required = mInflateSize * 2;
      char* tmp = new char[required];
      if (tmp) {
        memcpy(tmp, mInflate, mInflateLength);
        delete[] mInflate;
        mInflate = tmp;
        mInflateSize = required;
        strm.avail_out = mInflateSize - mInflateLength;
        strm.next_out = reinterpret_cast<unsigned char*>(mInflate +
                                                         mInflateLength);
      } else {
        break;
      }
    }
    ret = inflate(&strm, Z_FINISH);
    mInflateLength = mInflateSize - strm.avail_out;
  }
  while (ret == Z_BUF_ERROR);

  inflateEnd(&strm);
  return ret == Z_STREAM_END ? Z_OK : Z_DATA_ERROR;
}

}
}
