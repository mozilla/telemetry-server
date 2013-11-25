/* -*- Mode: C++; tab-width: 8; indent-tabs-mode: nil; c-basic-offset: 2 -*- */
/* vim: set ts=2 et sw=2 tw=80: */
/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

#include "CompressedFileWriter.h"
#include "Logger.h"

#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <errno.h>

namespace mozilla {
namespace telemetry {

CompressedFileWriter::CompressedFileWriter() : mFile(nullptr)
{
}

bool CompressedFileWriter::Initialize(FILE *aFile, uint32_t aPreset)
{
  // We cannot be initialized here
  assert(!mFile);

  mFile = aFile;

  // Create LZMA stream
  memset(&mStream, 0, sizeof(lzma_stream));

  // Initialize encoder
  lzma_ret ret = lzma_easy_encoder(&mStream, aPreset, LZMA_CHECK_CRC64);

  // Report errors
  if (ret != LZMA_OK) {
    // Print error code
    switch (ret) {
      case LZMA_MEM_ERROR:
        LOGGER(error) << "memory allocation failed (code " << ret << ")";
        break;

      case LZMA_OPTIONS_ERROR:
        LOGGER(error) << "specified preset is not supported (code " << ret << ")";
        break;
	
      case LZMA_UNSUPPORTED_CHECK:
        LOGGER(error) << "specified integrity check is not supported (code " << ret << ")";
        break;

      default:
        LOGGER(error) << "unknown error, possible bug (code " << ret << ")";
        break;
    }

    // Abort initialization
    return false;
  }

  // Setup mStream
  mStream.next_in    = NULL;
  mStream.avail_in   = 0;
  mStream.next_out   = (uint8_t*)mBuffer;
  mStream.avail_out  = BUF_SIZE;

  // Initialization successful
  return true;
}

bool CompressedFileWriter::Write(const char* aBuffer, size_t aSize, size_t *aCompressedSize)
{
  // We must have initialized here
  assert(mFile);

  if (aCompressedSize)
    *aCompressedSize = 0;

  // Set bytes to encode
  mStream.next_in = (uint8_t*)aBuffer;
  mStream.avail_in = aSize;

  // While there are bytes to encode
  while (mStream.avail_in != 0) {
    // Output buffer shouldn't be full
    assert(mStream.avail_out != 0);

    // Encode bytes
    lzma_ret ret = lzma_code(&mStream, LZMA_RUN);

    // Handle errors
    if (ret != LZMA_OK) {
      switch (ret) {
        case LZMA_STREAM_END:
          LOGGER(error) << "unexpected LZMA stream end";
          break;
        case LZMA_MEM_ERROR:
          LOGGER(error) << "memory allocation failed (code " << ret << ")";
          break;
        case LZMA_DATA_ERROR:
          LOGGER(error) << "file size limits exceeded (code " << ret << ")";
          break;
        default:
          LOGGER(error) << "unknown error, possibly a bug (code " << ret << ")";
          break;
      }
      return false;
    }

    // Write to file if output buffer is full
    if (mStream.avail_out == 0){
      if (fwrite(mBuffer, 1, BUF_SIZE, mFile) != BUF_SIZE) {
        LOGGER(error) << "fwrite failed, " << strerror(errno);
        return false;
      }

      // Reuse output buffer
      mStream.next_out = (uint8_t*)mBuffer;
      mStream.avail_out = BUF_SIZE;

      if(aCompressedSize)
        *aCompressedSize += BUF_SIZE;
    }
  }

  // There's no more data for encoding
  assert(mStream.avail_in == 0);

  // Encoding was successful
  return true;
}

bool CompressedFileWriter::Finalize(size_t *aCompressedSize)
{
  // We must have initialized here
  assert(mFile);

  // We shouldn't be providing any input here
  assert(mStream.avail_in == 0);

  if (aCompressedSize)
    *aCompressedSize = 0;

  // Keep encoding with no new data until end of stream
  lzma_ret ret;
  do {
    // Finish LZMA encoding
    ret = lzma_code(&mStream, LZMA_FINISH);

    // Handle errors
    if (ret != LZMA_OK && ret != LZMA_STREAM_END) {
      switch (ret) {
        case LZMA_MEM_ERROR:
          LOGGER(error) << "memory allocation failed (code " << ret << ")";
          break;
        case LZMA_DATA_ERROR:
          LOGGER(error) << "file size limits exceeded (code " << ret << ")";
          break;
        default:
          LOGGER(error) << "unknown error, possibly a bug (code " << ret << ")";
          break;
      }
      return false;
    }

    // Write output buffer if full or we're at the end of the LZMA stream
    if (mStream.avail_out == 0 || ret == LZMA_STREAM_END) {
      size_t outsize = BUF_SIZE - mStream.avail_out;

      // Write to file
      if (fwrite(mBuffer, 1, outsize, mFile) != outsize) {
        LOGGER(error) << "fwrite failed, " << strerror(errno);
        return false;
      }

      // Reuse output buffer (there maybe more stuck in the internal buffer)
      mStream.next_out = (uint8_t*)mBuffer;
      mStream.avail_out = BUF_SIZE;

      if(aCompressedSize)
        *aCompressedSize += outsize;
    }

    // We continue with this until we reach the end of the internal LZMA stream
  } while (ret != LZMA_STREAM_END);

  lzma_end(&mStream);
  mFile = nullptr;

  return true;
}

CompressedFileWriter::~CompressedFileWriter()
{
  // We should finalize before destruction
  if (mFile)
    Finalize();
}

} // namespace telemetry
} // namespace mozilla
