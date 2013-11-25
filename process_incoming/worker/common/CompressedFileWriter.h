/* -*- Mode: C++; tab-width: 8; indent-tabs-mode: nil; c-basic-offset: 2 -*- */
/* vim: set ts=2 et sw=2 tw=80: */
/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

#ifndef CompressedFileWriter_h
#define CompressedFileWriter_h

#include <stdio.h>
#include <stdint.h>
#include <string>

#include <lzma.h>

/** Buffer output buffer size, before writing to file */
#define BUF_SIZE BUFSIZ

namespace mozilla {
namespace telemetry {

/**
 * Compressed Wrapper class that writes data to an compressed XZ file
 * This is essentially LZMA2, XZ docs says not use LZMA1 unless you know what
 * you're doing.
 */
class CompressedFileWriter
{
public:
  /** Create CompressedFileWriter */
  CompressedFileWriter();

  /**
   * Initialize CompressedFileWriter given an LZMA compression level, a number
   * between 0 and 9.
   * See preset option in xz(1) for more details.
   */
  bool Initialize(FILE *aFile, uint32_t aPreset = 0);

  /** Write buffer to compressed file */
  bool Write(const char* aBuffer, size_t aSize, size_t *aCompressedSize = nullptr);

  /** Finalize compression */
  bool Finalize(size_t *aCompressedSize = nullptr);

  ~CompressedFileWriter();
private:
  FILE* mFile;
  lzma_stream mStream;
  char mBuffer[BUF_SIZE];
};

} // namespace telemetry
} // namespace mozilla

#endif // CompressedFileWriter_h
