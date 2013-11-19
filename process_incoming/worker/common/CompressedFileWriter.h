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

#include "Utils.h"

namespace mozilla {

/**
 * Compressed Wrapper class that writes data to an compressed XZ file
 * This is essentially LZMA2, XZ docs says not use LZMA1 unless you know what
 * you're doing.
 */
class CompressedFileWriter
{
public:
  /** Create CompressedFileWriter */
  CompressedFileWriter(FILE* aFile);

  /**
   * Initialize CompressedFileWriter given an LZMA compression level, a number
   * between 0 and 9.
   * See preset option in xz(1) for more details.
   */
  bool Initialize(uint32_t preset = 0);

  /** Write buffer to compressed file */
  bool Write(const char* aBuffer, size_t aLength);

  /** Size of data added to compressed file */
  uint64_t UncompressedSize() const;

  /** Size of compressed file so far */
  uint64_t CompressedSize() const;

  /** Finalize compression */
  bool Finalize();

  ~CompressedFileWriter();
private:
  FILE* mFile;
  lzma_stream* mStream;
  char* mBuffer;
};

} // namespace mozilla

#endif // CompressedFileWriter_h
