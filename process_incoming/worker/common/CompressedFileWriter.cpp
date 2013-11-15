#include "CompressedFileWriter.h"

#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <errno.h>

namespace mozilla {

/** Buffer output buffer size, before writing to file */
#define BUF_SIZE        BUFSIZ

CompressedFileWriter::CompressedFileWriter(FILE* aFile)
  : mFile(aFile), mStream(nullptr)
{
  // Allocate internal buffer
  mBuffer = new char[BUF_SIZE];
}

bool CompressedFileWriter::Initialize(uint32_t preset)
{
  // We cannot be initialized here
  assert(mStream == nullptr);
  assert(mFile && mBuffer);

  // Create LZMA stream
  mStream = new lzma_stream;
  memset(mStream, 0, sizeof(lzma_stream));

  // Initialize encoder
  lzma_ret ret = lzma_easy_encoder(mStream, preset, LZMA_CHECK_CRC64);

  // Report errors
  if (ret != LZMA_OK) {
    // Print error code
    switch (ret) {
      case LZMA_MEM_ERROR:
      fprintf(stderr, "CompressedFileWriter::Init: "
                      "Memory allocation failed (code %u)\n", ret);
      break;
    case LZMA_OPTIONS_ERROR:
      fprintf(stderr, "CompressedFileWriter::Init: "
                      "Specified preset is not supported (code %u)\n", ret);
      break;
    case LZMA_UNSUPPORTED_CHECK:
      fprintf(stderr, "CompressedFileWriter::Init: "
                      "Specified integrity check is not supported (code %u)\n", ret);
      break;
    default:
      fprintf(stderr, "CompressedFileWriter::Init: "
                      "Unknown error, possibly a bug (code %u)\n", ret);
      break;
    }

    // Abort initialization
    return false;
  }

  // Setup mStream
  mStream->next_in    = NULL;
  mStream->avail_in   = 0;
  mStream->next_out   = (uint8_t*)mBuffer;
  mStream->avail_out  = BUF_SIZE;

  // Initialization successful
  return true;
}

bool CompressedFileWriter::Write(const char* aBuffer, size_t aLength)
{
  // We must have initialized here
  assert(mStream != nullptr);
  assert(mFile && mBuffer);

  // Set bytes to encode
  mStream->next_in = (uint8_t*)aBuffer;
  mStream->avail_in = aLength;

  // While there is bytes to encode
  while (mStream->avail_in != 0) {

    // Output buffer shouldn't be full
    assert(mStream->avail_out != 0);

    // Encode bytes
    lzma_ret ret = lzma_code(mStream, LZMA_RUN);

    // Handle errors
    if (ret != LZMA_OK) {
      switch (ret) {
        case LZMA_STREAM_END:
          fprintf(stderr, "CompressedFileWriter::Write: "
                          "Unexpected LZMA stream end!\n");
          break;
        case LZMA_MEM_ERROR:
          fprintf(stderr, "CompressedFileWriter::Write: "
                          "Memory allocation failed (code %u)\n", ret);
          break;
        case LZMA_DATA_ERROR:
          fprintf(stderr, "CompressedFileWriter::Write: "
                          "File size limits exceeded (code %u)\n", ret);
          break;
        default:
          fprintf(stderr, "CompressedFileWriter::Write: "
                          "Unknown error, possibly a bug (code %u)\n", ret);
          break;
      }
      return false;
    }

    // If there's no more output buffer space
    if (mStream->avail_out == 0) {

      // Write to file
      if (fwrite(mBuffer, 1, BUF_SIZE, mFile) != BUF_SIZE) {
        fprintf(stderr, "CompressedFileWriter::Write: fwrite failed: %s\n",
                        strerror(errno));
        return false;
      }

      // Reuse output buffer
      mStream->next_out = (uint8_t*)mBuffer;
      mStream->avail_out = BUF_SIZE;
    }
  }

  // There's no more data for encoding
  assert(mStream->avail_in == 0);

  // Encoding was successful
  return true;
}

uint64_t CompressedFileWriter::UncompressedSize() const
{
  // We must have initialized here
  assert(mStream != nullptr);
  assert(mFile && mBuffer);

  // Return total compressed size
  return mStream->total_in;
}


uint64_t CompressedFileWriter::CompressedSize() const
{
  // We must have initialized here
  assert(mStream != nullptr);
  assert(mFile && mBuffer);

  // Return total compressed size
  return mStream->total_out;
}


bool CompressedFileWriter::Finalize()
{
  // We must have initialized here
  assert(mStream != nullptr);
  assert(mFile && mBuffer);

  // We shouldn't be providing any input here
  assert(mStream->avail_in == 0);

  // Keep encoding with no new data until end of stream
  lzma_ret ret;
  do {

    // Finish LZMA encoding
    ret = lzma_code(mStream, LZMA_FINISH);

    // Handle errors
    if (ret != LZMA_OK && ret != LZMA_STREAM_END) {
      switch (ret) {
        case LZMA_MEM_ERROR:
          fprintf(stderr, "CompressedFileWriter::Finalize: "
                          "Memory allocation failed (code %u)\n", ret);
          break;
        case LZMA_DATA_ERROR:
          fprintf(stderr, "CompressedFileWriter::Finalize: "
                          "File size limits exceeded (code %u)\n", ret);
          break;
        default:
          fprintf(stderr, "CompressedFileWriter::Finalize: "
                          "Unknown error, possibly a bug (code %u)\n", ret);
          break;
      }
      return false;
    }

    // Write output buffer if full or we're at the end of the LZMA stream
    if (mStream->avail_out == 0 || ret == LZMA_STREAM_END) {

      size_t outsize = BUF_SIZE - mStream->avail_out;

      // Write to file
      if (fwrite(mBuffer, 1, outsize, mFile) != outsize) {
        fprintf(stderr, "CompressedFileWriter::Finalize: fwrite failed: %s\n",
                        strerror(errno));
        return false;
      }

      // Reuse output buffer (there maybe more stuck in the internal buffer)
      mStream->next_out = (uint8_t*)mBuffer;
      mStream->avail_out = BUF_SIZE;
    }

    // We continue with this until we reach the end of the internal LZMA stream
  } while (ret != LZMA_STREAM_END);


  // Free underlying encoder
  lzma_end(mStream);

  // Release LZMA stream
  delete mStream;
  mStream = nullptr;

  return true;
}

CompressedFileWriter::~CompressedFileWriter()
{
  // We should finalize before destruction
  assert(mStream == nullptr);
  assert(mFile && mBuffer);

  // Free internal buffer
  delete mBuffer;
  mBuffer = nullptr;
}

} // namespace mozilla