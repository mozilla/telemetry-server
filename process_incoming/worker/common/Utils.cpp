#include <sys/stat.h>
#include <errno.h>

#include "Utils.h"

namespace {

#define DEFAULT_MODE      S_IRWXU | S_IRGRP |  S_IXGRP | S_IROTH | S_IXOTH

/** Utility function to create directory tree */
bool mkdirp(const char* path, mode_t mode = DEFAULT_MODE) {
  // Invalid string
  if(path[0] == '\0') {
    return false;
  }

  // const cast for hack
  char* p = const_cast<char*>(path);

  // Find next slash mkdir() it and until we're at end of string
  while (*p != '\0') {
    // Skip first character
    p++;

    // Find first slash or end
    while(*p != '\0' && *p != '/') p++;

    // Remember value from p
    char v = *p;

    // Write end of string at p
    *p = '\0';

    // Create folder from path to '\0' inserted at p
    if(mkdir(path, mode) != 0 && errno != EEXIST) {
      *p = v;
      return false;
    }

    // Restore path to it's former glory
    *p = v;
  }

  return true;
}

} // namespace anonymous

namespace mozilla {
namespace Utils {

bool EnsurePath(const std::string& aPath) {
  return mkdirp(aPath.c_str());
}


UUIDGenerator::UUIDGenerator()
 : mGenerator(),
 mDistribution(0, 15),
 mUUID(36, '0')
{
  std::random_device rd;
  mGenerator.seed(rd());
}

const std::string& UUIDGenerator::GetUUID()
{
  int i = 0;
  while(i < 36) {
    char rand = mDistribution(mGenerator);
    mUUID[i++] = rand + (rand < 10 ? 48 : 97);
  }
 
  // UUID4 format conformance 
  mUUID[8] = '-';
  mUUID[13] = '-';
  mUUID[14] = '4';
  mUUID[18] = '-';
  mUUID[19] = (mUUID[19] | 0x8) & ~ 0x4;
  mUUID[23] = '-';

  return mUUID;
}

} // namespace Utils
} // namespace mozilla
