/* -*- Mode: C++; tab-width: 8; indent-tabs-mode: nil; c-basic-offset: 2 -*- */
/* vim: set ts=2 et sw=2 tw=80: */
/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

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

} // namespace Utils
} // namespace mozilla
