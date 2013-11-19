/* -*- Mode: C++; tab-width: 8; indent-tabs-mode: nil; c-basic-offset: 2 -*- */
/* vim: set ts=2 et sw=2 tw=80: */
/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

#ifndef Utils_h
#define Utils_h

#include <string>
#include <random>

namespace mozilla {
namespace Utils {

bool EnsurePath(const std::string& aPath);

/** Simple class for generation UUIDs version 4 */
class UUIDGenerator
{
public:
  UUIDGenerator();

  /** Get a UUID */
  const std::string& GetUUID();

private:
  std::mt19937 mGenerator;
  std::uniform_int_distribution<char> mDistribution;
  std::string mUUID;
};

} // namespace Utils
} // namespace mozilla

#endif // Utils_h
