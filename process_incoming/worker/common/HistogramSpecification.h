/* -*- Mode: C++; tab-width: 8; indent-tabs-mode: nil; c-basic-offset: 2 -*- */
/* vim: set ts=2 et sw=2 tw=80: */
/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

/** @file
Accessor and utility functions for the Histogram.json data structure.
 */

#ifndef mozilla_telemetry_HistogramSpecification_h
#define mozilla_telemetry_HistogramSpecification_h

#include "Common.h"

#include <boost/functional/hash_fwd.hpp>
#include <boost/utility.hpp>
#include <functional>
#include <map>
#include <memory>
#include <rapidjson/document.h>
#include <unordered_map>
#include <vector>

namespace mozilla {
namespace telemetry {

/**
 * Stores a specific histogram definition within a histogram file
 *
 */
class HistogramDefinition : boost::noncopyable
{
public:

  HistogramDefinition(const RapidjsonValue& aValue);

  /**
   * Returns the index of the associated bucket based on the bucket's lower
   * bound.
   *
   * @param aLowerBound The lower bound of the data stored in this bucket
   *
   * @return int The bucket index or -1 if the lower bound is invalid.
   */
  int GetBucketIndex(long aLowerBound) const;

  /**
   * Returns the number of counter buckets in the definition.
   *
   * @return int Number of buckets.
   */
  int GetBucketCount() const;

private:
  int mKind;
  int mMin;
  int mMax;
  int mBucketCount;
  std::unordered_map<int, int> mBuckets;
};

inline int HistogramDefinition::GetBucketCount() const
{
  return mBucketCount;
}

struct Cstring_equal_to : std::binary_function<char*, char*, bool>
{
  bool operator ()(const char* p, const char* q) const
  {
    return std::strcmp(p, q) == 0;
  }
};

struct Cstring_hash : std::unary_function<char*, std::size_t>
{
  std::size_t operator ()(const char* p) const
  {
    std::size_t seed = 0;
    for (const char* i = p; *i != 0; ++i) {
      boost::hash_combine(seed, *i);
    }
    return seed;
  }
};

/**
 * Stores the set of histogram definitions within a histogram file.
 *
 */
class HistogramSpecification : boost::noncopyable
{
public:
  /**
   * Loads the specified Histogram.json into memory.
   *
   * @param aJSON JSON histogram data.
   *
   * @return
   *
   */
  HistogramSpecification(const std::string& aJSON);
  ~HistogramSpecification();


  /**
   * Retrieve a specific histogram definition by name.
   *
   * @param aName Histogram name.
   *
   * @return HistogramDefinition Histogram definition or nullptr if the
   * definition is not found.
   */
  const HistogramDefinition* GetDefinition(const char* aName) const;

private:

  /**
   * Loads the histogram definitions/verifies the schema
   *
   * @param aValue "histograms" object from the JSON document.
   *
   */
  void LoadDefinitions(const RapidjsonDocument& aDoc);

  std::unordered_map<char*, HistogramDefinition*, Cstring_hash,
                     Cstring_equal_to> mDefinitions;
};

}
}

#endif // mozilla_telemetry_HistogramSpecification_h
