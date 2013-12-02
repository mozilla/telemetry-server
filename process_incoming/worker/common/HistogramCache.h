/* -*- Mode: C++; tab-width: 8; indent-tabs-mode: nil; c-basic-offset: 2 -*- */
/* vim: set ts=2 et sw=2 tw=80: */
/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

/** @file
Retrieves the requested histogram revision from cache.  If not cached checks for
and loads the histogram file from disk and adds it to the cache.
*/

#ifndef mozilla_telemetry_Histogram_Cache_h
#define mozilla_telemetry_Histogram_Cache_h

#include "HistogramSpecification.h"
#include "Metric.h"

#include <boost/filesystem.hpp>
#include <map>
#include <memory>
#include <unordered_map>
#include <vector>
#include <string>

namespace mozilla {
namespace telemetry {

class HistogramCache
{
public:
  HistogramCache(const std::string& aHistogramServer);

  /**
   * Retrieves the requested histogram revision from cache.  If not cached it
   * will attempt to load the file from the histogram server and add it to the
   * cache.
   *
   * @param aRevision RevisionKey of the histogram file to load.
   *
   * @return const Histogram* nullptr if load fails
   */
  std::shared_ptr<HistogramSpecification>
  FindHistogram(const std::string& aRevisionKey);

  /**
   * Rolls up the internal metric data into the fields element of the provided
   * message. The metrics are reset after each call.
   *
   * @param aMsg The message fields element will be cleared and then populated
   *             with the HistogramCache metrics.
   */
  void GetMetrics(message::Message& aMsg);

private:
  struct Metrics
  {
    Metrics() :
      mConnectionErrors("Connection Errors"),
      mHTTPErrors("HTTP Errors"),
      mInvalidHistograms("Invalid Histograms"),
      mInvalidRevisions("Invalid Revisions"),
      mCacheHits("Cache Hits"),
      mCacheMisses("Cache Misses") { }

    Metric mConnectionErrors;
    Metric mHTTPErrors;
    Metric mInvalidHistograms;
    Metric mInvalidRevisions;
    Metric mCacheHits;
    Metric mCacheMisses;
  };

  /**
   * Retrieves the requested histogram revision from the histogram server.
   *
   * @param aRevisionKey Revision of the histogram file to load.
   *
   * @return const Histogram* nullptr if load fails
   */
  std::shared_ptr<HistogramSpecification>
  LoadHistogram(const std::string& aRevisionKey);

  std::string mHistogramServer;
  std::string mHistogramServerPort;

  /// Cache of histogram schema keyed by MD5
  std::unordered_map<std::string, std::shared_ptr<HistogramSpecification> >
    mCache;

  /// Cache of histogram schema keyed by revision
  std::map<std::string, std::shared_ptr<HistogramSpecification> > mRevisions;

  Metrics mMetrics;
};

}
}

#endif // mozilla_telemetry_Histogram_Cache_h
