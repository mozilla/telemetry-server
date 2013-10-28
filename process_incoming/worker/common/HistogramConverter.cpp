/* -*- Mode: C++; tab-width: 8; indent-tabs-mode: nil; c-basic-offset: 2 -*- */
/* vim: set ts=2 et sw=2 tw=80: */
/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

/// @brief Histogram converter implementation @file

#include "HistogramConverter.h"
#include "TelemetryConstants.h"

#include <iostream>
#include <memory>
#include <vector>

using namespace std;

namespace mozilla {
namespace telemetry {

bool RewriteValues(shared_ptr<HistogramDefinition> aDef,
                   const RapidjsonValue& aData,
                   vector<int>& aRewrite);

bool RewriteHistogram(shared_ptr<HistogramSpecification>& aHist, RapidjsonValue& aValue);


////////////////////////////////////////////////////////////////////////////////
bool ConvertHistogramData(HistogramCache& aCache, RapidjsonDocument& aDoc)
{
  const RapidjsonValue& info = aDoc["info"];
  if (!info.IsObject()) {
    //cerr << "ConvertHistogramData - missing info object\n";
    return false;
  }

  const RapidjsonValue& revision = info["revision"];
  if (!revision.IsString()) {
    //cerr << "ConvertHistogramData - missing info.revision\n";
    return false;
  }

  RapidjsonValue& histograms = aDoc["histograms"];
  if (!histograms.IsObject()) {
    //cerr << "ConvertHistogramData - missing hintograms object\n";
    return false;
  }

  RapidjsonValue& ver = aDoc["ver"];
  if (!ver.IsInt() || ver.GetInt() != 1) {
    //cerr << "ConvertHistogramData - missing ver\n";
    return false;
  }

  bool result = true;
  switch (ver.GetInt()) {
  case 1:
    {
      shared_ptr<HistogramSpecification> hist = aCache.FindHistogram(revision.GetString());
      if (hist) {
        result = RewriteHistogram(hist, histograms);
        if (result) {
          ver.SetInt(2);
        } else {
          ver.SetInt(-1);
        }
      } else {
        cerr << "ConvertHistogramData - histogram not found: " << revision.GetString() << endl;
        result = false;
      }
    }
    break;
  case 2: // already converted
    break;
  default:
    cerr << "ConvertHistogramData - invalid version\n";
    result = false;
    break;
  }

  return result;
}

////////////////////////////////////////////////////////////////////////////////
bool RewriteValues(const HistogramDefinition* aDef,
                   const RapidjsonValue& aData,
                   std::vector<int>& aRewrite)
{
  const RapidjsonValue& values = aData["values"];
  if (!values.IsObject()) {
    cerr << "RewriteValues - value object not found\n";
    return false;
  }
  for (RapidjsonValue::ConstMemberIterator it = values.MemberBegin();
       it != values.MemberEnd(); ++it) {
    if (!it->value.IsInt()) {
      cerr << "RewriteValues - invalid value object\n";
      return false;
    }
    long lb = strtol(it->name.GetString(), nullptr, 10);
    int i = it->value.GetInt();
    int index = aDef->GetBucketIndex(lb);
    if (index == -1) {
      cerr << "RewriteValues - invalid bucket lower bound\n";
      return false;
    }
    aRewrite[index] = i;
  }
  return true;
}

////////////////////////////////////////////////////////////////////////////////
bool RewriteHistogram(shared_ptr<HistogramSpecification>& aHist, RapidjsonValue& aValue)
{
  RapidjsonDocument doc;
  RapidjsonDocument::AllocatorType& alloc = doc.GetAllocator();
  vector<double> summary(kExtraBucketsSize);
  bool result = true;

  for (RapidjsonValue::MemberIterator it = aValue.MemberBegin(); result &&
       it != aValue.MemberEnd(); ++it) {
    if (it->value.IsObject()) {
      const char* name = reinterpret_cast<const char*>(it->name.GetString());
      const HistogramDefinition* hd = aHist->GetDefinition(name);
      if (!hd) {
        if (strncmp(name, "STARTUP_", 8)) {
          hd = aHist->GetDefinition(name + 8);
          if (hd) {
            it->name.SetString(name + 8);
            // chop off leading "STARTUP_" per
            // http://mxr.mozilla.org/mozilla-central/source/toolkit/components/telemetry/TelemetryPing.js#532
          }
        }
      }
      if (hd) {
        int bucketCount = hd->GetBucketCount();
        vector<int> rewrite(bucketCount);
        result = RewriteValues(hd, it->value, rewrite);
        if (result) {
          // save off the summary values before rewriting the histogram data
          for (int x = 0; kExtraBuckets[x] != nullptr; ++x) {
            const RapidjsonValue& v = it->value[kExtraBuckets[x]];
            if (v.IsNumber()) {
              summary[x] = v.GetDouble();
            } else {
              summary[x] = -1;
            }
          }
          // rewrite the JSON histogram data
          it->value.SetArray();
          it->value.Reserve(bucketCount + kExtraBucketsSize, alloc);
          auto end = rewrite.end();
          for (auto vit = rewrite.begin(); vit != end; ++vit){
            it->value.PushBack(*vit, alloc);
          }
          // add the summary information
          auto send = summary.end();
          for (auto vit = summary.begin(); vit != send; ++vit){
            it->value.PushBack(*vit, alloc);
          }
        }
      } else {
        //cerr << "RewriteHistogram - histogram definitition lookup failed: " << name << endl;
      }
    } else {
      cerr << "RewriteHistogram - not a histogram object\n";
    }
  }
  return result;
}

}
}
