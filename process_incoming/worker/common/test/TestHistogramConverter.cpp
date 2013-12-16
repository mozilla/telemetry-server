/* -*- Mode: C++; tab-width: 8; indent-tabs-mode: nil; c-basic-offset: 2 -*- */
/* vim: set ts=2 et sw=2 tw=80: */
/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

#define BOOST_TEST_MODULE TestHistogramConverter
#include <boost/test/unit_test.hpp>
#include "TestConfig.h"
#include "../HistogramConverter.h"

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

using namespace std;
using namespace mozilla::telemetry;

BOOST_AUTO_TEST_CASE(test_converter)
{
  const char* hist = "{\"ver\":1,\"histograms\":{\"A11Y_IATABLE_USAGE_FLAG\":{\"range\":[1,2],\"bucket_count\":3,\"histogram_type\":3,\"values\":{\"0\":1,\"1\":0},\"sum\":4984161763,\"sum_squares_lo\":1.23415,\"sum_squares_hi\":1.01}},\"info\":{\"revision\":\"http://hg.mozilla.org/releases/mozilla-release/rev/a55c55edf302\"}}";

  const char* conv = "{\"ver\":2,\"histograms\":{\"A11Y_IATABLE_USAGE_FLAG\":[1,0,0,4984161763,-1,-1,1.23415,1.01]},\"info\":{\"revision\":\"http://hg.mozilla.org/releases/mozilla-release/rev/a55c55edf302\"}}";

  RapidjsonDocument d;
  d.Parse<0>(hist);
  BOOST_REQUIRE(!d.HasParseError());

  HistogramCache cache("localhost:9898");
  BOOST_REQUIRE_EQUAL(true, ConvertHistogramData(cache, d));
  rapidjson::StringBuffer sb;
  rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
  d.Accept(writer);
  BOOST_REQUIRE_EQUAL(conv, sb.GetString());
}
