/* -*- Mode: C++; tab-width: 8; indent-tabs-mode: nil; c-basic-offset: 2 -*- */
/* vim: set ts=2 et sw=2 tw=80: */
/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

#define BOOST_TEST_MODULE TestHistogramCache
#include <boost/test/unit_test.hpp>
#include "TestConfig.h"
#include "../HistogramCache.h"

using namespace std;
using namespace mozilla::telemetry;

BOOST_AUTO_TEST_CASE(test_valid)
{
  HistogramCache cache("localhost:9898");
  auto h = cache.FindHistogram("https://hg.mozilla.org/releases/mozilla-release/rev/a55c55edf302");
  BOOST_REQUIRE(h);
}

BOOST_AUTO_TEST_CASE(test_unknown_revision)
{
  HistogramCache cache("localhost:9898");
  auto h = cache.FindHistogram("https://hg.mozilla.org/releases/mozilla-release/rev/f55c55edf302");
  BOOST_REQUIRE(!h);
}

BOOST_AUTO_TEST_CASE(test_invalid_revision)
{
  HistogramCache cache("localhost:9898");
  auto h = cache.FindHistogram("missing");
  BOOST_REQUIRE(!h);
}
