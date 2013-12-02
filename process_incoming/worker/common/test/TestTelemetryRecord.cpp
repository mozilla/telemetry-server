/* -*- Mode: C++; tab-width: 8; indent-tabs-mode: nil; c-basic-offset: 2 -*- */
/* vim: set ts=2 et sw=2 tw=80: */
/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

#define BOOST_TEST_MODULE TestTelemetryRecord
#include <boost/test/unit_test.hpp>
#include "TestConfig.h"
#include "../TelemetryRecord.h"

#include <string>
#include <fstream>
#include <sstream>

#include <rapidjson/writer.h>

#include <iostream>

using namespace std;
using namespace mozilla::telemetry;

static const string rec("\x1e\x04\x00\x07\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00" "abcd{\"a\":8}", 26);

BOOST_AUTO_TEST_CASE(test_read)
{
  string data(rec + rec);
  istringstream iss(data);
  TelemetryRecord tr;
  for (int i = 0; i < 2; ++i) {
    BOOST_REQUIRE_EQUAL(true, tr.Read(iss));
    BOOST_REQUIRE_EQUAL(1, tr.GetTimestamp());
    BOOST_REQUIRE_EQUAL("abcd", tr.GetPath());
    BOOST_REQUIRE_EQUAL(8, tr.GetDocument()["a"].GetInt());
  }
}

BOOST_AUTO_TEST_CASE(test_exceed_pathlength)
{
  string data(rec + string("\x1e\xff\xff\x07\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00", 15) + rec);
  istringstream iss(data);
  TelemetryRecord tr;
  for (int i = 0; i < 2; ++i) {
    BOOST_REQUIRE_EQUAL(true, tr.Read(iss));
    BOOST_REQUIRE_EQUAL(1, tr.GetTimestamp());
    BOOST_REQUIRE_EQUAL("abcd", tr.GetPath());
    BOOST_REQUIRE_EQUAL(8, tr.GetDocument()["a"].GetInt());
  }
}

BOOST_AUTO_TEST_CASE(test_short_pathlength)
{
  string bad_rec("\x1e\x02\x00\x07\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00" "abcd{\"a\":8}", 26);
  string data(bad_rec + rec);
  istringstream iss(data);
  TelemetryRecord tr;

  BOOST_REQUIRE_EQUAL(true, tr.Read(iss));
  BOOST_REQUIRE_EQUAL(1, tr.GetTimestamp());
  BOOST_REQUIRE_EQUAL("abcd", tr.GetPath());
  BOOST_REQUIRE_EQUAL(8, tr.GetDocument()["a"].GetInt());

  BOOST_REQUIRE_EQUAL(false, tr.Read(iss));
}

//BOOST_AUTO_TEST_CASE(test_large_file)
//{
//  ifstream file(kDataPath + "../../../../telemetry.log", ios_base::binary);
//  TelemetryRecord tr;
//  int cnt = 0;
//  while (tr.Read(file)) {
//    ++cnt;
//  }
//  BOOST_REQUIRE_EQUAL(7331, cnt);
//}
