
/* -*- Mode: C++; tab-width: 8; indent-tabs-mode: nil; c-basic-offset: 2 -*- */
/* vim: set ts=2 et sw=2 tw=80: */
/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

#define BOOST_TEST_MODULE TestTelemetrySchema
#include <boost/test/unit_test.hpp>
#include "TestConfig.h"
#include "../TelemetrySchema.h"

#include <rapidjson/document.h>

using namespace std;
using namespace mozilla::telemetry;

BOOST_AUTO_TEST_CASE(test_load)
{
  const char* info = "{\"info\":{\"reason\":\"idle-daily\",\"OS\":\"WINNT\",\"appID\":\"{ec8030f7-c20a-464f-9b0e-13a3a9e97384}\",\"appVersion\":\"23.0.1\",\"appName\":\"Firefox\",\"appBuildID\":\"20130814063812\",\"appUpdateChannel\":\"release\",\"platformBuildID\":\"20130814063812\",\"revision\":\"http://hg.mozilla.org/releases/mozilla-release/rev/a55c55edf302\",\"locale\":\"en-US\",\"cpucount\":1,\"memsize\":447,\"arch\":\"x86\",\"version\":\"5.1\",\"hasMMX\":true,\"hasSSE\":true,\"hasSSE2\":false,\"hasSSE3\":false,\"hasSSSE3\":false,\"hasSSE4A\":false,\"hasSSE4_1\":false,\"hasSSE4_2\":false,\"hasEDSP\":false,\"hasARMv6\":false,\"hasARMv7\":false,\"hasNEON\":false,\"isWow64\":false,\"adapterDescription\":\"NVIDIA GeForce4 MX Integrated GPU (Microsoft Corporation)\",\"adapterVendorID\":\"0x10de\",\"adapterDeviceID\":\"0x01f0\",\"adapterRAM\":\"Unknown\",\"adapterDriver\":\"nv4_disp\",\"adapterDriverVersion\":\"5.6.7.3\",\"adapterDriverDate\":\"4-7-2004\",\"DWriteVersion\":\"0.0.0.0\",\"persona\":\"56527\",\"addons\":\"ffxtlbra%40softonic.com:1.6.0,%7B972ce4c6-7e08-4474-a285-3208198ce6fd%7D:23.0.1\",\"flashVersion\":\"11.8.800.94\"}}";
  string fn(kDataPath + "telemetry_schema.json");
  try {
    TelemetrySchema t(fn);    
    RapidjsonDocument d;
    d.Parse<0>(info);
    boost::filesystem::path p = t.GetDimensionPath(d);
    BOOST_REQUIRE_EQUAL("idle_daily/Firefox/release/23.0.1/20130814063812/other", p);

  }
  catch (const exception& e) {
    BOOST_FAIL(e.what());
  }
}

BOOST_AUTO_TEST_CASE(test_missing_file)
{
  string fn(kDataPath + "missing.json");
  try {
    TelemetrySchema t(fn);
    BOOST_FAIL("exception expected");
  }
  catch (const exception& e) {
    BOOST_REQUIRE_EQUAL(e.what(), "file open failed: " + fn);
  }
}

BOOST_AUTO_TEST_CASE(test_invalid_file)
{
  string fn(kDataPath + "invalid.json");
  try {
    TelemetrySchema t(fn);
    BOOST_FAIL("exception expected");
  }
  catch (const exception& e) {
    BOOST_REQUIRE_EQUAL(e.what(), "json parse failed: Expect either an object"
                        " or array at root");
  }
}

BOOST_AUTO_TEST_CASE(test_invalid_schema)
{
  string fn(kDataPath + "invalid_schema.json");
  try {
    TelemetrySchema t(fn);
    BOOST_FAIL("exception expected");
  }
  catch (const exception& e) {
    BOOST_REQUIRE_EQUAL(e.what(), "version element is missing");
  }
}
