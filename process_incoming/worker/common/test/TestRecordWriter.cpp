/* -*- Mode: C++; tab-width: 8; indent-tabs-mode: nil; c-basic-offset: 2 -*- */
/* vim: set ts=2 et sw=2 tw=80: */
/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

#define BOOST_TEST_MODULE TestRecordWriter

#include "TestConfig.h"
#include "../RecordWriter.h"

#include <stdlib.h>
#include <fstream>

#include <boost/test/unit_test.hpp>

using namespace std;
using namespace mozilla::telemetry;

namespace fs = boost::filesystem;

BOOST_AUTO_TEST_CASE(test_converter)
{
  fs::path workDir = ".work";
  fs::path uploadDir = ".upload";

  BOOST_REQUIRE(!fs::exists(workDir));
  BOOST_REQUIRE(!fs::exists(uploadDir));
  fs::create_directory(workDir);
  fs::create_directory(uploadDir);

  RecordWriter writer(workDir.string(), uploadDir.string(), 1048576, 1000, 0);
  string payload = "431ab5c3-2712-4ab7-a4b6-e9b61f3a1f30	{\"ver\":2,\"histograms\":{\"A11Y_IATABLE_USAGE_FLAG\":[1,0,0,0,-1,-1,1.23415,1.01]},\"info\":{\"revision\":\"http://hg.mozilla.org/releases/mozilla-release/rev/a55c55edf302\"}}";
  writer.Write("output", payload.c_str(), payload.size() + 1);
  writer.Finalize();
  BOOST_REQUIRE(fs::is_empty(workDir));
  BOOST_REQUIRE(!fs::is_empty(uploadDir));

  fs::directory_iterator it(uploadDir);
  fs::path generated = it->path();

  string command = "xz -d " + generated.string();
  BOOST_REQUIRE(system(command.c_str()) == 0);

  fs::path decompressed = generated.replace_extension();
  ifstream decompressedFile(decompressed.string());
  string line;
  BOOST_REQUIRE(getline(decompressedFile, line, '\0'));
  BOOST_REQUIRE_EQUAL(line, payload);

  fs::remove_all(workDir);
  fs::remove_all(uploadDir);
}
