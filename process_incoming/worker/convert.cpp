/* -*- Mode: C++; tab-width: 8; indent-tabs-mode: nil; c-basic-offset: 2 -*- */
/* vim: set ts=2 et sw=2 tw=80: */
/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

/// @brief Telemetry data coverter implementation @file

#include "ConvertConfig.h"
#include "HekaLogger.h"
#include "HistogramCache.h"
#include "HistogramConverter.h"
#include "TelemetryRecord.h"
#include "TelemetrySchema.h"
#include "RecordWriter.h"
#include "Metric.h"
#include "message.pb.h"

#include <boost/filesystem.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <chrono>
#include <exception>
#include <fstream>
#include <iostream>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

using namespace std;
namespace fs = boost::filesystem;
namespace mt = mozilla::telemetry;

struct Metrics
{
  Metrics()
    : mRecordsProcessed("Records Processed")
    , mRecordsFailed("Records Discarded")
    , mDataIn("Data In", "B")
    , mDataOut("Data Out", "B")
    , mProcessingTime("Processing Time", "s")
    , mThroughput("Throughput", "MiB/s")
    , mExceptions("Exceptions")

  { }

  void GetMetrics(message::Message& aMsg)
  {
    aMsg.clear_fields();
    mt::ConstructField(aMsg, mRecordsProcessed);
    mt::ConstructField(aMsg, mRecordsFailed);
    mt::ConstructField(aMsg, mDataIn);
    mt::ConstructField(aMsg, mDataOut);
    mt::ConstructField(aMsg, mProcessingTime);
    mt::ConstructField(aMsg, mThroughput);
    mt::ConstructField(aMsg, mExceptions);

    mRecordsProcessed.mValue = 0;
    mRecordsFailed.mValue = 0;
    mDataIn.mValue = 0;
    mDataOut.mValue = 0;
    mProcessingTime.mValue = 0;
    mThroughput.mValue = 0;
    mExceptions.mValue = 0;
  }

  mt::Metric mRecordsProcessed;
  mt::Metric mRecordsFailed;
  mt::Metric mDataIn;
  mt::Metric mDataOut;
  mt::Metric mProcessingTime;
  mt::Metric mThroughput;
  mt::Metric mExceptions;
} gMetrics;

///////////////////////////////////////////////////////////////////////////////
bool ProcessFile(const boost::filesystem::path& aName,
                 mt::TelemetrySchema& aSchema,
                 mt::TelemetryRecord& aRecord,
                 mt::HistogramCache& aCache,
                 mt::RecordWriter& aWriter)
{
  try {
    cout << "processing file:" << aName.filename() << endl;
    chrono::time_point<chrono::system_clock> start, end;
    start = chrono::system_clock::now();
    ifstream file(aName.c_str());
    rapidjson::StringBuffer sb;
    rapidjson::Writer<rapidjson::StringBuffer> writer(sb);
    while (aRecord.Read(file)) {
      if (ConvertHistogramData(aCache, aRecord.GetDocument())) {
        sb.Clear();
        const char* s = aRecord.GetPath();
        for (int x = 0; s[x] != 0 && s[x] != '/'; ++x) { // extract uuid
          sb.Put(s[x]);
        }
        sb.Put('\t');
        aRecord.GetDocument().Accept(writer);
        sb.Put('\n');
        fs::path p = aSchema.GetDimensionPath(aRecord.GetDocument());
        aWriter.Write(p, sb.GetString(), sb.Size());
        gMetrics.mDataOut.mValue += sb.Size();
      } else {
        // cerr << "Conversion failed: " << aRecord.GetPath() << endl;
        ++gMetrics.mRecordsFailed.mValue;
      }
      ++gMetrics.mRecordsProcessed.mValue;
    }
    end = chrono::system_clock::now();
    chrono::duration<double> elapsed = end - start;
    gMetrics.mProcessingTime.mValue = elapsed.count();
    gMetrics.mDataIn.mValue = file_size(aName);

    if (gMetrics.mProcessingTime.mValue > 0) {
      gMetrics.mThroughput.mValue = gMetrics.mDataIn.mValue / 1024 / 1024
        / gMetrics.mProcessingTime.mValue;
    }
    cout << "done processing file:" << aName.filename()
      << " success:" <<  gMetrics.mRecordsProcessed.mValue
      << " failures:" << gMetrics.mRecordsFailed.mValue
      << " time:" << gMetrics.mProcessingTime.mValue
      << " throughput (MiB/s):" << gMetrics.mThroughput.mValue
      << " data in (B):" << gMetrics.mDataIn.mValue
      << " data out (B):" << gMetrics.mDataOut.mValue
      << endl;
  }
  catch (const exception& e) {
    cerr << "ProcessFile std exception: " << e.what() << endl;
    ++gMetrics.mExceptions.mValue;
    return false;
  }
  catch (...) {
    cerr << "ProcessFile unknown exception\n";
    ++gMetrics.mExceptions.mValue;
    return false;
  }
  return true;
}

///////////////////////////////////////////////////////////////////////////////
int main(int argc, char** argv)
{
  // Verify that the version of the library that we linked against is
  // compatible with the version of the headers we compiled against.
  GOOGLE_PROTOBUF_VERIFY_VERSION;

  if (argc < 3) {
    cerr << "usage: " << argv[0] << " <json config> <input config>\n";
    return EXIT_FAILURE;
  }
  char hostname[HOST_NAME_MAX];
  if (gethostname(hostname, HOST_NAME_MAX) != 0) {
    cerr << "gethostname() failed\n";
    return EXIT_FAILURE;
  }
  message::Message msg;
  msg.set_type("telemetry");
  msg.set_pid(getpid());
  msg.set_hostname(hostname);

  try {
    mt::ConvertConfig config;
    mt::ReadConfig(argv[1], config);
    mt::TelemetryRecord record;
    mt::HistogramCache cache(config.mHistogramServer);
    mt::TelemetrySchema schema(config.mTelemetrySchema);
    mt::RecordWriter writer(config.mStoragePath, config.mUploadPath,
                            config.mMaxUncompressed, config.mMemoryConstraint,
                            config.mCompressionPreset);
    mt::HekaLogger logger;
    if (!logger.Connect(config.mHekaServer)) {
      cerr << "Cannot connect to Heka, logging is disabled\n";
    }
    boost::asio::streambuf sb;
    ostream os(&sb);

    ifstream ifs(argv[2]);
    string filename;
    while (getline(ifs, filename)) {
      fs::path fn(filename);
      if (fn.empty()) {
        continue;
      }
      while (!exists(fn)) {
        sleep(2);
      }
      if (ProcessFile(fn, schema, record, cache, writer)) {
        remove(fn);
      }
      if (logger()) {
        boost::uuids::uuid u = boost::uuids::random_generator()();
        msg.set_uuid(&u, u.size());
        auto tp = chrono::high_resolution_clock::now();
        chrono::nanoseconds ts = tp.time_since_epoch();
        msg.set_timestamp(ts.count());
        msg.set_payload(fn.filename().c_str());

        msg.set_logger("cache");
        cache.GetMetrics(msg);
        mt::WriteMessage(os, msg);

        msg.set_logger("record");
        record.GetMetrics(msg);
        mt::WriteMessage(os, msg);

        msg.set_logger("schema");
        schema.GetMetrics(msg);
        mt::WriteMessage(os, msg);

        msg.set_logger("converter");
        gMetrics.GetMetrics(msg);
        mt::WriteMessage(os, msg);
        logger.Write(sb);
      }
    }
  }
  catch (const exception& e) {
    cerr << "std exception: " << e.what();
    return EXIT_FAILURE;
  }
  catch (...) {
    cerr << "unknown exception";
    return EXIT_FAILURE;
  }
// Optional:  Delete all global objects allocated by libprotobuf.
  google::protobuf::ShutdownProtobufLibrary();

  return EXIT_SUCCESS;
}
