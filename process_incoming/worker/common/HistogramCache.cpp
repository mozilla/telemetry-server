/* -*- Mode: C++; tab-width: 8; indent-tabs-mode: nil; c-basic-offset: 2 -*- */
/* vim: set ts=2 et sw=2 tw=80: */
/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

/// @brief Histogram cache implementation @file

#include "HistogramCache.h"

#include <boost/asio.hpp>

#include <openssl/md5.h>
#include <iostream>
#include <fstream>

using namespace std;
namespace fs = boost::filesystem;

namespace mozilla {
namespace telemetry {

////////////////////////////////////////////////////////////////////////////////
HistogramCache::HistogramCache(const std::string& aHistogramServer)
{
  size_t pos = aHistogramServer.find(':');
  mHistogramServer = aHistogramServer.substr(0, pos);
  if (pos != string::npos) {
    mHistogramServerPort = aHistogramServer.substr(pos + 1);
  } else {
    mHistogramServerPort = "http";
  }
}

////////////////////////////////////////////////////////////////////////////////
std::shared_ptr<HistogramSpecification>
HistogramCache::FindHistogram(const std::string& aRevisionKey)
{
  shared_ptr<HistogramSpecification> h;

  if (aRevisionKey.compare(0, 4, "http") != 0) {
    ++mMetrics.mInvalidRevisions.mValue;
    return h;
  }
  auto it = mRevisions.find(aRevisionKey);
  if (it != mRevisions.end()) {
    ++mMetrics.mCacheHits.mValue;
    h = it->second;
  } else {
    ++mMetrics.mCacheMisses.mValue;
    try {
      h = LoadHistogram(aRevisionKey);
    }
    catch (const exception& e) {
      ++mMetrics.mConnectionErrors.mValue;
      cerr << "LoadHistogram - " << e.what() << endl;
    }
  }
  return h;
}

////////////////////////////////////////////////////////////////////////////////
void
HistogramCache::GetMetrics(message::Message& aMsg)
{
  aMsg.clear_fields();
  ConstructField(aMsg, mMetrics.mConnectionErrors);
  ConstructField(aMsg, mMetrics.mHTTPErrors);
  ConstructField(aMsg, mMetrics.mInvalidHistograms);
  ConstructField(aMsg, mMetrics.mInvalidRevisions);
  ConstructField(aMsg, mMetrics.mCacheHits);
  ConstructField(aMsg, mMetrics.mCacheMisses);

  mMetrics.mConnectionErrors.mValue = 0;
  mMetrics.mHTTPErrors.mValue = 0;
  mMetrics.mInvalidHistograms.mValue = 0;
  mMetrics.mInvalidRevisions.mValue = 0;
  mMetrics.mCacheHits.mValue = 0;
  mMetrics.mCacheMisses.mValue = 0;
}

////////////////////////////////////////////////////////////////////////////////
/// Private Member Functions
////////////////////////////////////////////////////////////////////////////////
std::shared_ptr<HistogramSpecification>
HistogramCache::LoadHistogram(const std::string& aRevisionKey)
{
  string json;
  string tmpName = aRevisionKey;
  std::replace(tmpName.begin(), tmpName.end(), '/', '-');
  fs::path tmp_cache = fs::temp_directory_path() / (tmpName + ".json");
  ifstream ifs(tmp_cache.c_str());
  if (!ifs) {
    using boost::asio::ip::tcp;
    boost::asio::io_service io_service;

    // Get a list of endpoints corresponding to the server name.
    tcp::resolver resolver(io_service);
    tcp::resolver::query query(mHistogramServer, mHistogramServerPort);
    tcp::resolver::iterator endpoint_iterator = resolver.resolve(query);

    // Try each endpoint until we successfully establish a connection.
    tcp::socket socket(io_service);
    boost::asio::connect(socket, endpoint_iterator);

    // Form the request. We specify the "Connection: close" header so that the
    // server will close the socket after transmitting the response. This will
    // allow us to treat all data up until the EOF as the content.
    boost::asio::streambuf request;
    std::ostream request_stream(&request);
    request_stream << "GET " << "/histogram_buckets?revision=" << aRevisionKey
                   << " HTTP/1.0\r\n";
    request_stream << "Host: " << mHistogramServer << ":" << mHistogramServerPort
                   << "\r\n";
    request_stream << "Accept: */*\r\n";
    request_stream << "Connection: close\r\n\r\n";

    // Send the request.
    boost::asio::write(socket, request);

    // Read the response status line. The response streambuf will automatically
    // grow to accommodate the entire line. The growth may be limited by passing
    // a maximum size to the streambuf constructor.
    boost::asio::streambuf response;
    boost::asio::read_until(socket, response, "\r\n");

    // Check that response is OK.
    std::istream response_stream(&response);
    std::string http_version;
    response_stream >> http_version;
    unsigned int status_code;
    response_stream >> status_code;
    std::string status_message;
    std::getline(response_stream, status_message);
    if (!response_stream || http_version.substr(0, 5) != "HTTP/") {
      ++mMetrics.mHTTPErrors.mValue;
      return shared_ptr<HistogramSpecification>();
    }

    // Read the response headers, which are terminated by a blank line.
    boost::asio::read_until(socket, response, "\r\n\r\n");

    // Process the response headers.
    std::string header;
    while (std::getline(response_stream, header) && header != "\r");
    if (status_code != 200) {
      ++mMetrics.mHTTPErrors.mValue;
      shared_ptr<HistogramSpecification> h;
      mRevisions.insert(make_pair(aRevisionKey, h)); // prevent retries
      return h;
    }

    ostringstream oss;
    // Write whatever content we already have to output.
    if (response.size() > 0) oss << &response;
    // Read until EOF, writing data to output as we go.
    boost::system::error_code error;
    while (boost::asio::read(socket, response,
                             boost::asio::transfer_at_least(1), error)) {
      oss << &response;
    }
    if (error != boost::asio::error::eof) {
      throw boost::system::system_error(error);
    }
    try {
      json = oss.str();
      ofstream ofs(tmp_cache.c_str());
      ofs << json;
      ofs.close();
    }
    catch (exception &e){
      ++mMetrics.mInvalidHistograms.mValue;
      cerr << "LoadHistogram - invalid histogram specification: "
        << aRevisionKey << endl;
      return shared_ptr<HistogramSpecification>();
    }
  } else {
    json = string(istream_iterator<char>(ifs), istream_iterator<char>());
  }
  // histogram specs do not change often between revisions. dedup based on contents of the json
  unsigned char digest[MD5_DIGEST_LENGTH];
  MD5((unsigned char*)json.c_str(), json.size(), digest);

  string key(reinterpret_cast<char*>(digest), MD5_DIGEST_LENGTH);

  auto it = mCache.find(key);
  if (it != mCache.end()) {
    mRevisions.insert(make_pair(aRevisionKey, it->second));
    return it->second;
  }

  shared_ptr<HistogramSpecification> h(new HistogramSpecification(json));
  mCache.insert(make_pair(key, h));
  mRevisions.insert(make_pair(aRevisionKey, h));
  return h;
}

}
}
