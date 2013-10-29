/* -*- Mode: C++; tab-width: 8; indent-tabs-mode: nil; c-basic-offset: 2 -*- */
/* vim: set ts=2 et sw=2 tw=80: */
/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

/// @brief Heka logger implementation @file

#include "HekaLogger.h"

#include <boost/asio.hpp>

using boost::asio::ip::tcp;

namespace mozilla {
namespace telemetry {

////////////////////////////////////////////////////////////////////////////////
HekaLogger::HekaLogger() : mSocket(mIo) { }

////////////////////////////////////////////////////////////////////////////////
bool HekaLogger::Connect(const std::string& aHeka)
{
  if (mSocket.is_open()) {
    mSocket.close();
  }

  size_t pos = aHeka.find(':');
  std::string host = aHeka.substr(0, pos);
  std::string port;
  if (pos != std::string::npos) {
    port = aHeka.substr(pos + 1);
  } else {
    port = "5565";
  }
  try {
    boost::asio::ip::tcp::resolver resolver(mIo);
    boost::asio::ip::tcp::resolver::query query(host, port);
    boost::asio::ip::tcp::resolver::iterator end,  i = resolver.resolve(query);
    if (end == boost::asio::connect(mSocket, i)) {
      return false;
    }
  }
  catch (...) {
    return false;
  }
  return true;
}

////////////////////////////////////////////////////////////////////////////////
void HekaLogger::Disconnect()
{
  mSocket.close();
}

////////////////////////////////////////////////////////////////////////////////
bool HekaLogger::Write(boost::asio::streambuf& sb)
{
  if (!mSocket.is_open()) {
   return false;
  }

  try {
    write(mSocket, sb);
  }
  catch (...) {
    return false;
  }
  return true;
}

}
}
