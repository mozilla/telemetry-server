/* -*- Mode: C++; tab-width: 8; indent-tabs-mode: nil; c-basic-offset: 2 -*- */
/* vim: set ts=2 et sw=2 tw=80: */
/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

/** @file Writes log message to Heka via TCP */

#ifndef mozilla_telemetry_Heka_Logger_h
#define mozilla_telemetry_Heka_Logger_h

#include <boost/asio.hpp>
#include <string>

namespace mozilla {
namespace telemetry {

class HekaLogger
{
public:
  HekaLogger();

  /**
   * Connects the logger to a Heka instance.
   *
   * @param aHeka Hostname:port
   *
   * @return bool True if a connection could be established.
   */
  bool Connect(const std::string& aHeka);

  /**
   * Closes the connect to the Heka server.
   */
  void Disconnect();

  /**
   * Writes the data to the Heka server.
   *
   * @param sb Stream buffer containing the data to output.
   *
   * @return bool True if the data was successfully written to Heka.
   */
  bool Write(boost::asio::streambuf& sb);

  bool operator()()
  {
    return mSocket.is_open();
  }

private:
  boost::asio::io_service mIo;
  boost::asio::ip::tcp::socket mSocket;
};

}
}

#endif // mozilla_telemetry_Heka_Logger_h

