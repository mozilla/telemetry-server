/* -*- Mode: C++; tab-width: 8; indent-tabs-mode: nil; c-basic-offset: 2 -*- */
/* vim: set ts=2 et sw=2 tw=80: */
/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

/** @file 
Generic structure for tracking runtime statistics. 
*/

#ifndef mozilla_telemetry_Metric_h
#define mozilla_telemetry_Metric_h

#include "message.pb.h"

#include <ostream>

namespace mozilla {
namespace telemetry {

struct Metric
{
  Metric(std::string aName, std::string aRepresentation = "count") :
    mName(aName),
    mRepresentation(aRepresentation),
    mValue(0) { }

  std::string mName;
  std::string mRepresentation;
  double      mValue;
};

/**
 * Helper function to ture a Metric struct into a Heka message field.
 * 
 * @param aMsg Heka protobuf message to add the field to.
 * @param aMetric Metric to be converted to a field.
 */
void ConstructField(message::Message &aMsg, Metric& aMetric);

/**
 * Writes a Heka protobuf message with proper framing for stream output. 
 *  
 * @param os Output stream receiving the message.
 * @param aMsg Message to be framed, encoded, and written.
 */
void WriteMessage(std::ostream &os, message::Message &aMsg);

}
}

#endif // mozilla_telemetry_Metric_h
