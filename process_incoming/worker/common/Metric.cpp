/* -*- Mode: C++; tab-width: 8; indent-tabs-mode: nil; c-basic-offset: 2 -*- */
/* vim: set ts=2 et sw=2 tw=80: */
/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

/// @brief Metric implementation @file

#include "Metric.h"
#include "TelemetryConstants.h"

#include <iostream>

namespace mozilla {
namespace telemetry {

////////////////////////////////////////////////////////////////////////////////
void
ConstructField(message::Message& aMsg, Metric& aMetric)
{
  auto f = aMsg.add_fields();
  f->set_name(aMetric.mName);
  f->set_representation(aMetric.mRepresentation);
  f->set_value_type(message::Field_ValueType_DOUBLE);
  f->add_value_double(aMetric.mValue);
}

////////////////////////////////////////////////////////////////////////////////
void
WriteMessage(std::ostream& os, message::Message& aMsg)
{
  if (!os) return;

  message::Header h;
  h.set_message_length(aMsg.ByteSize());
  os.put(kRecordSeparator);
  os.put(h.ByteSize());
  h.SerializeToOstream(&os);
  os.put(kUnitSeparator);
  aMsg.SerializeToOstream(&os);
}

}
}

