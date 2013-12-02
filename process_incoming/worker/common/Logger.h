/* -*- Mode: C++; tab-width: 8; indent-tabs-mode: nil; c-basic-offset: 2 -*- */
/* vim: set ts=2 et sw=2 tw=80: */
/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

#ifndef mozilla_telemetry_logger_h
#define mozilla_telemetry_logger_h

#include <cstring>
#include <boost/log/trivial.hpp>

#define __SHORT_FORM_OF_FILE__ \
  (strrchr(__FILE__,'/') \
   ? strrchr(__FILE__,'/')+1 \
   : __FILE__ \
   )

#define LOGGER(level) BOOST_LOG_TRIVIAL(level) << __FUNCTION__ << " @ " << __SHORT_FORM_OF_FILE__ << ":" << __LINE__ << " - "

#endif
