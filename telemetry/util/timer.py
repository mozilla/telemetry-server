#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from datetime import datetime

def delta_ms(start, end=None):
    if end is None:
        end = datetime.now()
    delta = end - start
    ms = delta.seconds * 1000.0 + float(delta.microseconds) / 1000.0
    # prevent division-by-zero errors by cheating:
    if ms == 0.0:
        return 0.0001
    return ms

def delta_sec(start, end=None):
    return delta_ms(start, end) / 1000.0
