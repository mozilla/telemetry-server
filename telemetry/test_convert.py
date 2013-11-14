# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import os
import revision_cache
import shutil
import simplejson as json
from telemetry_schema import TelemetrySchema
from convert import Converter

cache_dir = "/tmp/histogram_revision_cache"
schema_filename = "./telemetry/telemetry_schema.json"
assert not os.path.exists(cache_dir)

schema_file = open(schema_filename, "r")
schema = TelemetrySchema(json.load(schema_file))
schema_file.close()
cache = revision_cache.RevisionCache(cache_dir, 'hg.mozilla.org')
converter = Converter(cache, schema)

revision = "http://hg.mozilla.org/mozilla-central/rev/26cb30a532a1"
histograms = {
  "STARTUP_CRASH_DETECTED": {
    "sum_squares_hi": 0, # what about log_sum and log_sum_squares?
    "sum_squares_lo": 0,
    "sum": 0,
    "values": {
      "1": 0,
      "0": 1
    },
    "histogram_type": 3,
    "bucket_count": 3,
    "range": [
      1,
      2
    ]
  },
  "DOM_TIMERS_FIRED_PER_NATIVE_TIMEOUT": {
    "log_sum_squares": 624.027626294358,
    "log_sum": 873.474196434021,
    "sum": 1279,
    "values": {
      "1114": 0,
      "414": 1,
      "8": 2,
      "3": 7,
      "1": 1232,
      "0": 0
    },
    "histogram_type": 0,
    "bucket_count": 10,
    "range": [
      1,
      3000
    ]
  }
}

test_anr = {
  "androidLogcat": "...logcat...",
  "androidANR": "...snip...",
  "info": {
    "hardware": "tuna",
    "appUpdateChannel": "default",
    "appBuildID": "20130225174321",
    "appName": "Fennec",
    "appVersion": "22.0a1",
    "appID": "{aa3c5121-dab2-40e2-81ca-7ea25febc110}",
    "version": "17",
    "OS": "Android",
    "reason": "android-anr-report",
    "platformBuildID": "20130225174321",
    "locale": "en-US",
    "cpucount": 2,
    "memsize": 694,
    "arch": "armv7l",
    "kernel_version": "3.0.31-gd5a18e0",
    "device": "Galaxy Nexus",
    "manufacturer": "samsung"
  },
  "simpleMeasurements": {
    "uptime": 0
  },
  "ver": 1
}

try:
    rewritten = converter.rewrite_hists(revision, histograms)
    print "Converted input:"
    print json.dumps(histograms)
    print "To output:"
    print json.dumps(rewritten)
    assert rewritten["STARTUP_CRASH_DETECTED"] == [1, 0, 0, 0, -1, -1, 0, 0]
    assert rewritten["DOM_TIMERS_FIRED_PER_NATIVE_TIMEOUT"] == [0,1232,7,2,0,0,0,1,0,0,1279,873.474196434021,624.027626294358,-1,-1]

    test_anr_converted, dimensions = converter.convert_json(json.dumps(test_anr), "20131114")
    assert dimensions[0] == "android-anr-report"
    assert test_anr["ver"] == Converter.VERSION_UNCONVERTED
    assert test_anr_converted["ver"] == Converter.VERSION_CONVERTED
finally:
    shutil.rmtree(cache_dir)
    pass
