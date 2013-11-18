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

expected_converted_histograms = {
  "STARTUP_CRASH_DETECTED": [1, 0, 0, 0, -1, -1, 0, 0],
  "DOM_TIMERS_FIRED_PER_NATIVE_TIMEOUT": [0,1232,7,2,0,0,0,1,0,0,1279,873.474196434021,624.027626294358,-1,-1]
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

test_normal = {
  "info": {
    "flashVersion": "11,2,202,327",
    "addons": "tabcount%403greeneggs.com:1.1",
    "adapterDriverVersion": "3.0 Mesa 9.2.1",
    "adapterDeviceID": "Mesa DRI Intel(R) Sandybridge Mobile ",
    "adapterVendorID": "Intel Open Source Technology Center",
    "adapterDescription": "Intel Open Source Technology Center -- Mesa DRI Intel(R) Sandybridge Mobile ",
    "hasNEON": False,  "hasARMv7": False, "hasARMv6": False, "hasEDSP": False,
    "hasSSE4_2": True, "hasSSE4_1": True, "hasSSE4A": False, "hasSSSE3": True,
    "hasSSE3": True,   "hasSSE2": True,   "hasMMX": True,    "hasSSE": True,
    "platformBuildID": "20131112030204",
    "appUpdateChannel": "nightly",
    "appBuildID": "20131112030204",
    "appName": "Firefox",
    "appVersion": "28.0a1",
    "appID": "{ec8030f7-c20a-464f-9b0e-13a3a9e97384}",
    "OS": "Linux",
    "reason": "saved-session",
    "revision": revision,
    "locale": "en-US",
    "cpucount": 4,
    "memsize": 7872,
    "arch": "x86-64",
    "version": "3.11.0-13-generic"
  },
  "ver": 1,
  "simpleMeasurements": {
    "savedPings": 2,
    "maximalNumberOfConcurrentThreads": 40,
    "shutdownDuration": 525,
    "js": {
      "customIter": 20,
      "setProto": 0
    },
    "debuggerAttached": 0,
    "startupInterrupted": 0,
    "sessionRestoreRestoring": 3077,
    "delayedStartupFinished": 2985,
    "delayedStartupStarted": 2860,
    "sessionRestoreInitialized": 790,
    "XPI_finalUIStartup": 742,
    "AMI_startup_end": 670,
    "XPI_startup_end": 670,
    "startupCrashDetectionEnd": 32930,
    "startupCrashDetectionBegin": 475,
    "afterProfileLocked": 369,
    "selectProfile": 368,
    "main": 301,
    "start": 6,
    "addonManager": {
      "XPIDB_parseDB_MS": 2,
      "XPIDB_decode_MS": 0,
      "XPIDB_asyncRead_MS": 1,
      "XPIDB_async_load": "BeforeFinalUIStartup"
    },
    "uptime": 1834,
    "firstPaint": 2860,
    "sessionRestored": 3118,
    "createTopLevelWindow": 795,
    "firstLoadURI": 3088,
    "AMI_startup_begin": 480,
    "XPI_startup_begin": 485,
    "XPI_bootstrap_addons_begin": 594,
    "XPI_bootstrap_addons_end": 670
  },
  "slowSQL": {
    "otherThreads": {
      "UPDATE ...": [ 1, 313 ],
      "SELECT ...": [ 8, 2584 ],
      "COMMIT TRANSACTION": [ 1, 187 ]
    },
    "mainThread": {}
  },
  "chromeHangs": { "durations": [], "stacks": [], "memoryMap": [] },
  "lateWrites": { "stacks": [], "memoryMap": [] },
  "addonHistograms": {},
  "addonDetails": {
    "XPI": {
      "tabcount@3greeneggs.com": {
        "location": "app-profile",
        "unpacked": 0
      }
    }
  }
}

test_normal["histograms"] = histograms

try:
    rewritten = converter.rewrite_hists(revision, histograms)
    print "  Original input histograms:"
    print json.dumps(histograms)
    print "  Converted output histograms:"
    print json.dumps(rewritten)
    assert rewritten["STARTUP_CRASH_DETECTED"] == expected_converted_histograms["STARTUP_CRASH_DETECTED"]
    assert rewritten["DOM_TIMERS_FIRED_PER_NATIVE_TIMEOUT"] == expected_converted_histograms["DOM_TIMERS_FIRED_PER_NATIVE_TIMEOUT"]

    test_anr_converted, dimensions = converter.convert_json(json.dumps(test_anr), "20131114")
    assert dimensions[0] == "android-anr-report"
    assert test_anr["ver"] == Converter.VERSION_UNCONVERTED
    assert test_anr_converted["ver"] == Converter.VERSION_CONVERTED

    test_normal_converted, dimensions = converter.convert_json(json.dumps(test_normal), "20131114")
    assert dimensions[0] == "saved-session"
    assert test_normal["ver"] == Converter.VERSION_UNCONVERTED
    assert test_normal_converted["ver"] == Converter.VERSION_CONVERTED
    assert test_normal_converted["histograms"]["STARTUP_CRASH_DETECTED"] == expected_converted_histograms["STARTUP_CRASH_DETECTED"]
    assert test_normal_converted["histograms"]["DOM_TIMERS_FIRED_PER_NATIVE_TIMEOUT"] == expected_converted_histograms["DOM_TIMERS_FIRED_PER_NATIVE_TIMEOUT"]
finally:
    shutil.rmtree(cache_dir)
    pass
