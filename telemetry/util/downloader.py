#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import sys
import telemetry.util.s3 as s3util
from telemetry.telemetry_schema import TelemetrySchema

def test_list(d):
    files = ["saved_session/Fennec/nightly/26.0a1/20130806030203.20131021.v2.log.8b30fadcf5b84df8b860bce47a23146a.lzma","saved_session/Firefox/release/24.0/20130910160258.20131002.v2.log.264b07580df349678b1247d13ea2e6f3.lzma","saved_session/Firefox/release/24.0/20130910160258.20131002.v2.log.25b53e7042c74188b08d71ce32e87237.lzma","saved_session/Firefox/release/24.0/20130910160258.20131002.v2.log.29afd7a250154729bd53c20253f8af78.lzma","saved_session/Firefox/release/24.0/20130910160258.20131002.v2.log.2bcf0e3a267d49f9a5256899f33ca484.lzma","saved_session/Fennec/nightly/26.0a1/20130806030203.20131021.v2.log.BOGUS.lzma"]
    for i in range(10):
        files.append(files[0])
    for f, err in d.fetch_list(files):
        if err is not None:
            print err
        else:
            print "Downloaded", f

def test_schema(d):
    schema_spec = {
      "version": 1,
      "dimensions": [
        {
          "field_name": "reason",
          "allowed_values": ["saved-session"]
        },
        {
          "field_name": "appName",
          "allowed_values": ["Firefox"]
        },
        {
          "field_name": "appUpdateChannel",
          "allowed_values": ["nightly"]
        },
        {
          "field_name": "appVersion",
          "allowed_values": ["27.0a1"]
        },
        {
          "field_name": "appBuildID",
          "allowed_values": ["20130918030202"]
        },
        {
          "field_name": "submission_date",
          "allowed_values": ["20131001"]
        }
      ]
    }

    schema = TelemetrySchema(schema_spec)
    for f, err in d.fetch_schema(schema):
        if err is not None:
            print err
        else:
            print "Downloaded", f

def main():
    print "Ohai."
    num_procs = 15
    print "Running with", num_procs, "processes."
    d = s3util.Downloader("/home/mark/tmp/s3downloader/out", "telemetry-published-v1", poolsize=num_procs)
    test_schema(d)
    return 0

if __name__ == "__main__":
    sys.exit(main())
