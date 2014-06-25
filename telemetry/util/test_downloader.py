#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import os
import shutil
import sys
import telemetry.util.s3 as s3util
from telemetry.telemetry_schema import TelemetrySchema

test_dir = "/tmp/test_telemetry_loader"

def test_list(d):
    files = ["saved_session/Fennec/nightly/26.0a1/20130806030203.20131021.v2.log.8b30fadcf5b84df8b860bce47a23146a.lzma","saved_session/Firefox/release/24.0/20130910160258.20131002.v2.log.264b07580df349678b1247d13ea2e6f3.lzma","saved_session/Firefox/release/24.0/20130910160258.20131002.v2.log.25b53e7042c74188b08d71ce32e87237.lzma","saved_session/Firefox/release/24.0/20130910160258.20131002.v2.log.29afd7a250154729bd53c20253f8af78.lzma","saved_session/Firefox/release/24.0/20130910160258.20131002.v2.log.2bcf0e3a267d49f9a5256899f33ca484.lzma","saved_session/Fennec/nightly/26.0a1/20130806030203.20131021.v2.log.BOGUS.lzma"]
    successfully_downloaded = []
    failfully_downloaded = []
    #for i in range(10):
    #    files.append(files[0])
    for f, r, err in d.get_list(files):
        if err is not None:
            print err
            failfully_downloaded.append(f)
        else:
            print "Downloaded", f
            successfully_downloaded.append(f)
    assert len(failfully_downloaded) == 1
    assert len(successfully_downloaded) == (len(files) - len(failfully_downloaded))
    for f in successfully_downloaded:
        print "Should exist:", f
        assert os.path.exists(f)
    for f in failfully_downloaded:
        assert not os.path.exists(f)

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

    successfully_downloaded = []
    failfully_downloaded = []
    for f, r, err in d.get_schema(schema):
        if err is not None:
            print err
            failfully_downloaded.append(f)
        else:
            print "Downloaded", f
            successfully_downloaded.append(f)
    assert len(failfully_downloaded) == 0
    print "Successfully downloaded", len(successfully_downloaded)
    assert len(successfully_downloaded) == 20

def main():
    try:
        assert not os.path.exists(test_dir)
        os.makedirs(test_dir)
        num_procs = 15
        print "Running with", num_procs, "processes."
        d = s3util.Loader(test_dir, "telemetry-published-v2", poolsize=num_procs)
        test_list(d)
        test_schema(d)
    finally:
        shutil.rmtree(test_dir)
    return 0

if __name__ == "__main__":
    sys.exit(main())
