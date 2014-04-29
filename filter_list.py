# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import simplejson as json
import os
import sys
import shutil
import unittest
from telemetry.telemetry_schema import TelemetrySchema

spec = {
    "version": 1,
    "dimensions": [
        {
            "field_name": "reason",
            "allowed_values": ["saved_session"]
        },
        {
            "field_name": "appName",
            "allowed_values": "*"
        },
        {
            "field_name": "appUpdateChannel",
            "allowed_values": ["release", "aurora", "nightly", "beta", "nightly-ux"]
        },
        {
            "field_name": "appVersion",
            "allowed_values": "*"
        },
        {
            "field_name": "appBuildID",
            "allowed_values": "*"
        },
        {
            "field_name": "submission_date",
            "allowed_values": {
                "max": "20140424"
            }
        }
    ]
}

print "Reading list from {} and writing to {}".format(sys.argv[1], sys.argv[2])
input_list = open(sys.argv[1], "r")
output_list = open(sys.argv[2], "w")

schema = TelemetrySchema(spec)
allowed = schema.sanitize_allowed_values()

include_count = 0
exclude_count = 0
errors = 0
for line in input_list:
    include = True
    try:
        dims = schema.get_dimensions(".", line)
        #print line.strip(), "=>", dims
        for i in range(len(allowed)):
            if not schema.is_allowed(dims[i], allowed[i]):
                include = False
                break
    except ValueError:
        include = False
        errors += 1
    if include:
        include_count += 1
        output_list.write(line)
    else:
        exclude_count += 1
    if (include_count + exclude_count) % 50000 == 0:
        print "Included:", include_count, "Excluded:", exclude_count, "Errors:", errors

print "Overall, Included:", include_count, "Excluded:", exclude_count, "Errors:", errors
input_list.close()
output_list.close()
