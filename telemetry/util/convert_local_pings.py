#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# This script will let you convert some local pings to the server's stored form.
# Simply restart Firefox a few times to generate some saved sessions, then
# $ cd /path/to/telemetry-server
# $ python -m telemetry.util.convert_local_pings \
#      --input-dir "/path/to/Firefox/Profiles/a7xqbbbu.default" \
#      --output-dir "/path/to/output"
# You can then use the generated output files as input for the MapReduce code
# or for other testing.
# You can also use it to test whether a payload change will cause conversion
# errors.

import json
import os
import re
import sys
import traceback
from argparse import ArgumentParser
from datetime import date

from telemetry.convert import Converter, BadPayloadError
from telemetry.persist import StorageLayout
from telemetry.revision_cache import RevisionCache
from telemetry.telemetry_schema import TelemetrySchema

UUID_PATTERN = re.compile('^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')

def get_pings(dirname):
    pings = []
    for root, dirs, files in os.walk(dirname):
        for f in files:
            if UUID_PATTERN.match(f):
                pings.append(f)
        # Don't recurse into subdirs
        break
    return pings

def main():
    parser = ArgumentParser(description='Convert local Telemetry pings to server storage structure')
    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--schema", type=file, default='./telemetry/telemetry_schema.json')
    parser.add_argument("--histogram-cache-dir", default='/tmp/telemetry_histogram_cache')
    args = parser.parse_args()

    print "Getting pings from", args.input_dir, "converting them and storing them in", args.output_dir
    schema = TelemetrySchema(json.load(args.schema))
    cache = RevisionCache(args.histogram_cache_dir, 'hg.mozilla.org')
    converter = Converter(cache, schema)
    storage = StorageLayout(schema, args.output_dir, 500000000)

    # /Users/mark/Library/Application Support/Firefox/Profiles/a7xqbbbu.default
    # /Users/mark/mozilla/github/telemetry-server/gunk/local_in

    ping_dir = args.input_dir
    ping_files = get_pings(ping_dir)
    if len(ping_files) == 0:
        # Try the usual ping dir (if the user just gave the Profile Dir)
        ping_dir = os.path.join(args.input_dir, "saved-telemetry-pings")
        ping_files = get_pings(ping_dir)

    print "found", len(ping_files), "pings"
    for ping_file in ping_files:
        with open(os.path.join(ping_dir, ping_file), "r") as f:
            ping = json.load(f)
            reason = ping['reason']
            key = ping['slug']
            payload = ping['payload']
            submission_date = date.today().strftime("%Y%m%d")
            dims = schema.dimensions_from(payload, submission_date)
            try:
                parsed_data, dims = converter.convert_obj(payload, dims[-1])
                serialized_data = converter.serialize(parsed_data)
                data_version = Converter.VERSION_CONVERTED
                try:
                    # Write to persistent storage
                    n = storage.write(key, serialized_data, dims, data_version)
                    print "Successfully saved ping", key, "to", n
                except Exception, e:
                    traceback.print_exc()
            except BadPayloadError, e:
                print "Bad Payload:", e.msg
            except Exception, e:
                traceback.print_exc()

if __name__ == '__main__':
    sys.exit(main())
