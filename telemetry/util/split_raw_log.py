# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import sys, os, argparse
import simplejson as json
from telemetry.persist import StorageLayout
from telemetry.telemetry_schema import TelemetrySchema
from datetime import date, datetime
import telemetry.util.timer as timer
import telemetry.util.files as fileutil

def main():
    parser = argparse.ArgumentParser(description='Split raw logs into partitioned files.', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-m", "--max-output-size", metavar="N", help="Rotate output files after N bytes", type=int, default=500000000)
    parser.add_argument("-i", "--input-file", help="Filename to read from", required=True)
    parser.add_argument("-o", "--output-dir", help="Base directory to store split files", required=True)
    parser.add_argument("-t", "--telemetry-schema", help="Filename of telemetry schema spec", required=True)
    parser.add_argument("-f", "--file-version", help="Log file version (if omitted, we'll guess)")
    args = parser.parse_args()

    schema_data = open(args.telemetry_schema)
    schema = TelemetrySchema(json.load(schema_data))
    schema_data.close()

    storage = StorageLayout(schema, args.output_dir, args.max_output_size)

    expected_dim_count = len(schema._dimensions)

    if not os.path.isdir(args.output_dir):
        os.makedirs(args.output_dir)

    record_count = 0
    bad_record_count = 0
    bytes_read = 0
    start = datetime.now()
    file_version = args.file_version
    if not file_version:
        file_version = fileutil.detect_file_version(args.input_file)
    for r in fileutil.unpack(args.input_file, file_version=file_version):
        record_count += 1
        if r.error:
            bad_record_count += 1
            continue
        # Incoming timestamps are in milliseconds, so convert to POSIX first
        # (ie. seconds)
        submission_date = date.fromtimestamp(r.timestamp / 1000).strftime("%Y%m%d")
        # Deal with unicode
        path = unicode(r.path, errors="replace")
        data = unicode(r.data, errors="replace")

        bytes_read += r.len_ip + r.len_path + r.len_data + fileutil.RECORD_PREAMBLE_LENGTH[file_version]
        #print "Path for record", record_count, path, "length of data:", r.len_data, "data:", data[0:5] + "..."

        path_components = path.split("/")
        if len(path_components) != expected_dim_count:
            # We're going to pop the ID off, but we'll also add the submission
            # date, so it evens out.
            print "Found an invalid path in record", record_count, path
            bad_record_count += 1
            continue

        key = path_components.pop(0)
        info = {}
        info["reason"] = path_components.pop(0)
        info["appName"] = path_components.pop(0)
        info["appVersion"] = path_components.pop(0)
        info["appUpdateChannel"] = path_components.pop(0)
        info["appBuildID"] = path_components.pop(0)
        dimensions = schema.dimensions_from(info, submission_date)
        #print "  Converted path to filename", schema.get_filename(args.output_dir, dimensions)
        storage.write(key, data, dimensions)
    duration = timer.delta_sec(start)
    mb_read = bytes_read / 1024.0 / 1024.0
    print "Read %.2fMB in %.2fs (%.2fMB/s), %d of %d records were bad" % (mb_read, duration, mb_read / duration, bad_record_count, record_count)
    return 0

if __name__ == "__main__":
    sys.exit(main())
