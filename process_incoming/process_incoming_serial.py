#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import argparse
import imp
import sys
import os
import json
import marshal
import traceback
from datetime import datetime
from multiprocessing import Process
from telemetry.telemetry_schema import TelemetrySchema
from telemetry.persist import StorageLayout
import subprocess
from subprocess import Popen, PIPE
from boto.s3.connection import S3Connection
import telemetry.util.timer as timer


def fetch_s3_files(files, fetch_cwd, bucket_name, aws_key, aws_secret_key):
    result = 0
    if len(files) > 0:
        if not os.path.isdir(fetch_cwd):
            os.makedirs(fetch_cwd)
        fetch_cmd = ["/usr/local/bin/s3funnel"]
        fetch_cmd.append(bucket_name)
        fetch_cmd.append("get")
        fetch_cmd.append("-a")
        fetch_cmd.append(aws_key)
        fetch_cmd.append("-s")
        fetch_cmd.append(aws_secret_key)
        fetch_cmd.append("-t")
        fetch_cmd.append("8")
        start = datetime.now()
        result = subprocess.call(fetch_cmd + files, cwd=fetch_cwd)
        duration_sec = timer.delta_sec(start)
        # TODO: verify MD5s
        downloaded_bytes = sum([ os.path.getsize(os.path.join(fetch_cwd, f)) for f in files ])
        downloaded_mb = downloaded_bytes / 1024.0 / 1024.0
        print "Downloaded %.2fMB in %.2fs (%.2fMB/s)" % (downloaded_mb, duration_sec, downloaded_mb / duration_sec)
    return result

def split_raw_logs(files, output_dir, schema_file):
    if not os.path.isdir(output_dir):
        os.makedirs(output_dir)
    split_cmd = ["python", "telemetry/util/split_raw_log.py"]
    split_cmd.append("-o")
    split_cmd.append(output_dir)
    split_cmd.append("-t")
    split_cmd.append(schema_file)
    split_cmd.append("-i")
    for raw_log in files:
       result = subprocess.call(split_cmd + [raw_log])
       if result != 0:
           return result
    return 0

def convert_split_logs(output_dir):
   print "Converting logs in", output_dir
   # TODO: force it to archive all log files, not just ones up to yesterday
   convert_cmd = ["/bin/bash", "util/archive_logs.sh", ".", output_dir]
   return subprocess.call(convert_cmd)

def export_converted_logs(output_dir, bucket_name, aws_key, aws_secret_key):
    export_cmd = ["python", "telemetry/util/export.py", "-d", output_dir, "-k", aws_key, "-s", aws_secret_key, "-b", bucket_name]
    return subprocess.call(export_cmd)


def main():
    parser = argparse.ArgumentParser(description='Process incoming Telemetry data', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("incoming_bucket", help="The S3 bucket containing incoming files")
    parser.add_argument("publish_bucket", help="The S3 bucket to save processed files")
    parser.add_argument("-n", "--num-helpers", metavar="N", help="Start N helper processes", type=int, default=1)
    parser.add_argument("-k", "--aws-key", help="AWS Key", required=True)
    parser.add_argument("-s", "--aws-secret-key", help="AWS Secret Key", required=True)
    parser.add_argument("-w", "--work-dir", help="Location to cache downloaded files", required=True)
    parser.add_argument("-o", "--output-dir", help="Base dir to store processed data", required=True)
    parser.add_argument("-i", "--input-files", help="File containing a list of keys to process", type=file)
    parser.add_argument("-t", "--telemetry-schema", help="Location of the desired telemetry schema", required=True)
    args = parser.parse_args()

    # TODO: keep track of partial success so that subsequent runs are idempotent.

    start = datetime.now()
    conn = S3Connection(args.aws_key, args.aws_secret_key)
    incoming_bucket = conn.get_bucket(args.incoming_bucket)
    incoming_filenames = []
    if args.input_files:
        print "Fetching file list from file", args.input_files
        incoming_filenames = [ l.strip() for l in args.input_files.readlines() ]
    else:
        print "Fetching file list from S3..."
        for f in incoming_bucket.list():
            incoming_filenames.append(f.name)
    print "Done"

    for f in incoming_filenames:
        print "  ", f

    result = 0
    print "Downloading", len(incoming_filenames), "files..."
    result = fetch_s3_files(incoming_filenames, args.work_dir, args.incoming_bucket, args.aws_key, args.aws_secret_key)
    if result != 0:
        print "Error downloading files. Return code of s3funnel was", result
        return result
    print "Done"

    print "Splitting raw logs..."
    local_filenames = [os.path.join(args.work_dir, f) for f in incoming_filenames]
    result = split_raw_logs(local_filenames, args.output_dir, args.telemetry_schema)
    if result != 0:
        print "Error splitting logs. Return code was", result
        return result
    print "Done"

    print "Converting split logs..."
    result = convert_split_logs(args.output_dir)
    if result != 0:
        print "Error converting logs. Return code was", result
        return result
    print "Done"

    print "Exporting converted logs back to S3..."
    result = export_converted_logs(args.output_dir, args.publish_bucket, args.aws_key, args.aws_secret_key)
    if result != 0:
        print "Error exporting logs. Return code was", result
        return result
    print "Done"

    print "Removing processed logs from S3..."
    for f in incoming_filenames:
        print "  Deleting", f
        incoming_bucket.delete_key(f)
    print "Done"

    duration = timer.delta_sec(start)
    print "All done in %.2fs" % (duration)
    return 0

if __name__ == "__main__":
    sys.exit(main())
