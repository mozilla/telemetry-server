#!/usr/bin/env python
# encoding: utf-8
"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""

import argparse
import re
import os
import sys
import getopt
import revision_cache
from telemetry_schema import TelemetrySchema
from persist import StorageLayout
import traceback
import persist
from datetime import date
from datetime import datetime
import time
import hashlib
import subprocess
from boto.s3.connection import S3Connection
from boto.exception import S3ResponseError


class Exporter:
    """A class for exporting archived payloads to long-term storage (Amazon S3)"""
    S3F_PATH = "/usr/local/bin/s3funnel"
    S3F_THREADS = 8
    UPLOADABLE_PATTERN = re.compile("^.*\\" + StorageLayout.COMPRESSED_SUFFIX + "$")
    MIN_UPLOADABLE_SIZE = 50

    def __init__(self, data_dir, bucket, aws_key, aws_secret_key, batch_size):
        self.data_dir = data_dir
        self.bucket = bucket
        self.aws_key = aws_key
        self.aws_secret_key = aws_secret_key
        self.batch_size = batch_size
        self.s3f_cmd = [Exporter.S3F_PATH, bucket, "put", "-a", aws_key,
                "-s", aws_secret_key, "-t", str(Exporter.S3F_THREADS),
                "--put-only-new", "--del-prefix", "./", "--put-full-path"]

    def clean_filename(self, filename):
        if filename[0:2] == "./":
            return filename[2:]
        return filename

    # might as well return the size too...
    def md5file(self, filename):
        md5 = hashlib.md5()
        size = 0
        with open(filename, "rb") as data:
            while True:
                chunk = data.read(8192)
                if not chunk:
                    break
                md5.update(chunk)
                size += len(chunk)
        return md5.hexdigest(), size

    def export_batch(self, files):
        # Time the s3funnel call:
        start = datetime.now()
        result = subprocess.call(self.s3f_cmd + files, cwd=self.data_dir)
        delta = (datetime.now() - start)
        sec = float(delta.seconds) + float(delta.microseconds) / 1000000.0

        total_size = 0
        if result == 0:
            # Success! Verify each file's checksum, then truncate it.
            conn = S3Connection(self.aws_key, self.aws_secret_key)
            bucket = conn.get_bucket(self.bucket)
            for f in files:
                # Verify checksum and track cumulative size so we can figure out MB/s
                full_filename = os.path.join(self.data_dir, f)
                md5, size = self.md5file(full_filename)
                total_size += size
                key = bucket.get_key(self.clean_filename(f))

                # Strip quotes from md5
                remote_md5 = key.etag[1:-1]
                if md5 != remote_md5:
                    print "ERROR: %s failed checksum verification: Local=%s, Remote=%s" % (f, md5, remote_md5)
                    result = -1
                else:
                    # Validation passed.
                    # Keep a copy of the original, just in case.
                    # TODO: This can either be removed for production use,
                    #       or we can have yet another cleanup job to remove
                    #       them on a schedule.
                    os.rename(full_filename, full_filename + ".uploaded")

                    # Create / Truncate: we must keep the original file around to
                    # properly calculate the next archived log number, ie if we
                    # are uploading whatever.log.5.lzma, the next one should still
                    # be whatever.log.6.lzma.
                    h = open(full_filename, "w")
                    h.close()
        else:
            print "Failed to upload one or more files in the current batch. Error code was", result

        total_mb = float(total_size) / 1024.0 / 1024.0
        # Don't divide by zero.
        if sec == 0.0:
            sec += 0.00001

        print "Transferred %.2fMB in %.2fs (%.2fMB/s)" % (total_mb, sec, total_mb / sec)
        return result

    def batches(self, batch_size, one_list):
        split = []
        current = 0
        while current + batch_size < len(one_list):
            split.append(one_list[current:current+batch_size])
            current += batch_size
        if current < len(one_list):
            split.append(one_list[current:])
        return split

    def remove_data_dir(self, full_file):
        if full_file.startswith(self.data_dir):
            chopped = full_file[len(self.data_dir):]
            if chopped[0] == "/":
                chopped = chopped[1:]
            return chopped
        else:
            print "ERROR: cannot remove", self.data_dir, "from", full_file
            raise ValueError("Invalid full filename: " + str(full_file))

    def export(self):
        # Find all uploadable files
        uploadables = []
        for root, dirs, files in os.walk(self.data_dir):
            for f in files:
                m = Exporter.UPLOADABLE_PATTERN.match(f)
                if m:
                    full_file = os.path.join(root, f)
                    file_size = os.path.getsize(full_file)
                    if file_size >= Exporter.MIN_UPLOADABLE_SIZE:
                        relative_file = self.remove_data_dir(full_file)
                        uploadables.append(relative_file)
                        # TODO: we may also want to check a "minimum time since
                        #       last modification" so that we don't upload
                        #       partial compressed files.  Note that the md5
                        #       verification should take care of this, but it
                        #       would save on transfer time/cost to check here.
                    else:
                        print full_file, "is too small to upload:", file_size, "bytes"

        if len(uploadables) == 0:
            print "Nothing to do!"
            return 0

        # Split into batches
        batches = self.batches(self.batch_size, uploadables)
        print "Found", len(uploadables), "files:", len(batches), "batches of size", len(batches[0])

        # Export each batch
        fail_count = 0
        batch_count = 0
        for batch in batches:
            batch_count += 1
            print "Exporting batch", batch_count, "of", len(batches)
            try:
                batch_response = self.export_batch(batch)
                if batch_response != 0:
                    print "Batch", batch_count, "failed: returned", batch_response
                    fail_count += 1
            except S3ResponseError, e:
                print "Batch", batch_count, "failed:", e
                fail_count += 1

        # Return zero for overall success or the number of batches with errors.
        return fail_count

def main(argv=None):
    parser = argparse.ArgumentParser(description="Export Telemetry data")
    parser.add_argument("-d", "--data-dir", help="Path to the root of the telemetry data", required=True)
    parser.add_argument("-b", "--bucket", help="S3 Bucket name", default="mreid-telemetry-dev")
    parser.add_argument("-k", "--aws-key", help="AWS Key", required=True)
    parser.add_argument("-s", "--aws-secret-key", help="AWS Secret Key", required=True)
    parser.add_argument("-B", "--batch-size", help="Number of files to upload at a time", default=8)
    args = parser.parse_args()

    # Validate args:
    if not os.path.isfile(Exporter.S3F_PATH):
        print "ERROR: s3funnel not found at", s3f_path
        print "You can get it from github: https://github.com/sstoiana/s3funnel"
        return -1

    if not os.path.isdir(args.data_dir):
        print "ERROR:", args.data_dir, "is not a valid directory"
        parser.print_help()
        return 2
    exporter = Exporter(args.data_dir, args.bucket, args.aws_key, args.aws_secret_key, args.batch_size)
    return exporter.export()

if __name__ == "__main__":
    sys.exit(main())
