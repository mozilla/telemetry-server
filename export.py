#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import argparse
import re
import os
import sys
import time
from persist import StorageLayout
from datetime import datetime
import util.files as fileutil
import subprocess
from boto.s3.connection import S3Connection
from boto.sqs.message import Message
from boto.exception import S3ResponseError
import util.timer as timer
import aws_provisioning.aws_util as aws_util


class Exporter:
    """A class for exporting archived payloads to long-term storage (Amazon S3)"""
    S3F_PATH = "/usr/local/bin/s3funnel"
    S3F_THREADS = 8
    UPLOADABLE_PATTERN = re.compile("^.*\\" + StorageLayout.COMPRESSED_SUFFIX + "$")
    # Minimum size in bytes (to support skipping truncated files)
    MIN_UPLOADABLE_SIZE = 50

    def __init__(self, bucket, aws_key, aws_secret_key, aws_region, batch_size, pattern, queue=None, keep_backups=False, remove_files=True):
        self.bucket = bucket
        self.queue = queue
        self.aws_key = aws_key
        self.aws_secret_key = aws_secret_key
        self.aws_region = aws_region
        self.batch_size = batch_size
        self.pattern = pattern
        self.remove_files = remove_files
        self.keep_backups = keep_backups
        self.q_incoming = None
        self.s3f_cmd = [Exporter.S3F_PATH, bucket, "put", "-a", aws_key,
                "-s", aws_secret_key, "-t", str(Exporter.S3F_THREADS),
                "--put-only-new", "--put-full-path"]

    def export_batch(self, data_dir, conn, bucket, files):
        # Time the s3funnel call:
        start = datetime.now()
        result = subprocess.call(self.s3f_cmd + files, cwd=data_dir)
        sec = timer.delta_sec(start)

        total_size = 0
        if result == 0:
            # Success! Verify each file's checksum, then truncate it.
            for f in files:
                # Verify checksum and track cumulative size so we can figure out MB/s
                full_filename = os.path.join(data_dir, f)
                md5, size = fileutil.md5file(full_filename)
                if size < Exporter.MIN_UPLOADABLE_SIZE:
                    # Check file size again when uploading in case it has been
                    # concurrently uploaded / truncated elsewhere.
                    print "Skipping upload for tiny file:", f
                    continue

                total_size += size

                # f is the key name - it does not include the full path to the
                # data dir. Try to fetch it a couple of times
                for i in range(3):
                    key = bucket.get_key(f)
                    if key is not None:
                        break
                if key is None:
                    print "ERROR: Failed to fetch key:", f
                    result = -2
                    continue

                # Strip quotes from md5
                remote_md5 = key.etag[1:-1]
                if md5 != remote_md5:
                    print "ERROR: %s failed checksum verification: Local=%s, Remote=%s" % (f, md5, remote_md5)
                    result = -1
                else:
                    # Validation passed. Time to clean up the file.
                    if self.keep_backups:
                        # Keep a copy of the original, just in case.
                        os.rename(full_filename, full_filename + ".uploaded")

                    # Create / Truncate: we must keep the original file around to
                    # properly calculate the next archived log number, ie if we
                    # are uploading whatever.log.5.lzma, the next one should still
                    # be whatever.log.6.lzma.
                    # TODO: if we switch to using UUIDs in the filename, we can
                    #       stop keeping the dummy files around.
                    if self.remove_files:
                        if not self.keep_backups:
                            os.remove(full_filename)
                            # Otherwise it was renamed already.
                    else:
                        h = open(full_filename, "w")
                        h.close()
                    # Send a message to SQS
                    self.enqueue_incoming(f)
        else:
            print "Failed to upload one or more files in the current batch. Error code was", result

        total_mb = float(total_size) / 1024.0 / 1024.0
        print "Transferred %.2fMB in %.2fs (%.2fMB/s)" % (total_mb, sec, total_mb / sec)
        return result

    def enqueue_incoming(self, filename):
        if self.queue is None:
            return

        if self.q_incoming is None:
            # Get a connection to the Queue if needed.
            conn = aws_util.connect_sqs(self.aws_region, self.aws_key, self.aws_secret_key)

            # This gets the queue if it already exists, otherwise creates it
            # using the supplied default timeout (in seconds).
            self.q_incoming = conn.create_queue(self.queue, 90 * 60)

        m = Message()
        m.set_body(filename)

        # Retry several times:
        for i in range(10):
            try:
                status = self.q_incoming.write(m)
                if status:
                    print "Successfully enqueued", filename
                    break
            except Exception, e:
                print "Failed to enqueue:", filename, "Error:", e

    def batches(self, batch_size, one_list):
        split = []
        current = 0
        while current + batch_size < len(one_list):
            split.append(one_list[current:current+batch_size])
            current += batch_size
        if current < len(one_list):
            split.append(one_list[current:])
        return split

    def strip_data_dir(self, data_dir, full_file):
        if full_file.startswith(data_dir):
            chopped = full_file[len(data_dir):]
            if chopped[0] == "/":
                chopped = chopped[1:]
            return chopped
        else:
            print "ERROR: cannot remove", data_dir, "from", full_file
            raise ValueError("Invalid full filename: " + str(full_file))

    def find_uploadables(self, data_dir):
        # Find all uploadable files relative to the given base dir.
        uploadables = []
        for root, dirs, files in os.walk(data_dir):
            for f in files:
                m = self.pattern.match(f)
                if m:
                    full_file = os.path.join(root, f)
                    file_size = os.path.getsize(full_file)
                    if file_size >= Exporter.MIN_UPLOADABLE_SIZE:
                        relative_file = self.strip_data_dir(data_dir, full_file)
                        uploadables.append(relative_file)
                        # TODO: we may also want to check a "minimum time since
                        #       last modification" so that we don't upload
                        #       partial compressed files.  Note that the md5
                        #       verification should take care of this, but it
                        #       would save on transfer time/cost to check here.
        return uploadables

    def export(self, data_dir, uploadables):
        if len(uploadables) == 0:
            print "Nothing to do!"
            return 0

        # Split into batches
        batches = self.batches(self.batch_size, uploadables)
        print "Found", len(uploadables), "files:", len(batches), "batches of size", len(batches[0])

        # Make sure the target S3 bucket exists.
        conn = S3Connection(self.aws_key, self.aws_secret_key)
        try:
            print "Verifying that we can write to", self.bucket
            bucket = conn.get_bucket(self.bucket)
            print "Looks good!"
        except S3ResponseError:
            print "Bucket", self.bucket, "not found.  Attempting to create it."
            bucket = conn.create_bucket(self.bucket)

        # Export each batch
        fail_count = 0
        batch_count = 0
        for batch in batches:
            batch_count += 1
            print "Exporting batch", batch_count, "of", len(batches)
            try:
                batch_response = self.export_batch(data_dir, conn, bucket, batch)
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
    parser.add_argument("-p", "--file-pattern", help="Filenames must match this regular expression to be uploaded", default=Exporter.UPLOADABLE_PATTERN)
    parser.add_argument("-b", "--bucket", help="S3 Bucket name", required=True)
    parser.add_argument("-q", "--queue", help="SQS Queue name")
    parser.add_argument("-k", "--aws-key", help="AWS Key", required=True)
    parser.add_argument("-s", "--aws-secret-key", help="AWS Secret Key", required=True)
    parser.add_argument("-r", "--aws-region", help="AWS Region", default="us-west-2")
    parser.add_argument("-l", "--loop", help="Run in a loop and keep watching for more files to export", action="store_true")
    parser.add_argument("--remove-files", help="Remove files after successfully uploading (default is to truncate them)", action="store_true")
    parser.add_argument("--keep-backups", help="Keep original files after uploading (rename them to X.uploaded)", action="store_true")
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

    pattern = None
    try:
        pattern = re.compile(args.file_pattern)
    except Exception, e:
        print "ERROR: invalid file pattern:", args.file_pattern, " (must be a valid regex)"
        return 3

    exporter = Exporter(args.bucket, args.aws_key, args.aws_secret_key, args.aws_region, args.batch_size, pattern, args.queue, args.keep_backups, args.remove_files)

    if not args.loop:
        return exporter.export(args.data_dir, exporter.find_uploadables(args.data_dir))

    while True:
        uploadables = exporter.find_uploadables(args.data_dir)
        if len(uploadables) == 0:
            print "No files to export yet.  Sleeping for a while..."
            time.sleep(60)
            continue

        print "Processing", len(uploadables), "uploadables:"
        for u in uploadables:
            print "  ", u
        err_count = exporter.export(args.data_dir, uploadables)
        if err_count > 0:
            print "ERROR: There were", err_count, "errors uploading."


if __name__ == "__main__":
    sys.exit(main())
