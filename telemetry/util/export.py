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
import simplejson as json
from telemetry.persist import StorageLayout
from datetime import datetime
import telemetry.util.timer as timer
import telemetry.util.s3 as s3util
from boto.s3.connection import S3Connection
from boto.sqs.message import Message
from boto.exception import S3ResponseError
import boto.sqs


class Exporter:
    """A class for exporting archived payloads to long-term storage (Amazon S3)"""
    UPLOADABLE_PATTERN = re.compile("^.*\\" + StorageLayout.COMPRESSED_SUFFIX + "$")
    # Minimum size in bytes (to support skipping truncated files)
    MIN_UPLOADABLE_SIZE = 50

    def __init__(self, config, data_dir, pattern, keep_backups=False):
        self.bucket = config["incoming_bucket"]
        self.queue = config.get("incoming_queue", None)
        self.aws_key = config.get("aws_key", None)
        self.aws_secret_key = config.get("aws_secret_key", None)
        self.aws_region = config.get("aws_region", None)
        self.data_dir = data_dir
        self.pattern = pattern
        self.keep_backups = keep_backups
        if self.queue is not None:
            # Get a connection to the Queue
            conn = boto.sqs.connect_to_region(self.aws_region,
                    aws_access_key_id=self.aws_key,
                    aws_secret_access_key=self.aws_secret_key)

            # This gets the queue if it already exists, otherwise creates it
            # using the supplied default timeout (in seconds).
            self.q_incoming = conn.create_queue(self.queue, 90 * 60)
        self.s3loader = s3util.Loader(self.data_dir, self.bucket, self.aws_key, self.aws_secret_key)

        # Make sure the target S3 bucket exists.
        s3conn = S3Connection(self.aws_key, self.aws_secret_key)
        try:
            print "Verifying that we can write to", self.bucket
            b = s3conn.get_bucket(self.bucket)
            print "Looks good!"
        except S3ResponseError:
            print "Bucket", self.bucket, "not found.  Attempting to create it."
            b = s3conn.create_bucket(self.bucket)

    def enqueue_incoming(self, filename):
        if self.queue is None:
            return

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

    def export(self, uploadables):
        if len(uploadables) == 0:
            print "Nothing to do!"
            return 0
        print "Found", len(uploadables), "files"

        fail_count = 0
        start = datetime.now()
        total_size = 0
        for local, remote, err in self.s3loader.put_list(uploadables):
            if err is None:
                # Great Success! Delete it locally.
                total_size += os.path.getsize(local)
                if self.keep_backups:
                    # Keep a copy of the original, just in case.
                    os.rename(local, local + ".uploaded")
                else:
                    os.remove(local)
                # Send a message to SQS
                self.enqueue_incoming(remote)

            else:
                fail_count += 1
                print "Failed to upload '{0}' to bucket {1} as '{2}':".format(local, self.bucket, remote), err
        sec = timer.delta_sec(start)
        total_mb = float(total_size) / 1024.0 / 1024.0
        print "Transferred %.2fMB in %.2fs (%.2fMB/s)" % (total_mb, sec, total_mb / sec)
        # TODO: log the transfer stats properly.

        # Return zero for overall success or the number of failures.
        return fail_count

def main(argv=None):
    parser = argparse.ArgumentParser(description="Export Telemetry data")
    parser.add_argument("-d", "--data-dir", help="Path to the root of the telemetry data", required=True)
    parser.add_argument("-p", "--file-pattern", help="Filenames must match this regular expression to be uploaded", default=Exporter.UPLOADABLE_PATTERN)
    parser.add_argument("-c", "--config", help="AWS Config file", required=True, type=file)
    parser.add_argument("-b", "--bucket", help="S3 Bucket name")
    parser.add_argument("-q", "--queue", help="SQS Queue name")
    parser.add_argument("-k", "--aws-key", help="AWS Key")
    parser.add_argument("-s", "--aws-secret-key", help="AWS Secret Key")
    parser.add_argument("-r", "--aws-region", help="AWS Region")
    parser.add_argument("-l", "--loop", help="Run in a loop and keep watching for more files to export", action="store_true")
    parser.add_argument("--keep-backups", help="Keep original files after uploading (rename them to X.uploaded)", action="store_true")
    args = parser.parse_args()

    # Validate args:
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

    config = None
    try:
        config = json.load(args.config)
    except Exception, e:
        print "ERROR: could not parse config file:", e
        return 4

    # Override config file with args if present:
    if args.bucket is not None:
        config["incoming_bucket"] = args.bucket
    if args.queue is not None:
        config["incoming_queue"] = args.queue
    if args.aws_key is not None:
        config["aws_key"] = args.aws_key
    if args.aws_secret_key is not None:
        config["aws_secret_key"] = args.aws_secret_key
    if args.aws_region is not None:
        config["aws_region"] = args.aws_region

    exporter = Exporter(config, args.data_dir, pattern, args.keep_backups)

    if not args.loop:
        return exporter.export(exporter.find_uploadables(args.data_dir))

    while True:
        uploadables = exporter.find_uploadables(args.data_dir)
        if len(uploadables) == 0:
            print "No files to export yet.  Sleeping for a while..."
            time.sleep(10)
            continue

        print "Processing", len(uploadables), "uploadables:"
        for u in uploadables:
            print "  ", u
        err_count = exporter.export(uploadables)
        if err_count > 0:
            print "ERROR: There were", err_count, "errors uploading."

if __name__ == "__main__":
    sys.exit(main())
