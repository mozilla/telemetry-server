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
import errno
from datetime import datetime
from multiprocessing import Process
from telemetry.telemetry_schema import TelemetrySchema
from telemetry.util.compress import CompressedFile
import telemetry.util.s3 as s3util
import telemetry.util.timer as timer
import subprocess
import csv
import signal
import cProfile
import collections
import gc
try:
    from boto.s3.connection import S3Connection
    BOTO_AVAILABLE=True
except ImportError:
    BOTO_AVAILABLE=False


class Fetcher:
    """A class for fetching filtered files from S3"""
    # 1. read input filter
    # 2a. generate filtered list of remote input files
    # 3. load mapper
    # 4. spawn N processes
    # 5. distribute files among processes
    # x. Download everything
    # x. Rename into the target dir on completion.

    def __init__(self, config):
        # Sanity check args.
        if config.get("num_loaders") <= 0:
            raise ValueError("Number of loaders must be greater than zero")
        if config.get("num_dirs") <= 0:
            raise ValueError("Number of directories must be greater than zero")

        if not os.path.isdir(config.get("base_dir")):
            # TODO: try to create it?
            raise ValueError("Base dir must be a valid directory")

        if not os.path.isfile(config.get("filter")):
            raise ValueError("Input filter must be a valid json file")

        self._base_dir = config.get("base_dir")
        if self._base_dir[-1] == os.path.sep:
            self._base_dir = self._base_dir[0:-1]

        with open(config.get("filter"), "r") as filter_file:
            self._filter = TelemetrySchema(json.load(filter_file))

        self._allowed_values = self._filter.sanitize_allowed_values()

        self._num_loaders = config.get("num_loaders")
        self._num_dirs = config.get("num_dirs")
        self._bucket_name = config.get("bucket")
        self._aws_key = config.get("aws_key")
        self._aws_secret_key = config.get("aws_secret_key")
        self._verbose = config.get("verbose")
        self._max_bytes = config.get("max_bytes")

    def fetch(self):
        remotes = self.filter()
        remote_names = [ r.name for r in remotes ]

        result = 0

        fetch_cwd = os.path.join(self._base_dir, "in")
        if not os.path.isdir(fetch_cwd):
            os.makedirs(fetch_cwd)

        for i in range(0, self._num_dirs):
            outdir = os.path.join(self._base_dir, "out", str(i))
            if not os.path.isdir(outdir):
                os.makedirs(outdir)
        # TODO: build retry count into Loader.
        loader = s3util.Loader(fetch_cwd, self._bucket_name,
            aws_key=self._aws_key, aws_secret_key=self._aws_secret_key,
            poolsize=self._num_loaders)

        start = datetime.now()
        downloaded_bytes = 0
        file_counter = 1
        for local, remote, err in loader.get_list(remote_names):
            if err is None:
                if self._verbose:
                    print "Downloaded", remote
                downloaded_bytes += os.path.getsize(local)
                out_name = os.path.join(self._base_dir, "out", str(file_counter % self._num_dirs), str(file_counter))
                if self._verbose:
                    print "Moving", local, "to", out_name
                os.rename(local, out_name)
                if self._max_bytes >= 0 and downloaded_bytes > self._max_bytes:
                    break
                file_counter += 1
            else:
                print "Failed to download", remote
                result += 1
        duration_sec = timer.delta_sec(start)
        downloaded_mb = float(downloaded_bytes) / 1024.0 / 1024.0
        print "Downloaded %.2fMB in %.2fs (%.2fMB/s)" % (downloaded_mb, duration_sec, downloaded_mb / duration_sec)
        if file_counter < len(remote_names):
            print "Downloaded", file_counter, "of", len(remote_names), "files. Hit max_bytes =", self._max_bytes
        return result

    def filter(self):
        if self._verbose:
            print "Fetching file list from S3..."

        # Plain boto should be fast enough to list bucket contents.
        if self._aws_key is not None:
            conn = S3Connection(self._aws_key, self._aws_secret_key)
        else:
            conn = S3Connection()
        bucket = conn.get_bucket(self._bucket_name)
        start = datetime.now()
        count = 0
        # Filter input files by partition. If the filter is reasonably
        # selective, this can be much faster than listing all files in the
        # bucket.
        for f in s3util.list_partitions(bucket, schema=self._filter, include_keys=True, dirs_only=True):
            count += 1
            if count == 1 or count % 1000 == 0:
                print "Listed", count, "so far"
            yield f
        conn.close()
        duration = timer.delta_sec(start)
        print "Listed", count, "files in", duration, "seconds"

def main():
    parser = argparse.ArgumentParser(description='Filter and fetch a bunch of files in S3.')
    parser.add_argument("filter", help="The file filter to use")
    parser.add_argument("-l", "--num-loaders", help="Number of loader processes to use", type=int, default=10)
    parser.add_argument("-n", "--num-dirs", help="Number of output directories", type=int, default=4)
    parser.add_argument("-r", "--retries", help="Number of times to retry", type=int, default=10)
    parser.add_argument("-d", "--base-dir", help="Base data directory", required=True)
    parser.add_argument("-b", "--bucket", help="S3 Bucket name")
    parser.add_argument("-k", "--aws-key", help="AWS Key", default=None)
    parser.add_argument("-x", "--max-bytes", help="Stop after fetching this many bytes.", type=int, default=-1)
    parser.add_argument("-s", "--aws-secret-key", help="AWS Secret Key", default=None)
    parser.add_argument("-v", "--verbose", help="Print verbose output", action="store_true")
    args = parser.parse_args()

    args = args.__dict__
    fetcher = Fetcher(args)
    start = datetime.now()
    exit_code = 0
    try:
        fetcher.fetch()
    except:
        traceback.print_exc(file=sys.stderr)
        exit_code = 2
    duration = timer.delta_sec(start)
    print "All done in %.2fs" % (duration)
    return exit_code

if __name__ == "__main__":
    sys.exit(main())
