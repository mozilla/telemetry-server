#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import argparse
import uuid
import multiprocessing
from multiprocessing import Process, Queue
import Queue as Q
import simplejson as json
import sys
import os
import io
from datetime import date, datetime
from telemetry_schema import TelemetrySchema
import subprocess
from subprocess import Popen
from boto.s3.connection import S3Connection
from boto.exception import S3ResponseError
import util.timer as timer
import util.files as fileutil
from convert import Converter, BadPayloadError
from revision_cache import RevisionCache
from persist import StorageLayout
import boto.sqs
import traceback


class Combiner:
    """A class for combining small Telemetry files into larger ones in S3"""
    MAX_ACTIVITIES = 10

    def __init__(self, log, dry_run):
        self.log = log
        self.dry_run = dry_run

    def log_activity(self, activity):
        with io.open(self.log, "a") as fout:                                    
            fout.write(json.dumps(activity, separators=(u',', u':')) + u"\n")

    def recover(self, source_bucket, dest_bucket):
        # TODO
        # Read through the log
        print "Reading log", self.log
        if not os.path.exists(self.log):
            print "No log file present. Recovery not needed."
            return

        last_publishables = None
        last_deletables = None
        log_file = open(self.log, "r")
        recent_activities = []
        for line in log_file.readlines():
            activity = json.loads(line)
            if activity["activity"] == "PUBLISH":
                last_publishables = activity["files"]
            if activity["activity"] == "DELETE" or activity["activity"] == "COMBINE":
                last_deletables = activity["files"]
            recent_activities.append(activity)
            if len(recent_activities) > Combiner.MAX_ACTIVITIES:
                recent_activities.pop(0)
        log_file.close()

        if len(recent_activities) == 0:
            print "No activities to recover from"
            return True
            
        last_activity = recent_activities[-1]
        # Find what we were up to
        print "We stopped after", last_activity["state"], last_activity["activity"]

        do_publish = False
        do_delete = False

        if last_activity["activity"] == "PUBLISH":
            if last_activity["state"] == "START":
                do_publish = True
            do_delete = True
        elif last_activity["activity"] == "DELETE":
            if last_activity["state"] == "START":
                do_delete = True
        else:
            print "Last activity does not require recovery. Yay!"

        success = True
        
        # If we stopped somewhere interesting, clean up
        if do_publish:
            print "Cleaning up by publishing (again?)"
            # Get publishables:
            if last_publishables is None:
                print "Cannot recover... missing required list of files to be published"
                # TODO: throw error
                success = False
            else:
                self.upload(last_publishables, dest_bucket)

        if do_delete:
            print "Cleaning up by deleting (again?)"
            if last_deletables is None:
                # Look for a recent "COMBINE" for the list.
                print "Cannot recover... missing required list of files to be deleted"
                # TODO: throw error
                success = False
            else:
                self.delete(last_deletables, source_bucket)
        return success

    def fetch_file_info(self, bucket, max_size, prefix=''):
        combinees = {}
        for k in bucket.list(prefix):
            partition = self.get_partition(k)
            if partition is None:
                print "Couldn't find partition for", k.name
                continue

            if partition not in combinees:
                combinees[partition] = []
            combinees[partition].append(k)
        return combinees

    def get_partition(self, key):
        name = key.name
        suffix_loc = name.find(".log")
        if suffix_loc < 0:
            return None
        else:
            return name[0:suffix_loc]

    def download(self, files, base_dir):
        print "Downloading"
        activity = {"state": "START", "activity": "DOWNLOAD", "files": [ s.name for s in files] }
        self.log_activity(activity)
        for f in files:
            print "Getting", f.name
            local_filename = os.path.join(base_dir, f.name)
            local_dir = os.path.dirname(local_filename)
            if not os.path.exists(local_dir):
                os.makedirs(local_dir)
            f.get_contents_to_filename(local_filename)
        activity["state"] = "FINISH"
        self.log_activity(activity)

    def concat(self, partition, files, base_dir, out_dir):
        print "Concatenating"
        tmp_name = os.path.join(out_dir, partition + ".temp")
        tmp_dir = os.path.dirname(tmp_name)
        tmp_file = open(tmp_name, "a")
        print "Concatenating", len(files), "into", tmp_name
        if not os.path.exists(tmp_dir):
            os.makedirs(tmp_dir)
        decompress_cmd = [StorageLayout.COMPRESS_PATH] + StorageLayout.DECOMPRESSION_ARGS
        for f in files:
            print "Appending", f.name
            full_name = os.path.join(base_dir, f.name)
            result = subprocess.call(decompress_cmd + [full_name], stdout=tmp_file)
            if result != 0:
                print "ERROR"
                # TODO: throw
        tmp_file.close()

        # Get md5sum of tmp_name
        checksum, size = fileutil.md5file(tmp_name)
        large_file = partition + ".log." + checksum
        # TODO: should we checksum before or after compressing?
        # rename it to partition.log.<md5sum>
        print "Renaming", tmp_name, "to", large_file
        full_large_file = os.path.join(out_dir, large_file)
        os.rename(tmp_name, full_large_file)
        compress_cmd = [StorageLayout.COMPRESS_PATH] + StorageLayout.COMPRESSION_ARGS + [full_large_file]
        result = subprocess.call(compress_cmd)

        return large_file + StorageLayout.COMPRESSED_SUFFIX

    def combine(self, partition, smalls, max_size, work_dir, output_dir):
        larges = []
        size = 0
        current = []
        activity = {"state": "START", "activity": "COMBINE", "files": [ s.name for s in smalls] }
        self.log_activity(activity)
        for s in smalls:
            size += s.size
            current.append(s)
            if size > max_size:
                size = 0
                larges.append(current)
                current = []
        if len(current) > 1:
            larges.append(current)

        # TODO: actually concatenate the files
        large_map = {}
        for to_be_combined in larges:
            self.download(to_be_combined, work_dir)
            large_name = self.concat(partition, to_be_combined, work_dir, output_dir)
            large_map[large_name] = to_be_combined

        activity["state"] = "FINISH"
        self.log_activity(activity)
        return large_map

    def upload(self, larges, bucket):
        # TODO: implement
        activity = {"state": "START", "activity": "PUBLISH", "files": larges}
        self.log_activity(activity)
        for large in larges:
            print "Uploading", large
        if self.dry_run:
            print "Dry run: not really uploading"
        else:
            print "TODO: upload for reals"
        activity["state"] = "FINISH"
        self.log_activity(activity)

    def delete(self, smalls, bucket):
        # TODO
        activity = {"state": "START", "activity": "DELETE", "files": smalls}
        self.log_activity(activity)
        print "Deleting", smalls
        if self.dry_run:
            print "Dry run: not really deleting"
        else:
            print "TODO: delete for reals"
            # result = bucket.delete_keys(smalls)
            # iterate result to make sure they all got deleted.
        activity["state"] = "FINISH"
        self.log_activity(activity)

def main():
    parser = argparse.ArgumentParser(description='Combine small Telemetry files into larger ones', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("source_bucket", help="The S3 bucket containing small files")
    parser.add_argument("dest_bucket", help="The S3 bucket to save combined files")
    parser.add_argument("-k", "--aws-key", help="AWS Key", required=True)
    parser.add_argument("-s", "--aws-secret-key", help="AWS Secret Key", required=True)
    parser.add_argument("-r", "--aws-region", help="AWS Region", default="us-west-2")
    parser.add_argument("-w", "--work-dir", help="Location to cache downloaded files", required=True)
    parser.add_argument("-o", "--output-dir", help="Base dir to store processed data", required=True)
    parser.add_argument("-p", "--prefix", help="Optional prefix to limit input files", default='')
    parser.add_argument("-m", "--max-output-size", metavar="N", help="Max combined file size", type=int, default=(100 * 1024 * 1024))
    parser.add_argument("-D", "--dry-run", help="Don't modify remote files", action="store_true")
    args = parser.parse_args()

    if not os.path.isdir(args.output_dir):
        os.makedirs(args.output_dir)

    conn = S3Connection(args.aws_key, args.aws_secret_key)
    source_bucket = conn.get_bucket(args.source_bucket)
    dest_bucket = conn.get_bucket(args.dest_bucket)
    # Overall:
    # 1. Recover* (if needed)
    # 2. Fetch file names+sizes -> F
    # 3. for each partition p in F
    #     3a. Get small files for prefix p -> Sp
    #     3b. Combine Sp into larger files Lp
    #     3c. Upload each l in Lp
    #     3d. Delete each s in Sp (using multi-delete)
    #
    # Recovery:
    # 1. Read log file to determine last state
    # 2. If last state was 3c or 3d, redo the required steps
    # 3. Reset state to start fresh.

    activity_log_file = os.path.join(args.work_dir, "activity.log")

    start = datetime.now()
    combiner = Combiner(activity_log_file, args.dry_run)

    if combiner.recover(source_bucket, dest_bucket):
        print "Recovery succeeded!"
        if args.dry_run:
            print "If this wasn't a dry run, we'd be deleting the log file after a successful recovery"
        else:
            os.remove(activity_log_file)
    else:
        print "Recovery failed... aborting"
        return 1

    combinees = combiner.fetch_file_info(source_bucket, args.max_output_size, args.prefix)
    partitions = sorted(combinees.keys())
    for partition in partitions:
        smalls = combinees[partition]
        large_map = combiner.combine(partition, smalls, args.max_output_size, args.work_dir, args.output_dir)
        larges = sorted(large_map.keys())
        print "Uploading", len(larges), "files to replace", len(smalls), "small ones."
        combiner.upload(larges, dest_bucket)

        print "Deleting", len(smalls), "small files."
        small_names = [ s.name for s in smalls ]
        combiner.delete(small_names, args.source_bucket)

    duration = timer.delta_sec(start)
    print "All done in %.2fs" % (duration)
    return 0

if __name__ == "__main__":
    sys.exit(main())
