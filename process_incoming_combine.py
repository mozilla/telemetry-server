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
from boto.s3.key import Key


class UploadError(Exception):
    pass
class CompressError(Exception):
    pass
class DecompressError(Exception):
    pass

class Combiner:
    """A class for combining small Telemetry files into larger ones in S3"""
    MAX_ACTIVITIES = 10

    def __init__(self, log, dry_run, keep_remote_files, keep_local_files):
        self.log = log
        self.dry_run = dry_run
        self.keep_remote_files = keep_remote_files
        self.keep_local_files = keep_local_files

    def log_activity(self, activity):
        with io.open(self.log, "a") as fout:                                    
            fout.write(json.dumps(activity, separators=(u',', u':')) + u"\n")

    def recover(self, conn):
        # Read through the log's activities
        print "Reading log", self.log
        if not os.path.exists(self.log):
            print "No log file present. Recovery not needed."
            return True

        last_pub_activity = None
        last_del_activity = None
        log_file = open(self.log, "r")
        recent_activities = []
        # TODO: handle json-parsing exceptions
        corrupt_count = 0
        for line in log_file.readlines():
            try:
                activity = json.loads(line)
            except json.decoder.JSONDecodeError, e:
                print "Found a corrupt log line:", line.strip()
                activity = {"activity": "CORRUPT"}
                corrupt_count += 1
            if activity["activity"] == "PUBLISH":
                last_pub_activity = activity
            if activity["activity"] == "DELETE" or activity["activity"] == "COMBINE":
                last_del_activity = activity
            recent_activities.append(activity)
            if len(recent_activities) > Combiner.MAX_ACTIVITIES:
                recent_activities.pop(0)
        log_file.close()

        if len(recent_activities) == 0:
            print "No activities to recover from"
            return True
            
        last_activity = recent_activities.pop()
        while last_activity["activity"] == "CORRUPT" and len(recent_activities) > 0:
            last_activity = recent_activities.pop()

        if last_activity["activity"] == "CORRUPT":
            # By now we've seen MAX_ACTIVITIES corrupt log entries in a row,
            # we can be pretty sure the log is completely useless.
            print "Found", corrupt_count, "corrupted log entries. Aborting recovery."
            # TODO: throw error?
            return False

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
            if last_pub_activity is None:
                print "Cannot recover... missing required list of files to be published"
                # TODO: throw error
                success = False
            else:
                dest_bucket = conn.get_bucket(last_pub_activity["bucket"])
                dest_dir = last_pub_activity["base_dir"]
                self.upload(last_pub_activity["files"], dest_bucket, dest_dir)
                self.local_delete(last_pub_activity["files"], dest_dir)

        if do_delete:
            print "Cleaning up by deleting (again?)"
            if last_del_activity is None:
                print "Cannot recover... missing required list of files to be deleted"
                # TODO: throw error
                success = False
            else:
                source_bucket = conn.get_bucket(last_del_activity["bucket"])
                source_dir = last_del_activity["base_dir"]
                self.delete(last_del_activity["files"], source_bucket, source_dir)
                self.local_delete(last_del_activity["files"], source_dir)
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
            # Check locally first (md5) and skip downloading files we already
            # have.
            local_filename = os.path.join(base_dir, f.name)
            if os.path.exists(local_filename):
                md5, size = fileutil.md5file(local_filename)
                if md5 == f.etag[1:-1]:
                    print "Already have", f.name
                    continue
            local_dir = os.path.dirname(local_filename)
            if not os.path.exists(local_dir):
                os.makedirs(local_dir)
            f.get_contents_to_filename(local_filename)
        activity["state"] = "FINISH"
        self.log_activity(activity)

    def concat(self, partition, files, base_dir, out_dir):
        print "Concatenating"
        activity = {"state": "START", "activity": "CONCAT", "files": [ s.name for s in files] }
        self.log_activity(activity)
        tmp_name = os.path.join(out_dir, partition + ".temp")
        tmp_dir = os.path.dirname(tmp_name)
        if not os.path.exists(tmp_dir):
            os.makedirs(tmp_dir)
        tmp_file = open(tmp_name, "a")
        print "Concatenating", len(files), "into", tmp_name
        decompress_cmd = [StorageLayout.COMPRESS_PATH] + StorageLayout.DECOMPRESSION_ARGS
        for f in files:
            print "Appending", f.name
            full_name = os.path.join(base_dir, f.name)
            result = subprocess.call(decompress_cmd + [full_name], stdout=tmp_file)
            if result != 0:
                raise DecompressError("Unexpected decompress return code: " + str(result))
        tmp_file.close()

        # Get md5sum of tmp_name (raw uncompressed contents)
        # We checksum before compression in order to get a stable value even if
        # we change compression algorithms. Also, the final file's md5 is
        # already available as the 'etag' in S3.
        checksum, size = fileutil.md5file(tmp_name)
        large_file = partition + ".log." + checksum
        print "Renaming", tmp_name, "to", large_file
        full_large_file = os.path.join(out_dir, large_file)
        os.rename(tmp_name, full_large_file)
        # If the compressed file already exists, delete it first.
        if os.path.exists(full_large_file + StorageLayout.COMPRESSED_SUFFIX):
            os.remove(full_large_file + StorageLayout.COMPRESSED_SUFFIX)

        compress_cmd = [StorageLayout.COMPRESS_PATH] + StorageLayout.COMPRESSION_ARGS + [full_large_file]
        result = subprocess.call(compress_cmd)
        if result != 0:
            raise CompressError("Unexpected compress return code: " + str(result))

        activity["state"] = "FINISH"
        self.log_activity(activity)
        return large_file + StorageLayout.COMPRESSED_SUFFIX

    def combine(self, partition, smalls, max_size, work_dir, output_dir):
        larges = []
        size = 0
        current = []
        bucket_name = smalls[0].bucket.name
        activity = {"state": "START", "activity": "COMBINE",
                "files": [ s.name for s in smalls], "bucket": bucket_name,
                "base_dir": work_dir}
        self.log_activity(activity)
        for s in smalls:
            size += s.size
            current.append(s)
            if size > max_size:
                size = 0
                larges.append(current)
                current = []
        # Don't "combine" groups of one item. leave these alone.
        # TODO: if source_bucket != dest_bucket, we should keep these.
        if len(current) > 1:
            larges.append(current)

        # Now download and concatenate the files
        large_map = {}
        for to_be_combined in larges:
            self.download(to_be_combined, work_dir)
            large_name = self.concat(partition, to_be_combined, work_dir, output_dir)
            large_map[large_name] = to_be_combined

        activity["state"] = "FINISH"
        self.log_activity(activity)
        return large_map

    def upload(self, larges, bucket, base_dir):
        activity = {"state": "START", "activity": "PUBLISH", "files": larges,
                "base_dir": base_dir, "bucket": bucket.name}
        self.log_activity(activity)
        for large in larges:
            print "Uploading", large
            if self.dry_run:
                print "Dry run: not really uploading"
            else:
                # TODO: wrap in retry.
                k = Key(bucket)
                k.key = large
                full_large = os.path.join(base_dir, large)
                bytes_written = k.set_contents_from_filename(full_large)
                actual_bytes = os.path.getsize(full_large)
                if bytes_written != actual_bytes:
                    message = "Error uploading %s - bytes written: %d, actual bytes: %d" % (full_large, bytes_written, actual_bytes)
                    print message
                    raise UploadError(message)
        activity["state"] = "FINISH"
        self.log_activity(activity)

    # Delete files from S3. Not to be confused with "local_delete" which
    # deletes files on local disk
    def delete(self, smalls, bucket, base_dir):
        activity = {"state": "START", "activity": "DELETE", "files": smalls,
                "base_dir": base_dir, "bucket": bucket.name}
        self.log_activity(activity)
        print "Deleting", smalls
        if self.dry_run:
            print "Dry run: not really deleting"
        elif self.keep_remote_files:
            print "Not deleting remote files (received --keep-remote-files arg)"
        else:
            # S3 multi-deletes can do 1000 at a time. The boto docs don't say
            # whether this is abstracted by the library, so we split the list
            # "just in case".
            delete_groups = [ smalls[i:i+1000] for i in range(0, len(smalls), 1000) ]
            for delete_group in delete_groups:
                print "Deleting", len(delete_group), "keys"
                # TODO: wrap in retry
                result = bucket.delete_keys(delete_group)
                # Note: An attempt to delete a non-existent file does not cause
                #       an error.
                # Check result to make sure they all got deleted.
                if len(result.deleted) < len(delete_group):
                    print "Not everything was deleted successfully"
                    for e in result.errors:
                        print "Not deleted:", e
                    raise DeleteError("Not Deleted:" + ",".join(result.errors))
                else:
                    print "Successfully deleted", len(result.deleted), "keys"
        activity["state"] = "FINISH"
        self.log_activity(activity)

    def local_delete(self, files, base_dir):
        activity = {"state": "START", "activity": "LOCAL_DELETE",
                "files": files, "base_dir": base_dir}
        self.log_activity(activity)

        print "Locally deleting", files
        if self.dry_run:
            print "Dry run: not really deleting"
        elif self.keep_local_files:
            print "Not deleting local files (received --keep-local-files arg)"
        else:
            for f in files:
                print "Deleting", f
                os.remove(os.path.join(base_dir, f))
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
    parser.add_argument("--keep-remote-files", help="Don't remove small files from S3", action="store_true")
    parser.add_argument("--keep-local-files", help="Don't remove local files from disk", action="store_true")
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
    combiner = Combiner(activity_log_file, args.dry_run,
            args.keep_remote_files, args.keep_local_files)

    if combiner.recover(conn):
        print "Recovery succeeded!"
        if args.dry_run:
            print "If this wasn't a dry run, we'd be deleting the log file after a successful recovery"
        else:
            if os.path.exists(activity_log_file):
                os.remove(activity_log_file)
    else:
        print "Recovery failed... aborting"
        # TODO: email the (compressed) activity log, upload it to S3, or
        #       otherwise complain loudly somehow.
        return 1

    combinees = combiner.fetch_file_info(source_bucket, args.max_output_size, args.prefix)
    partitions = sorted(combinees.keys())
    for partition in partitions:
        smalls = combinees[partition]
        large_map = combiner.combine(partition, smalls, args.max_output_size, args.work_dir, args.output_dir)
        larges = sorted(large_map.keys())
        if len(larges) == 0:
            print "Didn't find anything to combine for partition", partition
            continue
        print "Uploading", len(larges), "files to replace", len(smalls), "small ones."
        combiner.upload(larges, dest_bucket, args.output_dir)
        combiner.local_delete(larges, args.output_dir)

        # Delete values from large_map (to avoid deleting items skipped during
        # combine)
        smalls_to_delete = []
        for large in larges:
            smalls_to_delete.append(large_map[large])
        if len(smalls_to_delete) > 0:
            print "Deleting", len(smalls_to_delete), "small files."
            small_names = [ s.name for s in smalls_to_delete ]
            combiner.delete(small_names, source_bucket, args.work_dir)
            combiner.local_delete(small_names, args.work_dir)
        else:
            print "Nothing to delete for", partition

    duration = timer.delta_sec(start)
    print "All done in %.2fs" % (duration)
    return 0

if __name__ == "__main__":
    sys.exit(main())
