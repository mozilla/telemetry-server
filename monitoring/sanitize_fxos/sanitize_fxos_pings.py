#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from boto.s3.connection import S3Connection
from datetime import datetime
import argparse
import logging
import os
import socket
from subprocess import Popen, PIPE
import sys
import telemetry.util.files as fu
import telemetry.util.timer as timer
from telemetry.persist import StorageLayout
import traceback
from uuid import uuid4

def get_args():
    parser = argparse.ArgumentParser(
            description='Sanitize FirefoxOS pings and move them from ' \
                        'source-bucket to dest-bucket',
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-d", "--dry-run", action="store_true",
            help="Dry run: don't modify anything")
    parser.add_argument("--source-bucket", required=True,
            help="Source S3 Bucket name")
    parser.add_argument("--dest-bucket", required=True,
            help="Destination S3 Bucket name")
    parser.add_argument("-p", "--prefix", default="ftu/",
            help="Prefix for fxos pings")
    parser.add_argument("-k", "--aws-key", help="AWS Key")
    parser.add_argument("-s", "--aws-secret-key", help="AWS Secret Key")
    parser.add_argument("-w", "--work-dir", default="/tmp/fxos",
            help="Location to put temporary work files")
    parser.add_argument("-v", "--verbose", action="store_true",
            help="Print verbose output")
    return parser.parse_args()

def should_run(dry_run, logger, message):
    if dry_run:
        logger.info("Dry run: Not really " + message)
    else:
        logger.info(message)
    # dry_run == False -> should_run == True
    # and vice versa
    return not dry_run

def main():
    args = get_args()
    logging.basicConfig()
    logger = logging.getLogger(__name__)
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.WARNING)

    if not os.path.exists(args.work_dir):
        os.makedirs(args.work_dir)

    logger.info("Sanitizing FirefoxOS data from {} and moving it to {}".format(args.source_bucket, args.dest_bucket))
    logger.debug("Connecting to S3...")
    conn = S3Connection(args.aws_key, args.aws_secret_key)
    source_bucket = conn.get_bucket(args.source_bucket)
    dest_bucket = conn.get_bucket(args.dest_bucket)

    compress_cmd = [StorageLayout.COMPRESS_PATH] + StorageLayout.COMPRESSION_ARGS
    prefix = args.prefix
    last_key = ''
    done = False
    total_count = 0
    total_bytes = 0
    start_time = datetime.now()
    dupe_map = {}
    while not done:
        try:
            for k in source_bucket.list(prefix=prefix, marker=last_key):
                if k.name.endswith('/'):
                    logger.debug("Skipping directory '{}'".format(k.name))
                    continue
                total_count += 1
                total_bytes += k.size
                last_key = k.name
                if total_count % 100 == 0:
                    logger.info("Looked at {} total records in {} seconds. Last key was {}".format(total_count, timer.delta_sec(start_time), last_key))
                logger.debug("Fetching {} from source bucket".format(k.name))
                full_source_filename = os.path.join(args.work_dir, "__source", k.name)
                full_dest_filename = os.path.join(args.work_dir, "__dest", k.name)

                # Ensure that the necessary local dirs exist:
                for f in [full_source_filename, full_dest_filename]:
                    dirname = os.path.dirname(f)
                    if dirname != '' and not os.path.exists(dirname):
                        os.makedirs(dirname)
                logger.debug("Getting '{}' to '{}'".format(k.name, full_source_filename))
                k.get_contents_to_filename(full_source_filename)

                logger.info("Removing pingIDs...")
                tmp_out_file = full_dest_filename + ".tmp"
                out_handle = open(tmp_out_file, "w")
                logger.debug("Uncompressing...")
                if full_source_filename.endswith(StorageLayout.COMPRESSED_SUFFIX):
                    decompress_cmd = [StorageLayout.COMPRESS_PATH] + StorageLayout.DECOMPRESSION_ARGS
                    raw_handle = open(full_source_filename, "rb")
                    # Popen the decompressing version of StorageLayout.COMPRESS_PATH
                    p_decompress = Popen(decompress_cmd, bufsize=65536, stdin=raw_handle, stdout=PIPE, stderr=sys.stderr)
                    handle = p_decompress.stdout
                else:
                    handle = open(full_source_filename, "r")
                    raw_handle = None

                logger.debug("Generating new pingIDs...")
                for line in handle:
                    # Lines are of the form <key><tab><json payload><newline>.
                    # Split on tab character to get the pieces.
                    key, payload = line.split(u"\t", 1)
                    # Replace key with a fresh UUID:
                    if key in dupe_map:
                        logger.info("Already saw key {}, skipping any more occurrences".format(key))
                    else:
                        new_key = str(uuid4())
                        dupe_map[key] = new_key
                        out_handle.write(u"%s\t%s" % (new_key, payload))

                handle.close()
                out_handle.close()
                if raw_handle:
                    raw_handle.close()

                sql_update = None
                empty_result = False
                if os.stat(tmp_out_file).st_size > 0:
                    logger.debug("Compressing new file...")
                    f_comp = open(full_dest_filename, "wb")
                    f_raw = open(tmp_out_file, "r", 1)
                    p_compress = Popen(compress_cmd, bufsize=65536, stdin=f_raw,
                            stdout=f_comp, stderr=sys.stderr)
                    p_compress.communicate()
                    f_raw.close()
                    f_comp.close()
                    local_md5, size = fu.md5file(full_dest_filename)
                    sql_update = "UPDATE published_files SET " \
                          "file_md5 = '{0}', " \
                          "file_size = {1}, " \
                          "bucket_name = '{2}' " \
                          "WHERE file_name = '{3}';".format(local_md5, size,
                            dest_bucket.name, k.name)
                else:
                    # Don't upload empty files.
                    empty_result = True
                    sql_update = "DELETE FROM published_files WHERE file_name = '{0}';".format(k.name)
                    logger.debug("File was empty, skipping: {}".format(tmp_out_file))

                logger.info("Removing temp output file: {}".format(tmp_out_file))
                os.remove(tmp_out_file)

                if not empty_result and should_run(args.dry_run, logger,
                                              "Uploading to dest bucket"):
                    dest_key = dest_bucket.new_key(k.name)
                    dest_key.set_contents_from_filename(full_dest_filename)
                    # Compare the md5 to be sure it succeeded.
                    dest_md5 = dest_key.etag[1:-1]
                    local_md5, size = fu.md5file(full_dest_filename)
                    if dest_md5 != local_md5:
                        raise Exception("Failed to upload {}".format(full_dest_filename))

                if should_run(args.dry_run, logger, "Removing input file: {}".format(full_source_filename)):
                    os.remove(full_source_filename)

                if not empty_result and should_run(args.dry_run, logger, "Removing output file: {}".format(full_dest_filename)):
                    os.remove(full_dest_filename)

                if empty_result or args.source_bucket != args.dest_bucket:
                    if should_run(args.dry_run, logger, "Deleting from source bucket"):
                        k.delete()
                else:
                    logger.info("Not deleting source: either non-empty or same bucket: {}".format(k.name))

                if sql_update is None:
                    logger.error("Missing sql_update :(")
                else:
                    logger.info(sql_update)
                if should_run(args.dry_run, logger, "Notifying coordinator"):
                    #TODO
                    logger.debug("Should be actually notifying coordinator")

            done = True
        except socket.error, e:
            logger.error("Error listing keys: {}".format(e))
            logger.error(traceback.format_exc())
            logger.info("Continuing from last seen key: {}".format(last_key))
    total_mb = round(total_bytes / 1024.0 / 1024.0, 2)
    logger.info("Total bytes: {}".format(total_bytes))
    logger.info("Overall, listed {} files ({} MB) in {} seconds.".format(
        total_count, total_mb, timer.delta_sec(start_time)))
    return 0

if __name__ == "__main__":
    sys.exit(main())
