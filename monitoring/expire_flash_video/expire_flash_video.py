#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from boto.s3.connection import S3Connection
from datetime import datetime
import argparse
import logging
import psycopg2
import socket
import sys
import telemetry.util.timer as timer
import traceback

def get_args():
    parser = argparse.ArgumentParser(
            description='Expire any `flash_video` pings older than ' \
                        'the specified date.',
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-d", "--dry-run", action="store_true",
            help="Dry run: don't modify anything")
    parser.add_argument("--bucket", required=True,
            help="S3 Bucket name")
    parser.add_argument("-p", "--prefix", default="flash_video/",
            help="Prefix for pings to be expired")
    parser.add_argument("-k", "--aws-key", help="AWS Key")
    parser.add_argument("-s", "--aws-secret-key", help="AWS Secret Key")
    parser.add_argument("-v", "--verbose", action="store_true",
            help="Print verbose output")
    parser.add_argument("-x", "--expiry-date", required=True,
            help="Remove any files older than this date")

    parser.add_argument("--db-host", default="localhost")
    parser.add_argument("--db-user", default="telemetry")
    parser.add_argument("--db-port", type=int, default=5432)
    parser.add_argument("--db-pass")
    parser.add_argument("--db-name", default="telemetry")

    return parser.parse_args()

def should_run(dry_run, logger, message):
    if dry_run:
        logger.info("Dry run: Not really " + message)
    else:
        logger.info(message)
    # dry_run == False -> should_run == True
    # and vice versa
    return not dry_run

def should_expire(key_name, expiry_date, logger):
    if expiry_date is None:
        return False
    path_pieces = key_name.split("/")
    file_pieces = path_pieces[-1].split(".")
    submission_date = file_pieces[1]
    if file_pieces[2] == "v2" and file_pieces[3] == "log":
        # the filename is in the expected format.
        if submission_date < expiry_date:
            logger.debug("Should expire: {} < {} in {}".format(submission_date, expiry_date, key_name))
            return True
        else:
            logger.debug("Should not expire: {} >= {} in {}".format(submission_date, expiry_date, key_name))
            return False
    logger.warn("Filename not in expected format a/b/c/build_id.submission_date.v2.log.hash.extension: {}".format(key_name))
    return False

def main():
    args = get_args()
    logging.basicConfig()
    logger = logging.getLogger(__name__)
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.WARNING)

    logger.info("Expiring `flash_video` data older than {}.".format(args.expiry_date))
    logger.debug("Connecting to S3...")
    conn = S3Connection(args.aws_key, args.aws_secret_key)
    bucket = conn.get_bucket(args.bucket)

    connection_string = ""
    if hasattr(args, "db_name"):
        connection_string += "dbname={0} ".format(args.db_name)
    if hasattr(args, "db_host"):
        connection_string += "host={0} ".format(args.db_host)
    if hasattr(args, "db_port"):
        connection_string += "port={0} ".format(args.db_port)
    if hasattr(args, "db_user"):
        connection_string += "user={0} ".format(args.db_user)
    if hasattr(args, "db_pass"):
        connection_string += "password={0} ".format(args.db_pass)

    db_conn = None
    db_cursor = None
    if should_run(args.dry_run, logger, "Connecting to database"):
        db_conn = psycopg2.connect(connection_string)
        db_cursor = db_conn.cursor()

    prefix = args.prefix
    last_key = ''
    done = False
    total_count = 0
    exp_count = 0
    total_bytes = 0
    start_time = datetime.now()
    while not done:
        try:
            for k in bucket.list(prefix=prefix, marker=last_key):
                if k.name.endswith('/'):
                    logger.debug("Skipping directory '{}'".format(k.name))
                    continue
                total_count += 1
                if not should_expire(k.name, args.expiry_date, logger):
                    continue
                exp_count += 1
                total_bytes += k.size
                last_key = k.name
                if total_count % 100 == 0:
                    logger.info("Expired {} of {} total files in {}s. Last key was {}".format(
                        exp_count, total_count, timer.delta_sec(start_time), last_key))
                logger.debug("Deleting {} from S3 bucket".format(k.name))
                sql_update = "DELETE FROM published_files WHERE file_name = '{0}';".format(k.name)
                if should_run(args.dry_run, logger, "Deleting from S3 bucket"):
                    k.delete()

                if should_run(args.dry_run, logger, "Notifying coordinator"):
                    logger.debug("Actually notifying coordinator")
                    db_cursor.execute(sql_update)
                    db_conn.commit()
                    logger.debug("Coordinator notified")
            done = True
        except socket.error, e:
            logger.error("Error listing keys: {}".format(e))
            logger.error(traceback.format_exc())
            logger.info("Continuing from last seen key: {}".format(last_key))
    if db_conn is not None:
        db_conn.close()
    total_mb = round(total_bytes / 1024.0 / 1024.0, 2)
    logger.info("Overall, expired {} of {} files ({} MB) in {} seconds.".format(
        exp_count, total_count, total_mb, timer.delta_sec(start_time)))
    return 0

if __name__ == "__main__":
    sys.exit(main())
