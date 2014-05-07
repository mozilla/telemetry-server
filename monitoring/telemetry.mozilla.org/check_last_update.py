#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import argparse
import sys
from boto.s3.connection import S3Connection
from datetime import datetime, timedelta

default_date_format = '%a, %d %b %Y %H:%M:%S %Z'
message_template = "s3://{0}/{1} was modified {2} than {3} hours ago: {4}"

def is_older(target, max_hrs, date_format=default_date_format, verbose=False):
    target_date = datetime.strptime(target, date_format)
    now_date = datetime.utcnow()
    delta = timedelta(hours=(-max_hrs))
    cutoff_date = now_date + delta
    if target_date < cutoff_date:
        if verbose:
            print target_date.strftime(date_format), "<", cutoff_date.strftime(date_format)
        return True
    if verbose:
        print target_date.strftime(date_format), ">=", cutoff_date.strftime(date_format)
    return False

def get_args(argv):
    parser = argparse.ArgumentParser(description="Check the last_modified timestamp of an object in S3")
    parser.add_argument("-k", "--aws-key", help="AWS Key", default=None)
    parser.add_argument("-s", "--aws-secret-key", help="AWS Secret Key", default=None)
    parser.add_argument("-b", "--bucket", required=True, help="S3 bucket name")
    parser.add_argument("-p", "--path", required=True, help="S3 object path")
    parser.add_argument("-m", "--max-age", help="Threshold for alerting (in hours, default is 24)", type=int, default=24)
    parser.add_argument("-f", "--date-format", help="Override the default date format", default=default_date_format)
    parser.add_argument("-v", "--verbose", action="store_true", help="Print more detailed output")
    args = parser.parse_args(argv)
    return args

def main(argv):
    args = get_args(argv)
    conn = S3Connection(args.aws_key, args.aws_secret_key)
    bucket = conn.get_bucket(args.bucket)
    key = bucket.get_key(args.path)

    # File was not modified recently.
    if is_older(key.last_modified, args.max_age, args.date_format, args.verbose):
        print message_template.format(args.bucket, key.name, "more",
                                      args.max_age, key.last_modified)
        return 1

    # File was modified recently.
    if args.verbose:
        print message_template.format(args.bucket, key.name, "less",
                                      args.max_age, key.last_modified)
    return 0

if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
