#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from multiprocessing import Pool
import multiprocessing
import os
import sys
from traceback import print_exc


class Downloader:
    def __init__(self, local_path, bucket_name, aws_key=None, aws_secret_key=None, poolsize=10, verbose=False):
        self.verbose = verbose
        self.local_path = local_path
        self.aws_key = aws_key
        self.aws_secret_key = aws_secret_key
        self.conn = S3Connection(self.aws_key, self.aws_secret_key)
        self.bucket_name = bucket_name
        self.bucket = self.conn.get_bucket(self.bucket_name)
        self.poolsize=poolsize

    def progress(self):
        p_len = len(self.pending)
        if p_len == 0:
            return 100
        d_len = len(self.downloaded)
        if d_len == 0:
            return 0
        return int(d_len / (p_len + d_len))

    def make_args(self, files):
        for f in files:
            yield [self.local_path, self.bucket, f]

    def make_args_from_key(self, keys):
        for f in keys:
            yield [self.local_path, self.bucket, f.name]

    def fetch_list(self, files):
        pool = Pool(processes=self.poolsize)
        result_iterator = pool.imap_unordered(download_one, self.make_args(files))
        while True:
            try:
                yield result_iterator.next(timeout=1)
            except multiprocessing.TimeoutError:
                print "no results yet.."
            except StopIteration:
                break

    def fetch_schema(self, schema):
        pool = Pool(processes=self.poolsize)
        result_iterator = pool.imap_unordered(download_one, self.make_args_from_key(list_partitions(self.bucket, schema=schema, include_keys=True)))
        while True:
            try:
                yield result_iterator.next(timeout=1)
            except multiprocessing.TimeoutError:
                print "no results yet.."
            except StopIteration:
                break

def download_one(args):
    local_path, bucket, remote_key = args
    target = os.path.join(local_path, remote_key)
    target_dir = os.path.dirname(target)
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)
    success = False
    err = None
    for retry in range(1, 4):
        try:
            k = Key(bucket)
            k.key = remote_key
            k.get_contents_to_filename(target)
            # TODO: compare md5? Note that it will fail if we switch to
            #       multipart uploads.
            success = True
            break
        except:
            print >> sys.stderr, "Error on attempt #%i:" % retry
            print_exc(file = sys.stderr)
    if not success:
        err = "Failed to download '%s' as '%s'" % (remote_key, target)
        print >> sys.stderr, err
    return target, err

def list_partitions(bucket, prefix='', level=0, schema=None, include_keys=False):
    #print "Listing...", prefix, level
    if schema is not None:
        allowed_values = schema.sanitize_allowed_values()
    delimiter = '/'
    if level > 3:
        delimiter = '.'
    for k in bucket.list(prefix=prefix, delimiter=delimiter):
        partitions = k.name.split("/")
        if level > 3:
            # split the last couple of partition components by "." instead of "/"
            partitions.extend(partitions.pop().split(".", 2))
        if schema is None or schema.is_allowed(partitions[level], allowed_values[level]):
            if level >= 5:
                if include_keys:
                    for f in bucket.list(prefix=k.name):
                        yield f
                else:
                    yield k.name
            else:
                for prefix in list_partitions(bucket, k.name, level + 1, schema, include_keys):
                    yield prefix
