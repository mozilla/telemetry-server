#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from multiprocessing import Pool
import multiprocessing
import os
import sys
from traceback import print_exc
import telemetry.util.files as fu
from boto.exception import S3ResponseError
from boto.s3.connection import S3Connection
from boto.s3.key import Key

class Loader:
    def __init__(self, local_path, bucket_name, aws_key=None, aws_secret_key=None, poolsize=10, verbose=False):
        self.verbose = verbose
        self.local_path = local_path
        self.aws_key = aws_key
        self.aws_secret_key = aws_secret_key
        self.conn = S3Connection(self.aws_key, self.aws_secret_key)
        self.bucket_name = bucket_name
        self.bucket = self.conn.get_bucket(self.bucket_name)
        self.poolsize=poolsize

    def make_args(self, files):
        for f in files:
            yield [self.local_path, self.bucket, f.name if type(f) == Key else f]

    def load_list(self, files, load_function):
        pool = Pool(processes=self.poolsize)
        result_iterator = pool.imap_unordered(load_function, self.make_args(files))
        pool.close()
        while True:
            try:
                yield result_iterator.next(timeout=1)
            except multiprocessing.TimeoutError:
                if self.verbose:
                    print "no results yet.."
            except StopIteration:
                break
        pool.join()

    def get_list(self, files):
        for local_filename, remote_filename, err in self.load_list(files, download_one):
            yield local_filename, remote_filename, err

    def get_schema(self, schema):
        for local_filename, remote_filename, err in self.load_list(list_partitions(self.bucket, schema=schema, include_keys=True), download_one):
            yield local_filename, remote_filename, err

    def put_list(self, files):
        for local_filename, remote_filename, err in self.load_list(files, upload_one):
            yield local_filename, remote_filename, err


def download_one(args):
    local_path, bucket, remote_key = args
    target = os.path.join(local_path, remote_key)
    target_dir = os.path.dirname(target)
    if not os.path.exists(target_dir):
        fu.makedirs_concurrent(target_dir)
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
        except S3ResponseError, e:
            print >> sys.stderr, "S3 Error on attempt #%i:" % retry, e.status, e.reason
        except:
            print >> sys.stderr, "Error on attempt #%i:" % retry
            print_exc(file = sys.stderr)
    if not success:
        err = "Failed to download '%s' as '%s'" % (remote_key, target)
        print >> sys.stderr, err
    return target, remote_key, err

def upload_one(args):
    local_path, bucket, remote_key = args
    target = os.path.join(local_path, remote_key)
    success = False
    err = None
    for retry in range(1, 4):
        try:
            k = Key(bucket)
            k.key = remote_key
            # TODO: use multipart upload for large files
            k.set_contents_from_filename(target)
            # TODO: compare md5?
            success = True
            break
        except S3ResponseError, e:
            print >> sys.stderr, "S3 Error on attempt #%i:" % retry, e.status, e.reason
        except:
            print >> sys.stderr, "Error on attempt #%i:" % retry
            print_exc(file = sys.stderr)
    if not success:
        err = "Failed to upload '%s' as '%s'" % (target, remote_key)
        print >> sys.stderr, err
    return target, remote_key, err


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
