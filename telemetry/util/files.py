#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import hashlib
import struct
import gzip
import StringIO as StringIO
import os
import errno


# might as well return the size too...
def md5file(filename, chunksize=8192):
    md5 = hashlib.md5()
    size = 0
    with open(filename, "rb") as data:
        while True:
            chunk = data.read(chunksize)
            if not chunk:
                break
            md5.update(chunk)
            size += len(chunk)
    return md5.hexdigest(), size

# 15 == 1 separator + 2 len_path + 4 len_data + 8 timestamp
RECORD_PREAMBLE_LENGTH = 15

def unpack(filename, raw=False, verbose=False):
    fin = open(filename, "rb")

    record_count = 0
    bad_records = 0
    bytes_skipped = 0
    total_bytes_skipped = 0
    while True:
        # Read 1 byte record separator (and keep reading until we get one)
        separator = fin.read(1)
        if separator == '':
            break
        if ord(separator[0]) != 0x1e:
            bytes_skipped += 1
            continue
        # We got our record separator as expected.
        if bytes_skipped > 0:
            if verbose:
                print "Skipped", bytes_skipped, "bytes after record", record_count, "to find a valid separator"
            total_bytes_skipped += bytes_skipped
            bytes_skipped = 0

        # Read 2 + 4 + 8 = 14 bytes
        lengths = fin.read(14)
        if lengths == '':
            break
        record_count += 1
        # The "<" is to force it to read as Little-endian to match the way it's
        # written. This is the "native" way in linux too, but might as well make
        # sure we read it back the same way.
        len_path, len_data, timestamp = struct.unpack("<HIQ", lengths)
        path = fin.read(len_path)
        data = fin.read(len_data)
        error = None
        if not raw:
            if len(data) > 1 and ord(data[0]) == 0x1f and ord(data[1]) == 0x8b:
                # Data is gzipped, uncompress it:
                try:
                    reader = StringIO.StringIO(data)
                    gunzipper = gzip.GzipFile(fileobj=reader, mode="r")
                    data = gunzipper.read()
                    gunzipper.close()
                    reader.close()
                except Exception, e:
                    # Probably wasn't gzipped, pass along the error.
                    bad_records += 1
                    error = e
        yield len_path, len_data, timestamp, path, data, error

    if bytes_skipped > 0:
        if verbose:
            print "Skipped", bytes_skipped, "at the end of the file to find a valid separator"
        total_bytes_skipped += bytes_skipped
    if verbose:
        print "Processed", record_count, "records, with", bad_records, "bad records, and skipped", total_bytes_skipped, "bytes of corruption"
    fin.close()

def makedirs_concurrent(target_dir):
    try:
        os.makedirs(target_dir)
    except OSError, e:
        # errno EEXIST == 17 means "directory exists". This is a race condition
        # in a multi-process environment, and can safely be ignored.
        if e.errno != errno.EEXIST:
            raise
