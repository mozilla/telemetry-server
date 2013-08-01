#!/usr/bin/env python
# encoding: utf-8
"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""

import os
import sys
try:
    import simplejson as json
except ImportError:
    import json
from telemetry_schema import TelemetrySchema
import time


class StorageLayout:
    """A class for encapsulating the on-disk data layout for Telemetry"""
    COMPRESSED_SUFFIX = ".lzma"
    COMPRESS_PATH = "/usr/bin/lzma"
    COMPRESSION_ARGS = ["-0"]
    DECOMPRESSION_ARGS = ["--decompress", "--stdout"]

    PENDING_COMPRESSION_SUFFIX = ".compressme"

    def __init__(self, schema, basedir, max_log_size):
        self._max_log_size = max_log_size
        self._schema = schema
        self._basedir = basedir

    def write(self, uuid, obj, dimensions):
        filename = self._schema.get_filename(self._basedir, dimensions)
        self.write_filename(uuid, obj, filename)

    def write_invalid(self, uuid, obj, dimensions, err):
        # TODO: put 'err' into file?
        filename = self._schema.get_filename_invalid(self._basedir, dimensions)
        self.write_filename(uuid, obj, filename, err)

    def clean_newlines(self, value, tag="value"):
        # Clean any newlines (replace with spaces)
        for eol in ["\r", "\n"]:
            if eol in value:
                print "Warning: found an EOL in", tag
                value = value.replace(eol, " ")
        return value

    def write_filename(self, uuid, obj, filename, err=None):
        # Working filename is like
        #   a.b.c.log
        # We want to roll this over (and compress) when it reaches a size limit
        # The compressed log filenames will be something like
        #   a.b.c.log.3.COMPRESSED_SUFFIX
        try:
            fout = open(filename, "a")
        except IOError:
            os.makedirs(os.path.dirname(filename))
            fout = open(filename, "a")

        # TODO: should we actually write "err" to file?
        fout.write(uuid)
        fout.write("\t")
        if isinstance(obj, basestring):
            fout.write(self.clean_newlines(obj, obj))
        else:
            # Use minimal json (without excess spaces)
            fout.write(json.dumps(obj, separators=(',', ':')))
        fout.write("\n")

        filesize = fout.tell()
        print "Wrote to", filename, "new size is", filesize
        fout.close()
        if filesize >= self._max_log_size:
            self.rotate(filename)

    def rotate(self, filename):
        print "Rotating", filename

        # rename current file
        tmp_name = "%s.%d.%f%s" % (filename, os.getpid(), time.time(), self.PENDING_COMPRESSION_SUFFIX)
        os.rename(filename, tmp_name)
        # Note that files are expected to be compressed elsewhere (see compressor.py)
