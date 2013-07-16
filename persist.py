#!/usr/bin/env python
# encoding: utf-8
"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""

import os
import glob
import sys
try:
    import simplejson as json
except ImportError:
    import json
import re
from telemetry_schema import TelemetrySchema
import gzip
import time
from multiprocessing import Process

# Compress the specified temp file (which is an iteration of 'basename')
# and remove it when done.  This is expected to be done in a separate process
# so we don't block while compressing.
def compress_and_delete(tmp_name, basename):
    #print "Compressing", basename, "==", tmp_name
    f_raw = open(tmp_name, 'rb')
    comp_log_name = tmp_name + ".compressing"
    f_comp = gzip.open(comp_log_name, 'wb')
    f_comp.writelines(f_raw)
    #print "Size before compression:", f_raw.tell(), "Size after compression:", os.path.getsize(comp_log_name)
    f_comp.close()
    f_raw.close()
    os.remove(tmp_name)
    # find the last existing basename.X.gz (or close to last, if we're creating
    # lots of them)
    existing_logs = glob.glob(basename + "*.[0-9]*.gz")
    suffixes = [ int(s[len(basename) + 1:-3]) for s in existing_logs ]

    if len(suffixes) == 0:
        next_log_num = 1
    else:
        next_log_num = sorted(suffixes)[-1] + 1

    #print "Next suffix for", basename, "is", next_log_num

    # TODO: handle race condition?
    #   http://stackoverflow.com/questions/82831/how-do-i-check-if-a-file-exists-using-python
    while os.path.exists(basename + "." + str(next_log_num) + StorageLayout.COMPRESSED_SUFFIX):
        # get the first unused one (in case some have been created since we checked)
        #print "Had to skip one more:", next_log_num
        next_log_num += 1

    comp_name = basename + "." + str(next_log_num) + StorageLayout.COMPRESSED_SUFFIX
    # rename comp_log_name to basename.{X+1}.gz
    os.rename(comp_log_name, comp_name)
    #print "Finished compressing", basename, "as", comp_name


class StorageLayout:
    """A class for encapsulating the on-disk data layout for Telemetry"""
    COMPRESSED_SUFFIX = ".gz"
    PENDING_COMPRESSION_SUFFIX = ".compressme"

    def __init__(self, schema, basedir, max_log_size, max_open_handles=500):
        self._max_log_size = max_log_size
        # FIXME: don't need this anymore:
        self._max_open_handles = max_open_handles
        self._compressors = []
        self._schema = schema
        self._basedir = basedir

    def write(self, uuid, obj, dimensions):
        filename = self._schema.get_filename(self._basedir, dimensions)
        self.write_filename(uuid, obj, filename)

    def write_invalid(self, uuid, obj, dimensions, err):
        # TODO: put 'err' into file?
        filename = self._schema.get_filename_invalid(self._basedir, dimensions)
        self.write_filename(uuid, obj, filename, err)

    def write_filename(self, uuid, obj, filename, err=None):
        # Working filename is like
        #   a.b.c.log
        # We want to roll this over (and compress) when it reaches a size limit
        # The compressed log filenames will be something like
        #   a.b.c.log.3.gz
        try:
            fout = open(filename, "a")
        except IOError:
            os.makedirs(os.path.dirname(filename))
            fout = open(filename, "a")

        # TODO: should we actually write "err" to file?
        fout.write(uuid)
        fout.write("\t")
        if isinstance(obj, basestring):
            fout.write(obj)
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

        # Asynchronously compress file
        #p = Process(target=compress_and_delete, args=[tmp_name, filename])
        #self._compressors.append(p)
        #p.start()

    def __del__(self):
        self.close()

    def close(self):
        # Wait for any in-flight compressors to finish.
        for c in self._compressors:
            c.join()
