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

def compress_and_delete(old_log_name):
    print "Compressing", old_log_name
    f_raw = open(old_log_name, 'rb')
    comp_log_name = old_log_name + StorageLayout.COMPRESSED_SUFFIX
    f_comp = gzip.open(comp_log_name, 'wb')
    f_comp.writelines(f_raw)
    print "Size before compression:", f_raw.tell(), "Size after compression:", os.path.getsize(comp_log_name)
    f_comp.close()
    f_raw.close()
    os.remove(old_log_name)
    print "Finished compressing", old_log_name

class StorageLayout:
    """A class for encapsulating the on-disk data layout for Telemetry"""
    COMPRESSED_SUFFIX = ".gz"

    def __init__(self, schema, basedir, max_log_size, max_open_handles=500):
        self._max_log_size = max_log_size
        self._max_open_handles = max_open_handles
        self._logcache = {}
        self._compressors = []
        self._schema = schema
        self._basedir = basedir

        #if !self._schema:
        #    schema = json.load(os.path.join(self._basedir, "telemetry_schema.json"))

    def write(self, uuid, obj, dimensions):
        filename = self._schema.get_filename(self._basedir, dimensions)
        self.write_filename(uuid, obj, filename)

    def write_invalid(self, uuid, obj, dimensions, err):
        # TODO: put 'err' into file?
        filename = self._schema.get_filename_invalid(self._basedir, dimensions)
        self.write_filename(uuid, obj, filename, err)

    def write_filename(self, uuid, obj, filename, err=None):
        # Incoming filename is like
        #   a.b.c.log
        # We may have already written some logs and rolled over
        # So the actual filename we want to append may be something like
        #   a.b.c.log.3
        # With other files called a.b.c.log.1.gz and a.b.c.log.2.gz
        #
        # For a fresh write request:
        # - find a.b.c.log*
        # - find the one with the highest number -> N
        # - if a.b.c.log.N is already compressed, use N+1
        # - if it's not already compressed, use N
        # - if after writing, the file exceeds max size, close it, open the next, and asynchronously compress the previous one.
        # Keep a cache of heavily used log files

        if filename not in self._logcache:
            self._logcache[filename] = self.load_log_info(filename)

        log_info = self._logcache[filename]

        fout = log_info["handle"]
        # TODO: should we actually write "err" to file?
        fout.write(uuid)
        fout.write("\t")
        if isinstance(obj, basestring):
            fout.write(obj)
        else:
            # Use minimal json (without excess spaces)
            fout.write(json.dumps(obj, separators=(',', ':')))
        fout.write("\n")
        log_info["wtime"] = time.time()

        if fout.tell() >= self._max_log_size:
            self.rotate(log_info)

    def load_log_info(self, filename):
        print "Loading info for", filename
        log_info = {
                "name": filename,
                "wtime": time.time()
        }
        existing_logs = glob.glob(filename + "*")
        suffixes = [ s[len(filename) + 1:] for s in existing_logs ]
        # suffixes now contains [ 1.gz, 2.gz, ..., N.gz ], where the
        # final one may or may not be compressed.
        # If we didn't find anything, always start with filename.1
        last_log = 1
        last_log_compressed = False
        comp_extension_len = len(StorageLayout.COMPRESSED_SUFFIX)
        for suffix in suffixes:
            if suffix.endswith(StorageLayout.COMPRESSED_SUFFIX):
                curr_log = int(suffix[0:-comp_extension_len])
                if curr_log > last_log:
                    last_log = curr_log
                    last_log_compressed = True
            else:
                curr_log = int(suffix)
                if curr_log > last_log:
                    last_log = curr_log
                    last_log_compressed = False

        if last_log_compressed:
            last_log += 1
        log_info["last_log"] = last_log

        # TODO: if the cache is full, evict someone first.
        if len(self._logcache) >= self._max_open_handles:
            self.evict_logcache()
        log_info["handle"] = self.open_log(log_info)
        return log_info

    def real_name(self, name, number):
        return ".".join((name, str(number)))

    def open_log(self, log_info):
        real_filename = self.real_name(log_info["name"], log_info["last_log"])
        try:
            handle = open(real_filename, "a")
        except IOError:
            os.makedirs(os.path.dirname(real_filename))
            handle = open(real_filename, "a")
        return handle

    def rotate(self, log_info):
        print "Rotating", log_info["name"]
        # close current file (N)
        log_info["handle"].close()
        old_log_num = log_info["last_log"]

        # open next file (N+1)
        log_info["last_log"] += 1
        log_info["handle"] = self.open_log(log_info)
        log_info["wtime"] = time.time()

        # asynchronously compress file N
        # TODO: async
        old_log_name = self.real_name(log_info["name"], old_log_num)
        #print "preparing to compress and delete", old_log_name
        #compress_and_delete(old_log_name)
        p = Process(target=compress_and_delete, args=[old_log_name])
        self._compressors.append(p)
        p.start()

    def evict_logcache(self):
        print "Cache is full. Time to evict someone."
        # Use some heuristic of least recently used. Some combination of LRU
        # and Least Frequently Used would be better, but LRU is resistant to
        # old files getting stuck.
        lucky_winner = self._logcache.itervalues().next()
        # Find the oldest write time.
        for log_info in self._logcache.itervalues():
            if log_info["wtime"] < lucky_winner["wtime"]:
                lucky_winner = log_info
        print "Evicting", lucky_winner["name"], "last written at", lucky_winner["wtime"]

        del(self._logcache[lucky_winner["name"]])

    def __del__(self):
        self.close()

    def close(self):
        # Close cached log files
        for log_info in self._logcache.itervalues():
            #print "Closing", log_info["name"], "#", log_info["last_log"], "last written at", log_info["wtime"]
            log_info["handle"].close()
        self._logcache.clear()

        # Wait for any in-flight compressors to finish.
        for c in self._compressors:
            c.join()
