#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import gzip
import os
import sys
from subprocess import Popen, PIPE
try:
    import lzma
    has_lzma = True
except ImportError:
    try:
        from backports import lzma
        has_lzma = True
    except ImportError:
        has_lzma = False

class CompressedFile():
    SEARCH_PATH = ['/usr/bin', '/usr/local/bin']
    CHUNK_SIZE = 1024 * 1024
    def __init__(self, filename, mode="r", compression_type="auto", open_now=False, force_popen=False):
        self.filename = filename
        self.mode = mode
        self.force_popen = force_popen
        if compression_type == "auto":
            self.compression_type = self.detect_compression_type(self.filename)
        else:
            self.compression_type = compression_type
        self.handle = None
        self.raw_handle = None
        self.line_num = 0
        # Don't automatically open the file right away - we may want to open it
        # just before we try to read from it. This lets us instantiate with
        # bogus filenames.
        if open_now:
            self.open()

        # Handle read- or write-only files.  Mixed mode is not supported due to
        # complexity with Popen stuff.
        if self.mode.startswith("r"):
            self.can_read = True
        else:
            self.can_read = False

        if self.mode.startswith("w"):
            self.can_write = True
        else:
            self.can_write = False

    def close(self):
        if self.raw_handle:
            self.raw_handle.close()
        if self.handle:
            self.handle.close()

    def open(self):
        self.line_num = 0
        if self.compression_type == 'lzma' or self.compression_type == 'xz':
            if has_lzma and not self.force_popen:
                # Use in-process lzma library if possible.
                self.handle = lzma.open(self.filename, self.mode)
            else:
                if self.mode == 'r':
                    # Use Popen to invoke the OS's compression executable
                    decompress_cmd = [self.get_executable(), "--decompress", "--stdout"]
                    self.raw_handle = open(self.filename, "rb")

                    # Popen the decompressing flavour of the given compression
                    # type.
                    self.p_decompress = Popen(decompress_cmd, bufsize=65536,
                        stdin=self.raw_handle, stdout=PIPE, stderr=sys.stderr)
                    self.handle = self.p_decompress.stdout
                elif self.mode == 'w':
                    compress_cmd = [self.get_executable(), "-0"]
                    # open it!
                    self.raw_handle = open(self.filename, "wb")

                    # Popen the compressing flavour of the given compression
                    # type.
                    self.p_compress = Popen(compress_cmd, bufsize=65536,
                        stdin=PIPE, stdout=self.raw_handle, stderr=sys.stderr)

                    self.handle = self.p_compress.stdin
                else:
                    raise ValueError("Unknown mode '{}' for type {}".format(
                            self.mode, self.compression_type))
        elif self.compression_type == 'gz':
            self.handle = gzip.GzipFile(self.filename, mode=self.mode)

    def write(self, content):
        if not self.can_write:
            raise IOError("Cannot write to file with mode '{}'".format(self.mode))
        if not self.handle:
            self.open()

        return self.handle.write(content)

    # Helper function to compress an existing uncompressed file.
    def compress_from(self, raw_filename, remove_original=False):
        with open(raw_filename, 'rb') as raw:
            while True:
                chunk = raw.read(CompressedFile.CHUNK_SIZE)
                if chunk == '':
                    break
                self.write(chunk)

        if remove_original:
            # Remove raw file
            os.remove(raw_filename)

    def get_executable(self):
        if self.compression_type == 'lzma' or self.compression_type == 'xz':
            for p in CompressedFile.SEARCH_PATH:
                executable = os.path.join(p, self.compression_type)
                if os.path.isfile(executable):
                    return executable
            raise RuntimeError("Could not find '{}' executable".format(self.compression_type))

    def __iter__(self):
        return self

    def next(self):
        if not self.can_read:
            raise IOError("Cannot read from file with mode '{}'".format(self.mode))
        if not self.handle:
            self.open()

        line = self.handle.readline()
        if line == '':
            raise StopIteration
        self.line_num += 1
        return line

    def detect_compression_type(self, filename):
        if "." not in filename:
            raise ValueError("Unknown file type: " + str(filename))
        last_dot = filename.rfind(".")
        compression_type = filename[last_dot + 1:]

        # TODO: check it against supported types
        return compression_type
