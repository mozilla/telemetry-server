#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import message_pb2  # generated from https://github.com/mozilla-services/heka (message/message.proto)
import boto
import snappy
import struct
import gzip

from cStringIO import StringIO
from google.protobuf.message import DecodeError


_record_separator = 0x1e


class BacktrackableFile:
    def __init__(self, stream):
        self._stream = stream
        self._buffer = StringIO()

    def read(self, size):
        buffer_data = self._buffer.read(size)
        to_read = size - len(buffer_data)

        if to_read == 0:
            return buffer_data

        stream_data = self._stream.read(to_read)
        self._buffer.write(stream_data)

        return buffer_data + stream_data

    def close(self):
        self._buffer.close()
        if type(self._stream) == boto.s3.key.Key:
            if self._stream.resp:  # Hack! Connections are kept around otherwise!
                self._stream.resp.close()

            self._stream.close(True)
        else:
            self._stream.close()

    def backtrack(self):
        buffer = self._buffer.getvalue()
        index = buffer.find(chr(_record_separator), 1)

        self._buffer = StringIO()
        if index >= 0:
            self._buffer.write(buffer[index:])
            self._buffer.seek(0)


class UnpackedRecord():
    def __init__(self, raw, header, message=None, error=None):
        self.raw = raw
        self.header = header
        self.message = message
        self.error = error


# Returns (bytes_skipped=int, eof_reached=bool)
def read_until_next(fin, separator=_record_separator):
    bytes_skipped = 0
    while True:
        c = fin.read(1)
        if c == '':
            return (bytes_skipped, True)
        elif ord(c) != separator:
            bytes_skipped += 1
        else:
            break
    return (bytes_skipped, False)


# Stream Framing:
#  https://hekad.readthedocs.org/en/latest/message/index.html
def read_one_record(input_stream, raw=False, verbose=False, strict=False, try_snappy=True):
    # Read 1 byte record separator (and keep reading until we get one)
    total_bytes = 0
    skipped, eof = read_until_next(input_stream, 0x1e)
    total_bytes += skipped
    if eof:
        return None, total_bytes
    else:
        # we've read one separator (plus anything we skipped)
        total_bytes += 1

    if skipped > 0:
        if strict:
            raise ValueError("Unexpected character(s) at the start of record")
        if verbose:
            print "Skipped", skipped, "bytes to find a valid separator"

    raw_record = struct.pack("<B", 0x1e)

    # Read the header length
    header_length_raw = input_stream.read(1)
    if header_length_raw == '':
        return None, total_bytes

    total_bytes += 1
    raw_record += header_length_raw

    # The "<" is to force it to read as Little-endian to match the way it's
    # written. This is the "native" way in linux too, but might as well make
    # sure we read it back the same way.
    (header_length,) = struct.unpack('<B', header_length_raw)

    header_raw = input_stream.read(header_length)
    if header_raw == '':
        return None, total_bytes
    total_bytes += header_length
    raw_record += header_raw

    header = message_pb2.Header()
    header.ParseFromString(header_raw)
    unit_separator = input_stream.read(1)
    total_bytes += 1
    if ord(unit_separator[0]) != 0x1f:
        error_msg = "Unexpected unit separator character in record #{} " \
                "at offset {}: {}".format(record_count, total_bytes,
                ord(unit_separator[0]))
        if strict:
            raise ValueError(error_msg)
        return UnpackedRecord(raw_record, header, error=error_msg), total_bytes
    raw_record += unit_separator

    #print "message length:", header.message_length
    message_raw = input_stream.read(header.message_length)

    total_bytes += header.message_length
    raw_record += message_raw

    message = None
    if not raw:
        message = message_pb2.Message()
        parsed_ok = False
        if try_snappy:
            try:
                message.ParseFromString(snappy.decompress(message_raw))
                parsed_ok = True
            except:
                # Wasn't snappy-compressed
                pass
        if not parsed_ok:
            # Either we didn't want to attempt snappy, or the
            # data was not snappy-encoded (or it was just bad).
            message.ParseFromString(message_raw)

    return UnpackedRecord(raw_record, header, message), total_bytes


def unpack_file(filename, **kwargs):
    fin = None
    if filename.endswith(".gz"):
        fin = gzip.open(filename, "rb")
    else:
        fin = open(filename, "rb")
    return unpack(fin, **kwargs)


def unpack_string(string, **kwargs):
    return unpack(StringIO(string), **kwargs)


def unpack(fin, raw=False, verbose=False, strict=False, backtrack=False, try_snappy=False):
    record_count = 0
    bad_records = 0
    total_bytes = 0

    while True:
        r = None
        try:
            r, bytes = read_one_record(fin, raw, verbose, strict, try_snappy)
        except Exception as e:
            if strict:
                fin.close()
                raise e
            elif verbose:
                print e

            if backtrack and type(e) == DecodeError:
                fin.backtrack()
                continue

        if r is None:
            break

        if verbose and r.error is not None:
            print r.error

        record_count += 1
        total_bytes += bytes

        yield r, total_bytes

    if verbose:
        print "Processed", record_count, "records"

    fin.close()
