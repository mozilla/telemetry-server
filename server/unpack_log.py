# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import sys, struct, gzip
import StringIO as StringIO
import simplejson as json

fin = open(sys.argv[1], "rb")
parse = False
if len(sys.argv) > 2 and sys.argv[2] == 'parse':
    parse = True

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
    apparent_type = "unknown"
    if ord(data[0]) == 0x1f and ord(data[1]) == 0x8b:
        apparent_type = "gzipped"
        try:
            reader = StringIO.StringIO(data)
            gunzipper = gzip.GzipFile(fileobj=reader, mode="r")
            data = gunzipper.read()
            # Data was gzipped.
            reader.close()
            gunzipper.close()
        except:
            # Probably wasn't gzipped.
            pass
    elif data[0] == "{":
        apparent_type = "uncompressed"
    else:
        apparent_type = "weird (" + ":".join("{0:x}".format(ord(c)) for c in data[0:5]) + ")"

    if parse:
        try:
            parsed_json = json.loads(data)
        except Exception, e:
            bad_records += 1
            print "Record", record_count, "failed to parse json:", e
    #print "Record", record_count, path, "data length:", len_data, "timestamp:", timestamp, apparent_type, "data:", data[0:5] + "..."


if bytes_skipped > 0:
    print "Skipped", bytes_skipped, "at the end of the file to find a valid separator"
    total_bytes_skipped += bytes_skipped
print "Processed", record_count, "records, with", bad_records, "bad records, and skipped", total_bytes_skipped, "bytes of corruption"
