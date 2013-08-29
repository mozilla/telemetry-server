import sys, struct, gzip
import StringIO as StringIO

fin = open(sys.argv[1], "rb")

record_count = 0;
while True:
    record_count += 1
    # Read 2 * 4 + 8 bytes
    lengths = fin.read(16)
    if lengths == '':
        break
    # The "<" is to force it to read as Little-endian to match the way it's
    # written. This is the "native" way in linux too, but might as well make
    # sure we read it back the same way.
    len_path, len_data, timestamp = struct.unpack("<IIQ", lengths)
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

    print "Record", record_count, path, "data length:", len_data, "timestamp:", timestamp, apparent_type, "data:", data[0:5] + "..."

