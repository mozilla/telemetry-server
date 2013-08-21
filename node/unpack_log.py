import sys, struct, gzip
import StringIO as StringIO

fin = open(sys.argv[1], "rb")

record_count = 0;
while True:
    record_count += 1
    # Read 2 * 4 bytes
    lengths = fin.read(8)
    if lengths == '':
        break
    len_path, len_data = struct.unpack("II", lengths)
    path = fin.read(len_path)
    data = fin.read(len_data)
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

    print "Path for record", record_count, path, "length of data:", len_data, "data:", data[0:5] + "..."

