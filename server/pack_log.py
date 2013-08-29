import sys, struct, gzip, time
import StringIO as StringIO

fin = open(sys.argv[1], "rb")
fout = open(sys.argv[2], "wb")

record_count = 0;
while True:
    record_count += 1
    line = fin.readline()
    if line == '':
        break
    else:
        line = line.strip()

    [path, data] = line.split("\t", 1)

    # The "<" is to force it to read as Little-endian to match the way it's
    # written. This is the "native" way in linux too, but might as well make
    # sure we read it back the same way.
    # Write 2 * 4 + 8 bytes
    packed = struct.pack("<IIQ", len(path), len(data), int(round(time.time() * 1000)))
    fout.write(packed)
    fout.write(path)
    fout.write(data)

    print "Wrote record", record_count
