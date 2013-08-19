import sys, struct

fin = open(sys.argv[1], "rb")

record_count = 0;
while True:
    record_count += 1
    b_path = fin.read(4)
    if b_path == '':
        break
    b_data = fin.read(4)
    len_path = struct.unpack("I", b_path)[0]
    len_data = struct.unpack("I", b_data)[0]
    path = fin.read(len_path)
    data = fin.read(len_data)
    print "Path for record", record_count, path, "length of data:", len_data, "data:", data[0:5] + "..."

