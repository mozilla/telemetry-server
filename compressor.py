import sys, os, re, gzip, glob
from persist import StorageLayout
from datetime import datetime

searchdir = sys.argv[1]

paths = {}
pcs = StorageLayout.PENDING_COMPRESSION_SUFFIX
acs = StorageLayout.COMPRESSED_SUFFIX

compressed_log = re.compile("^.*[.]log[.]([0-9]+)" + acs)

for root, dirs, files in os.walk(searchdir):
    for f in files:
        if f.endswith(pcs):
            if root not in paths:
                paths[root] = {"compressed": [], "pending": []}
            paths[root]["pending"].append(f)

for path in paths.iterkeys():
    pending = paths[path]["pending"]
    print "Found a path %s with %d pending files" % (path, len(pending))
    for filename in pending:
        print "  Compressing", filename

        base_ends = filename.find(".log") + 4
        if base_ends < 4:
            print "   Bad filename encountered, skipping:", filename
            continue
        basename = filename[0:base_ends]
        full_basename = os.path.join(path, basename)

        existing_logs = glob.glob(full_basename + ".[0-9]*" + acs)
        suffixes = [ int(s[len(full_basename) + 1:-3]) for s in existing_logs ]

        if len(suffixes) == 0:
            next_log_num = 1
        else:
            next_log_num = sorted(suffixes)[-1] + 1

        # TODO: handle race condition?
        #   http://stackoverflow.com/questions/82831/how-do-i-check-if-a-file-exists-using-python
        while os.path.exists(full_basename + "." + str(next_log_num) + acs):
            print "Another challenger appears!"
            next_log_num += 1

        comp_name = full_basename + "." + str(next_log_num) + acs
        # claim it!
        f_comp = gzip.open(comp_name, "wb")
        print "    compressing %s to %s" % (filename, comp_name)

        # Rename uncompressed file to a temp name
        tmp_name = comp_name + ".compressing"
        print "    moving %s to %s" % (os.path.join(path, filename), tmp_name)
        os.rename(os.path.join(path, filename), tmp_name)

        start = datetime.now()
        f_raw = open(tmp_name, "rb")
        f_comp.writelines(f_raw)
        raw_mb = float(f_raw.tell()) / 1024.0 / 1024.0
        f_raw.close()
        f_comp.close()
        comp_mb = float(os.path.getsize(comp_name)) / 1024.0 / 1024.0
        print "    Size before compression: %.2f MB, after: %.2f MB" % (raw_mb, comp_mb)

        # Remove raw file
        os.remove(tmp_name)
        delta = (datetime.now() - start)
        sec = float(delta.seconds) + float(delta.microseconds) / 1000000.0
        print    "  Finished compressing %s as #%d in %.2fs (r: %.2fMB/s, w: %.2fMB/s)" % (filename, next_log_num, sec, (raw_mb/sec), (comp_mb/sec))

