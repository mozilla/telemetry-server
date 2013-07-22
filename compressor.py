"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""

import sys, os, re, gzip, glob
from persist import StorageLayout
from datetime import datetime
from datetime import date

searchdir = sys.argv[1]

dry_run = False

if len(sys.argv) > 2 and sys.argv[2] == "--dry-run":
    dry_run = True

paths = {}
pcs = StorageLayout.PENDING_COMPRESSION_SUFFIX
acs = StorageLayout.COMPRESSED_SUFFIX

today = date.today().strftime("%Y%m%d")

for root, dirs, files in os.walk(searchdir):
    for f in files:
        if root not in paths:
            paths[root] = []

        if f.endswith(pcs):
            paths[root].append(f)
        elif f.endswith(".log") and f[-12:-4] < today:
            # it's a regular .log file, but it's older than today, so it will
            # not be written to anymore.
            paths[root].append(f)

for path in paths.iterkeys():
    pending = paths[path]
    print "Found a path %s with %d pending files" % (path, len(pending))
    for filename in pending:
        print "  Compressing", filename

        # Don't actually do anything.
        if dry_run:
            continue

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

