"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""
import sys, os, re, glob
from persist import StorageLayout
from datetime import datetime

comp_name = sys.argv[1]
filename = sys.argv[2]

if comp_name.endswith(".gz"):
    import gzip as compression
    acs = ".gz"
elif comp_name.endswith('.lzma'):
    try:
        import lzma as compression
    except ImportError:
        from backports import lzma as compression
    acs = ".lzma"
else:
    print "Unknown compression type:", comp_type
    sys.exit(2)

f_comp = compression.open(comp_name, "rb")
print "Decompressing %s to %s" % (comp_name, filename)

start = datetime.now()
f_raw = open(filename, "wb")
f_raw.writelines(f_comp)
raw_mb = float(f_raw.tell()) / 1024.0 / 1024.0
f_raw.close()
f_comp.close()
comp_mb = float(os.path.getsize(comp_name)) / 1024.0 / 1024.0
print "    Size before compression: %.2f MB, after: %.2f MB" % (raw_mb, comp_mb)

delta = (datetime.now() - start)
sec = float(delta.seconds) + float(delta.microseconds) / 1000000.0
print    "  Finished compressing %s as %s in %.2fs (r: %.2fMB/s, w: %.2fMB/s)" % (filename, comp_name, sec, (raw_mb/sec), (comp_mb/sec))
