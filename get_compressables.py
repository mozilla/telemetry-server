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

pcs = StorageLayout.PENDING_COMPRESSION_SUFFIX
acs = StorageLayout.COMPRESSED_SUFFIX

today = date.today().strftime("%Y%m%d")
log_date_pattern = re.compile("^.*\.([0-9]{8})\.log$")

start = datetime.now()

total_files = 0
matches = 0

for root, dirs, files in os.walk(searchdir):
    for f in files:
        total_files += 1
        candidate = False

        if f.endswith(pcs):
            candidate = True
        else:
            m = log_date_pattern.match(f)
            if m and m.group(1) < today:
                # it's a regular .log file, but it's older than today, so it will
                # not be written to anymore. Let's compress it.
                candidate = True
        if candidate:
            matches += 1
            print os.path.join(root, f)

delta = (datetime.now() - start)
sec = float(delta.seconds) + float(delta.microseconds) / 1000000.0
sys.stderr.write("#  Found %d of %d files to compress in %.2fs (%.2f per sec)\n" % (matches, total_files, sec, (total_files/sec)))

