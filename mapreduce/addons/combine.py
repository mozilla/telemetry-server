import unicodecsv as ucsv
import gzip
import os
import re
import sys
import numpy
from collections import Counter

APP_COLUMN=0
OS_COLUMN=1
VER_COLUMN=2
CHAN_COLUMN=3
TEXT_COLUMN=4
COUNT_COLUMN=5

# Accumulate the data into a big dict.
# We use the tuple (app, os, version, channel) as our key
# for each key, keep a Counter dict of text => total count
accum = defaultdict(Counter)

filenames = {}
totals = {}
total_rows = 0
output_dir = sys.argv[1]
week_start = sys.argv[2]
week_end = sys.argv[3]
inputs = []
file_pattern = re.compile("^am_exceptions([0-9]{8}).csv.gz$")

for f in os.listdir('.'):
    m = file_pattern.match(f)
    if m:
        print "found a file:", f
        d = m.group(1)
        if d >= week_start and d <= week_end:
            print "and it's good!", f
            inputs.append(os.path.realpath(f))

for a in inputs:
    print "processing", a
    f = gzip.open(a, 'rb')
    reader = ucsv.reader(f)
    headers = reader.next()
    rowcount = 0

# APP_COLUMN=0
# OS_COLUMN=1
# VER_COLUMN=2
# CHAN_COLUMN=3
# TEXT_COLUMN=4
# COUNT_COLUMN=5

    for row in reader:
        key = (row[APP_COLUMN], row[OS_COLUMN], row[VER_COLUMN], row[CHAN_COLUMN])
        accum[key][row[TEXT_COLUMN]] += int(row[COUNT_COLUMN])
        rowcount += 1
    total_rows += rowcount
    f.close()

print "overall, found", total_rows, "rows, with", len(filenames.keys()), "unique filenames"

filename = "weekly_am_exceptions_{0}-{1}.csv.gz".format(week_start, week_end)
outfile = gzip.open(os.path.join(output_dir, filename), "wb")
writer = ucsv.writer(outfile)
headers.append("sessions")
writer.writerow(headers)

print "Generating", filename

for key, texts in accum:
    if 'Sessions' in texts:
        sessions = texts['Sessions']
    else:
        print "No session count for " + str(key)
        sessions = 0

    for text, count in texts:
        if text == 'Sessions':
            continue
        line = list(key).extend(count, sessions)
        writer.writerow(line)
outfile.close()
