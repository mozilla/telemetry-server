# Combine output from multiple daily runs of am_exceptions.py map/reduce
# into a weekly summary
# usage: combine.py output-filename input-filename [input-filename ...]

import unicodecsv as ucsv
import gzip
import sys
from collections import defaultdict, Counter

APP_COLUMN=0
OS_COLUMN=1
VER_COLUMN=2
CHAN_COLUMN=3
TEXT_COLUMN=4
COUNT_COLUMN=5

# Accumulate the data into a big dict.
# We use the tuple (app, os, version, channel) as our key
# for each key, keep a Counter dict of text => total count
# e.g. {("Firefox","WINNT","29","nightly"): {"Sessions": 4500, "exception text 1": 5, ...},
#       ("Fennec","Android","30","aurora"): {"Sessions":400, ...}}
accum = defaultdict(Counter)

total_rows = 0

outfilename = sys.argv[1]
for a in sys.argv[2:]:
    print "processing", a
    f = gzip.open(a, 'rb')
    reader = ucsv.reader(f)
    headers = reader.next()
    rowcount = 0

    for row in reader:
        key = (row[APP_COLUMN], row[OS_COLUMN], row[VER_COLUMN], row[CHAN_COLUMN])
        accum[key][row[TEXT_COLUMN]] += int(row[COUNT_COLUMN])
        rowcount += 1
    total_rows += rowcount
    f.close()

outfile = gzip.open(outfilename, "wb")
writer = ucsv.writer(outfile)
headers.append("sessions")
writer.writerow(headers)

print "Generating", outfilename

for key, texts in accum.iteritems():
    if 'Sessions' in texts:
        sessions = texts['Sessions']
    else:
        print "No session count for " + str(key)
        sessions = 0

    for text, count in texts.iteritems():
        if text == 'Sessions':
            continue
        line = list(key)
        line.extend([text, count, sessions])
        print line
        writer.writerow(line)
outfile.close()
