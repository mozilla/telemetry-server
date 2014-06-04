# Combine output from multiple daily runs of am_exceptions.py map/reduce
# into a weekly summary
# usage: combine.py output-filename input-filename [input-filename ...]

import unicodecsv as ucsv
import gzip
import sys
from collections import defaultdict, Counter

APP_COLUMN=1
OS_COLUMN=2
VER_COLUMN=3
CHAN_COLUMN=4
TEXT_COLUMN=5

# Accumulate the data into a big dict.
# We use the tuple (app, os, version, channel) as our key
# for each key, keep a Counter dict of text => total count
# e.g. {("Firefox","WINNT","29","nightly"): {"Sessions": 4500, "exception text 1": 5, ...},
#       ("Fennec","Android","30","aurora"): {"Sessions":400, ...}}
exceptions = defaultdict(Counter)

outfilename = sys.argv[1]
for a in sys.argv[2:]:
    print "processing", a
    f = gzip.open(a, 'rb')

    for line in f:
        try:
            keyblob, data = line.split("\t", 1)
        except:
            print line
            continue
        key = eval(keyblob)

        if key[0] == "E":
            excKey = key[1:5]
            exceptions[excKey][key[5]] += int(data)
    f.close()

# Write out gathered exceptions data
outfile = gzip.open(outfilename, "wb")
writer = ucsv.writer(outfile)
writer.writerow(["app_name", "platform", "app_version", "app_update_channel",
                 "message", "count", "sessions"])

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
        writer.writerow(line)
outfile.close()
