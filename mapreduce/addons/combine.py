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

# For add-on performance data, for each key (app info + add-on ID + add-on name),
# keep a dict of measure name to Counter containing histogram
def dc(): return defaultdict(Counter)
addonPerf = defaultdict(dc)
measures = ['scan_items', 'scan_MS', 'startup_MS', 'shutdown_MS', 'install_MS', 'uninstall_MS']

outpath = sys.argv[1]
outdate = sys.argv[2]

for a in sys.argv[3:]:
    print "processing", a
    f = gzip.open(a, 'rb')

    for line in f:
        try:
            keyblob, datablob = line.split("\t", 1)
            key = json.loads(keyblob)

            if key[0] == "E":
                excKey = tuple(key[1:5])
                exceptions[excKey][key[5]] += int(data)
                continue
            # otherwise it's an add-on performance data point
            # For now, aggregate over app version and channel
            aoKey = (key[1], key[2], key[6], key[7])
            data = json.loads(datablob)
            for measure in measures:
                if measure in data:
                    addonPerf[aoKey][measure].update(data[measure])
        except:
            print "Bad line: " + line
            continue
    f.close()

# Write out gathered exceptions data
outfile = gzip.open(outpath + "weekly_exceptions_" + outdate + ".csv.gz", "wb")
writer = ucsv.writer(outfile)
writer.writerow(["app_name", "platform", "app_version", "app_update_channel",
                 "message", "count", "sessions"])

print "Generating", outfilename

for key, texts in exceptions.iteritems():
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

# Write out gathered add-on info
aofile = gzip.open(outpath + "weekly_addons_" + outdate + ".csv.gz", "wb")
aoWriter = ucsv.writer(outfile)
aoWriter.writerow(["app_name", "platform", "addon ID", "addon name",
                   "measure", "histogram"])
for key, values in addonPerf.iteritems():
    for measure, hist in values.iteritems():
        line = list(key)
        line.extend([measure, json.dumps(hist)])
        aoWriter.writerow(line)
aofile.close()
