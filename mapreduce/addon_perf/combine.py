# Combine output from multiple daily runs of addon_perf.py map/reduce
# into a weekly summary
# usage: combine.py output-path date input-filename [input-filename ...]

import unicodecsv as ucsv
import simplejson as json
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

# For add-on performance data, for each key (app info + add-on ID),
# keep a dict of measure name to Counter containing histogram
def dc(): return defaultdict(Counter)
addonPerf = defaultdict(dc)
# measures = ['scan_items', 'scan_MS', 'startup_MS', 'shutdown_MS', 'install_MS', 'uninstall_MS']
measures = ['scan_items', 'scan_MS', 'startup_MS', 'shutdown_MS']
# And keep a separate dict of total session count for (appName, platform)
# across all channels/versions
addonSessions = Counter()

# Keep track of how many different names we see for a given add-on ID
addonNames = defaultdict(Counter)

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
                exceptions[excKey][key[5]] += int(datablob)
                if key[5] == 'Sessions':
                    addonSessions[(key[1], key[2])] += int(datablob)
                continue
            # otherwise it's an add-on performance data point
            # For now, aggregate over app version and channel
            # so the key is just appName, platform, addon ID
            aoKey = (key[1], key[2], key[5])
            data = json.loads(datablob)
            for measure in measures:
                if measure in data:
                    addonPerf[aoKey][measure].update(data[measure])
            # extract add-on names; might be a single entry, might be a histogram...
            if len(key) == 7:
                addonNames[key[5]][key[6]] += 1
            else:
                addonNames[key[5]].update(data['name'])
        except Exception as e:
            print "Bad line: " + str(e) + ": " + line
            continue
    f.close()

# Write out gathered exceptions data
outfilename = outpath + "/weekly_exceptions_" + outdate + ".csv.gz"
outfile = gzip.open(outfilename, "w")
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

# take a dict of {bucket: count, ...} and return a list of percentiles
# [count, total, 50th, 75th, 95th, max]
def getPercentiles(bucketList):
    if bucketList == None:
      return [0, 0, 0, 0, 0, 0]
    if 'sum' in bucketList:
        total = bucketList['sum']
        del bucketList['sum']
    else:
        # Get rough total by adding up all the buckets
        total = 0
        for bucket, count in bucketList.iteritems():
            total = total + (int(bucket) * count)
    points = sum(bucketList.values())
    buckets = sorted(bucketList.keys(), key = int)
    result = [points, total]
    accum = 0
    b = iter(buckets)
    bucket = 0
    for percentile in [50, 75, 95]:
      while accum < (points * percentile / 100):
        bucket = b.next()
        accum += bucketList[bucket]
      result.append(bucket)
    result.append(buckets[-1])
    return result

# Write out gathered add-on info
aofilename = outpath + "/weekly_addons_" + outdate + ".csv.gz"
print "Generating", aofilename

aofile = gzip.open(aofilename, "w")
aoWriter = ucsv.writer(aofile)
aoWriter.writerow(["app_name", "platform", "addon ID", "names",
                   "measure", "sessions", "count", "total", "50%", "75%", "95%", "max"])
for key, values in addonPerf.iteritems():
    for measure, hist in values.iteritems():
        line = list(key)
        line.append(json.dumps(addonNames.get(key[2])))
        line.append(measure)
        line.append(addonSessions.get((key[0], key[1]), 0))
        line.extend(getPercentiles(hist))
        aoWriter.writerow(line)
aofile.close()
