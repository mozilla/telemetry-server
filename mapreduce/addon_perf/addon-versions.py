# Process daily add-on telemetry extract to see variation in add-on version #
# usage: addon-versions.py input-filename [input-filename ...]

import unicodecsv as ucsv
import simplejson as json
import gzip
import sys
import re
from collections import defaultdict, Counter

APP_COLUMN=1
OS_COLUMN=2
VER_COLUMN=3
CHAN_COLUMN=4
TEXT_COLUMN=5

# Keep track of how many version #s we see for an add-on ID
addonVersions = defaultdict(Counter)
# Keep track of how many different names we see for a given add-on ID
addonNames = defaultdict(Counter)

# Total number of pings received
sessions = 0

for a in sys.argv[1:]:
    print "processing", a
    with gzip.open(a, 'rb') as f:

        for line in f:
            try:
                keyblob, datablob = line.split("\t", 1)
                key = json.loads(keyblob)

                if key[0] == "E":
                    if key[5] == 'Sessions':
                        sessions += int(datablob)
                    continue
                (addonID, sep, version) = key[5].rpartition(':')
                data = json.loads(datablob)
                addonVersions[addonID][version] += sum(data['name'].values())
                addonNames[addonID].update(data['name'])
            except Exception as e:
                print "Bad line: " + str(e) + ": " + line
                continue

# Get the most popular name for the add-on, collapsing ugly broken unicode
rx = re.compile(u'\ufffd+')
def getName(addonID):
    names = addonNames.get(addonID, {})
    if "?" in names:
        del names["?"]
    if len(names) < 1:
        return "?"
    return max(names, key=names.get)

print sessions, "sessions,", len(addonVersions), "different add-on IDs"

# Things worth knowing?
# Total different add-on IDs
# total # sessions
# count of IDs that have more than one version
# for each ID: most popular name, total count, # versions, count of most popular version

writer = ucsv.writer(sys.stdout)
writer.writerow(['Add-on ID', 'name', 'total', 'versions', 'mainVersion', 'count'])

# add-ons with more than one version
multiversion = 0

for addonID, counts in addonVersions.iteritems():
    name = getName(addonID)
    total = sum(counts.values())
    versions = len(counts)
    if '?' in counts:
        # Don't count the 'disabled' version
        versions = versions - 1
    if versions < 2:
        continue

    multiversion += 1
    version, count = max(counts.iteritems(), key=lambda k:k[1])

    writer.writerow([addonID, name, total, versions, version, count])

print
print multiversion, "add-ons with more than one version"
