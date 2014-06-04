# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import csv
import gzip
import os
import re
import sys

STACK_COLUMN=0
SUB_COLUMN=1
APP_COLUMN=2
VER_COLUMN=3
CHAN_COLUMN=4
COUNT_COLUMN=5
TOTAL_DUR_COLUMN=6
MEDIAN_DUR_COLUMN=7
APP_UPTIME_COLUMN=8
SYS_UPTIME_COLUMN=9

MAX_ROWS=100

# Expected input columns:
#   hang_stack,submission_date,app_name,app_version,app_update_channel,
#   ping_count,total_duration,median_duration,app_uptime,system_uptime
version_regex = re.compile(r'^([0-9]+).*$')
def clean_version(ver):
    m = version_regex.match(ver);
    if m:
        return m.group(1);
    return ver;

def get_key(row):
    # Excluding COUNT_COLUMN
    return ",".join(row[APP_COLUMN:COUNT_COLUMN]);

def median(v, already_sorted=False):
    ls = len(v)
    if ls == 0:
        return 0
    if already_sorted:
        s = v
    else:
        s = sorted(v)
    middle = int(ls / 2)
    if ls % 2 == 1:
        return s[middle]
    else:
        return (s[middle] + s[middle-1]) / 2.0

def combine(key, rows):
    num_docs = 0
    total_dur = 0
    median_dur = []
    median_app_uptime = []
    median_sys_uptime = []
    for r in rows:
        current_count = int(r[COUNT_COLUMN])
        num_docs += current_count
        total_dur += float(r[TOTAL_DUR_COLUMN])

        # If we insert current_count values with a given row's median, then
        # taking a median over all the values should give a reasonable guess at
        # the overall median.
        median_dur.extend([float(r[MEDIAN_DUR_COLUMN])] * current_count)
        median_app_uptime.extend([float(r[APP_UPTIME_COLUMN])] * current_count)
        median_sys_uptime.extend([float(r[SYS_UPTIME_COLUMN])] * current_count)

    key_median = median(median_dur)
    #print "median of", median_dur, "is", key_median
    return [num_docs, key_median, total_dur, median(median_app_uptime), median(median_sys_uptime), key]

hang_stacks = {}
totals = {}
total_rows = 0
output_dir = sys.argv[1]
week_start = sys.argv[2]
week_end = sys.argv[3]
inputs = []

file_pattern = re.compile("^chromehangs-common-([0-9]{8}).csv.gz$")
for root, dirs, files in os.walk("."):
    for f in files:
        m = file_pattern.match(f)
        if m:
            print "found a file:", f
            d = m.group(1)
            if d >= week_start and d <= week_end:
                print "and it's good!", f
                inputs.append(os.path.join(root, f))
        #else:
        #    print "no match file:", f

for a in inputs:
    print "processing", a
    f = gzip.open(a, 'rb')
    reader = csv.reader(f)
    headers = reader.next()
    #for i in range(len(headers)):
    #    print "field", i, "is", headers[i]
    rowcount = 0
    for row in reader:
        if len(row) > SYS_UPTIME_COLUMN:
            stack = row[STACK_COLUMN].replace("\t", " ")
            stack_key = "\t".join([stack, row[APP_COLUMN], row[CHAN_COLUMN], clean_version(row[VER_COLUMN])])
            if stack_key not in hang_stacks:
                hang_stacks[stack_key] = []
            hang_stacks[stack_key].append(row)
        else:
            print "not enough columns:", row
        rowcount += 1
    total_rows += rowcount
    f.close()

print "Overall, found", total_rows, "rows, with", len(hang_stacks.keys()), "unique stacks"

combined = []
for key, rows in hang_stacks.iteritems():
    #print "Combining", len(rows), "items for", key
    combined.append(combine(key, rows))

for stub, column in [["frequency", 0],
                     ["median_duration", 1],
                     ["total_duration", 2]]:
    filename = "weekly_{0}_{1}-{2}.csv.gz".format(stub, week_start, week_end)
    print "Generating", filename
    counters = {}
    outfile = gzip.open(os.path.join(output_dir, filename), "wb")
    writer = csv.writer(outfile)
    for row in sorted(combined, key=lambda r: r[column], reverse=True):
        stack, app, chan, ver = row[-1].split("\t")
        # We only need up to MAX_ROWS items for each set of dimensions, so
        # once we've seen that many, skip any more.
        counter_key = "\t".join([app, chan, ver])
        if counter_key not in counters:
            counters[counter_key] = 0
        counters[counter_key] += 1

        if counters[counter_key] > MAX_ROWS:
            #print "Already saw key", counter_key, counters[counter_key], "times... skipping"
            continue

        writer.writerow(row[0:-1] + [stack, app, chan, ver])
    outfile.close()

# Output columns:
#stack, document_count, median_duration, total_duration, median_app_uptime, median_sys_uptime, stack, app_name, channel, version
