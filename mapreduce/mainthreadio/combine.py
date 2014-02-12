import csv
import gzip
import os
import re
import sys
import numpy

SUB_COLUMN=0
APP_COLUMN=1
VER_COLUMN=2
CHAN_COLUMN=3
FILE_COLUMN=4
SUBMISSION_COUNT_COLUMN=5
MEDIAN_TIME_COLUMN=6
MEDIAN_COUNT_COLUMN=7
version_regex = re.compile(r'^([0-9]+).*$')

def clean_version(ver):
    m = version_regex.match(ver);
    if m:
        return m.group(1);
    return ver;

def get_key(row):
    return ",".join(row[APP_COLUMN:FILE_COLUMN]);

def combine(f, rows):
    seen_keys = {}
    total_docs = 0
    f_docs = 0
    f_median_time = []
    f_median_count = []

    for r in rows:
        k = get_key(r)

        try:
            if k not in seen_keys:
                total_docs += totals[k]
                seen_keys[k] = 1
        except KeyError, e:
            print "Key not found:", k, r

        f_docs += int(r[SUBMISSION_COUNT_COLUMN])
        f_median_time.append(float(r[MEDIAN_TIME_COLUMN]))
        f_median_count.append(float(r[MEDIAN_COUNT_COLUMN]))

    f_comb_median_time = numpy.median(f_median_time)
    f_comb_median_count = numpy.median(f_median_count)
    return [round(float(f_docs) / float(total_docs) * 100, 2), f_docs, f_comb_median_time, f_comb_median_count, f]

filenames = {}
totals = {}
total_rows = 0
output_dir = sys.argv[1]
week_start = sys.argv[2]
week_end = sys.argv[3]
inputs = []
file_pattern = re.compile("^mainthreadio([0-9]{8}).csv.gz$")

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
    reader = csv.reader(f)
    headers = reader.next()
    rowcount = 0

    for row in reader:
        if row[FILE_COLUMN] == "TOTAL":
            # sum up the submission_dates.
            total_key = get_key(row)
            if total_key not in totals:
                totals[total_key] = 0
            totals[total_key] += int(row[SUBMISSION_COUNT_COLUMN])
        else:
            fk = "\t".join([row[FILE_COLUMN], row[APP_COLUMN], row[CHAN_COLUMN], clean_version(row[VER_COLUMN])])
            if fk not in filenames:
                filenames[fk] = []
            filenames[fk].append(row)

        rowcount += 1
    total_rows += rowcount
    f.close()

print "overall, found", total_rows, "rows, with", len(filenames.keys()), "unique filenames"

combined = []
for f, rows in filenames.iteritems():
    combined.append(combine(f, rows))

for stub, column in [["frequency", 1], ["median_time", 2], ["median_count", 3]]:
    filename = "weekly_{0}_{1}-{2}.csv.gz".format(stub, week_start, week_end)
    counters = {}
    outfile = gzip.open(os.path.join(output_dir, filename), "wb")
    writer = csv.writer(outfile)

    print "Generating", filename

    for row in sorted(combined, key=lambda r: r[column], reverse=True):
        f = row[-1]
        filename, app, chan, ver = f.split("\t")
        writer.writerow(row[0:-1] + [filename, app, chan, ver])
    outfile.close()
