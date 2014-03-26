import simplejson as json
import numpy
import io
import csv
from string import maketrans

def clean(s):
    return normalize(s).translate(None, ",")

def normalize(s):
    if type(s) == unicode:
        return s.encode('utf8', 'ignore')
    else:
        return str(s)

def safe_key(pieces):
    output = io.BytesIO()
    writer = csv.writer(output, quoting=csv.QUOTE_MINIMAL)
    writer.writerow(pieces)
    return output.getvalue().strip()

def map(k, d, v, cx):
    global n_pings

    parsed = json.loads(v)
    reason, appName, appUpdateChannel, appVersion, appBuildID, submission_date = d

    if not "fileIOReports" in parsed:
        return

    if not parsed["fileIOReports"]:
        return

    cx.write(safe_key([submission_date, appName, appVersion, appUpdateChannel, "TOTAL"]), [0, 0, 0, 0, 0, 0])

    for f, arr in parsed["fileIOReports"].iteritems():
        if len(arr) != 3: # Don't support the old format
            continue

        if arr[1] is not None:
            cx.write(safe_key([submission_date, appName, appVersion, appUpdateChannel, clean(f)]), arr[1])

def setup_reduce(cx):
    cx.field_separator = ","

def reduce(k, v, cx):
    totals = []
    avgs = []
    counts = []
    n_opens = []
    n_reads = []
    n_writes = []
    n_fsyncs = []
    n_stats = []
    n_pings = 0

    for total, n_open, n_read, n_write, n_fsync, n_stat in v:
        totals.append(total)
        n_opens.append(n_open)
        n_reads.append(n_read)
        n_writes.append(n_write)
        n_fsyncs.append(n_fsync)
        n_stats.append(n_stat)
        n_pings += 1

        count = n_open + n_read + n_write + n_fsync + n_stat
        counts.append(count)

    if n_pings > 100:
        # Output fields:
        # submission_date, app_name, app_version
        # app_update_channel, filename, submission_count, median_time, median_count
        cx.write(k, ",".join([str(n_pings), str(numpy.median(totals)), str(numpy.median(count))]))
