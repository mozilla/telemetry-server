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

    if "fileIOReports" not in v or '"fileIOReports":null' in v:
        return

    parsed = json.loads(v)
    reason, appName, appUpdateChannel, appVersion, appBuildID, submission_date = d

    startup_sub = False
    execution_sub = False
    shutdown_sub = False

    for f, arr in parsed["fileIOReports"].iteritems():
        if len(arr) != 3: # Don't support the old format
            continue

        if arr[0] is not None:
            cx.write(safe_key([submission_date, appName, appVersion, appUpdateChannel, "startup", clean(f)]), [arr[0][0], sum(arr[0][1:])])
            if not startup_sub:
                cx.write(safe_key([submission_date, appName, appVersion, appUpdateChannel, "startup", "TOTAL"]), [0, 0])
                startup_sub = True

        if arr[1] is not None:
            cx.write(safe_key([submission_date, appName, appVersion, appUpdateChannel, "execution", clean(f)]), [arr[1][0], sum(arr[1][1:])])
            if not execution_sub:
                cx.write(safe_key([submission_date, appName, appVersion, appUpdateChannel, "execution", "TOTAL"]), [0, 0])
                execution_sub = True

        if arr[2] is not None:
            cx.write(safe_key([submission_date, appName, appVersion, appUpdateChannel, "shutdown", clean(f)]), [arr[2][0], sum(arr[2][1:])])
            if not shutdown_sub:
                cx.write(safe_key([submission_date, appName, appVersion, appUpdateChannel, "shutdown", "TOTAL"]), [0, 0])
                shutdown_sub = True

def setup_reduce(cx):
    cx.field_separator = ","

def reduce(k, v, cx):
    totals = []
    counts = []

    if len(v) > 10000:
        sup = min(len(v), 10000)

        for total, count in v[:sup]:
            totals.append(total)
            counts.append(count)

        # Output fields:
        # submission_date, app_name, app_version, app_update_channel, interval, filename,
        # submission_count, median_time, median_count
        cx.write(k, ",".join([str(len(v)), str(numpy.median(totals)), str(numpy.median(counts))]))
