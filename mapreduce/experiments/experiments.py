# SlowSQL export, ported from:
#   https://github.com/mozilla-metrics/telemetry-toolbox
import csv
import io
import re
import simplejson as json
import traceback

def safe_key(pieces):
    output = io.BytesIO()
    writer = csv.writer(output, quoting=csv.QUOTE_MINIMAL)
    writer.writerow(pieces)
    # remove the trailing EOL chars:
    return unicode(output.getvalue().strip().translate(eol_trans_table))

def map(k, d, v, cx):
    [reason, appName, appUpdateChannel, appVersion, appBuildID, submission_date] = d
    if appName != "Firefox":
        return

    cx.write(("Totals", appUpdateChannel, appVersion), 1)
    try:
        j = json.loads(v)
        for item in j.get("log", []):
            entrytype = item[0]
            if entrytype == "EXPERIMENT_ACTIVATION":
                item.pop(1) # The time isn't relevant for now
                cx.write(tuple(item), 1)
            elif entrytype == "EXPERIMENT_TERMINATION":
                cx.write(tuple(item), 1)
    except Exception as e:
        cx.write(("Error",), str(e) + traceback.format_exc() + d)

def reduce(k, v, cx):
    if k[0] == "Error":
        cx.writecsv(("Error", v))
    else:
        cx.writecsv(list[k] + [sum(v)])
