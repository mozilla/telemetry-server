# SlowSQL export, ported from:
#   https://github.com/mozilla-metrics/telemetry-toolbox
import simplejson as json
import traceback
import sys
import urllib

# The telemetry payload doesn't currently list the current experiment ID. So
# for now we're hardcoding the list of experiments by addon ID
interesting = [
    "jid1-tile-switcher@jetpack",
    ]

def map(k, d, v, cx):
    [reason, appName, appUpdateChannel, appVersion, appBuildID, submission_date] = d
    if appName != "Firefox":
        print >>sys.stderr, "Got non-Firefox appName", appName
        return

    cx.write(("Totals", appUpdateChannel, appVersion), 1)
    try:
        j = json.loads(v)
        for item in j.get("log", []):
            entrytype = item[0]
            if entrytype == "EXPERIMENT_ACTIVATION":
                cx.write(("EXPERIMENT_ACTIVATION",
                          appUpdateChannel,
                          appVersion) + tuple(item[2:]), 1)
            elif entrytype == "EXPERIMENT_TERMINATION":
                cx.write(("EXPERIMENT_TERMINATION",
                          appUpdateChannel,
                          appVersion) + tuple(item[1:]), 1)

        addons = set([urllib.unquote(i.split(":")[0])
                      for i in j.get("info", {}).get("addons", "").split(",")])
        for id in interesting:
            if id in addons:
                cx.write(("ACTIVE", appUpdateChannel, appVersion, id), 1)

    except Exception as e:
        print >>sys.stderr, "Error during map: ", e
        cx.write(("Error",), "%s: %s\n%s" % (e, d, traceback.format_exc()))

def reduce(k, v, cx):
    if k[0] == "Error":
        cx.writecsv(("Error", v))
    else:
        cx.writecsv(list[k] + [sum(v)])
