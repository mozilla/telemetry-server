# Experiments export
import simplejson as json
import traceback
import sys
import urllib

def map(k, d, v, cx):
    [reason, appName, appUpdateChannel, appVersion, appBuildID, submission_date] = d
    if appName != "Firefox":
        print >>sys.stderr, "Got non-Firefox appName", appName
        return

    cx.write(("Totals", appUpdateChannel, appVersion), 1)
    process = False
    if v.find("EXPERIMENT") != -1:
        process = True
    elif v.find("activeExperiment") != -1:
        process = True

    if not process:
        return

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
                          appVersion) + tuple(item[2:]), 1)

        info = j.get("info", {})
        active = info.get("activeExperiment", None)
        if active is not None:
            activeBranch = info.get("activeExperimentBranch", None)
            cx.write(("ACTIVE", appUpdateChannel, appVersion, active, activeBranch), 1)

    except Exception as e:
        print >>sys.stderr, "Error during map: ", e
        cx.write(("Error",), "%s: %s\n%s" % (e, d, traceback.format_exc()))

def reduce(k, v, cx):
    if k[0] == "Error":
        cx.writecsv(("Error", v))
    else:
        cx.writecsv(list(k) + [sum(v)])
