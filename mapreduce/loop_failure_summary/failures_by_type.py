import simplejson as json

def map(k, d, v, cx):
    reason, appName, appUpdateChannel, appVersion, appBuildID, submission_date = d
    try:
        j = json.loads(v)

        # Filter just the ice failure reports:
        if "report" in j and j["report"] == "ice failure":
            cx.write(k, (submission_date, j.get("connectionstate", "UNKNOWN"), v))
    except Exception as e:
        cx.write("ERROR", str(e))

def reduce(k, v, cx):
    if k == "ERROR":
        for err in v:
            cx.write(k, err)
    else:
        # data contains duplicates, so we just output the first record for each
        # key.
        submission_date, connectionstate, payload = v[0]
        cx.write(submission_date, "\t".join((connectionstate, payload)))
