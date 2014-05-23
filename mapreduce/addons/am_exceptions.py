import simplejson as json
import io
import unicodecsv as ucsv
from cStringIO import StringIO
from string import maketrans

stamps = ['AMI_startup_begin',
          'XPI_startup_begin',
          'XPI_bootstrap_addons_begin',
          'XPI_bootstrap_addons_end',
          'XPI_startup_end',
          'AMI_startup_end']

def report(cx, app, channel, version, missing, text):
    f = StringIO()
    w = ucsv.writer(f, encoding='utf-8')
    w.writerow((app, channel, version, missing, text))
    cx.write(f.getvalue(), 1)
    f.close()

def map(k, d, v, cx):
    reason, appName, appUpdateChannel, appVersion, appBuildID, submission_date = d
    report(cx, appName, appUpdateChannel, appVersion, "None", "Sessions")
    j = json.loads(v)

    if not 'simpleMeasurements' in j:
        report(cx, appName, appUpdateChannel, appVersion, "None", "No simpleMeasurements")
        return
    s = j['simpleMeasurements']

    # Make sure we have all our phase timestamps
    missing_stamp = "NONE"
    for stamp in stamps:
        if not stamp in s:
            missing_stamp = stamp
            break

    if not 'addonManager' in s:
        report(cx, appName, appUpdateChannel, appVersion, missing_stamp, "No addonManager")
        return
    a = s['addonManager']

    if 'exception' in a:
        report(cx, appName, appUpdateChannel, appVersion, missing_stamp, json.dumps(a['exception']))
    elif missing_stamp != "NONE":
        # missing stamp but no exception logged!
        report(cx, appName, appUpdateChannel, appVersion, missing_stamp, "None")

    XXX OLD
    parsed = json.loads(v)

    if not "fileIOReports" in parsed:
        return

    if not parsed["fileIOReports"]:
        return

    startup_sub = False
    execution_sub = False
    shutdown_sub = False

    for f, arr in parsed["fileIOReports"].iteritems():
        if len(arr) != 3: # Don't support the old format
            continue

        if arr[0] is not None:
            cx.write(safe_key([submission_date, appName, appVersion, appUpdateChannel, "startup", clean(f)]), arr[0])
            if not startup_sub:
                cx.write(safe_key([submission_date, appName, appVersion, appUpdateChannel, "startup", "TOTAL"]), [0, 0, 0, 0, 0, 0])
                startup_sub = True

        if arr[1] is not None:
            cx.write(safe_key([submission_date, appName, appVersion, appUpdateChannel, "execution", clean(f)]), arr[1])
            if not execution_sub:
                cx.write(safe_key([submission_date, appName, appVersion, appUpdateChannel, "execution", "TOTAL"]), [0, 0, 0, 0, 0, 0])
                execution_sub = True

        if arr[2] is not None:
            cx.write(safe_key([submission_date, appName, appVersion, appUpdateChannel, "shutdown", clean(f)]), arr[2])
            if not shutdown_sub:
                cx.write(safe_key([submission_date, appName, appVersion, appUpdateChannel, "shutdown", "TOTAL"]), [0, 0, 0, 0, 0, 0])
                shutdown_sub = True

def setup_reduce(cx):
    cx.field_separator = ","

def reduce(k, v, cx):
    cx.write(k, sum(v))
