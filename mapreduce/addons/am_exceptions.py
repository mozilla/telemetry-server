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

def setup_reduce(cx):
    cx.field_separator = ","

def reduce(k, v, cx):
    cx.write(k, sum(v))
