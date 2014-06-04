import simplejson as json
import io
import unicodecsv as ucsv
from cStringIO import StringIO
import re

stamps = ['AMI_startup_begin',
          'XPI_startup_begin',
          'XPI_bootstrap_addons_begin',
          'XPI_bootstrap_addons_end',
          'XPI_startup_end',
          'AMI_startup_end']

version_regex = re.compile(r'^([0-9]+).*$')

def clean_version(ver):
    m = version_regex.match(ver)
    if m:
        return m.group(1)
    return ver

def report(cx, app, platform, version, channel, text):
    f = StringIO()
    w = ucsv.writer(f, encoding='utf-8')
    w.writerow((app, platform, version, channel, text))
    cx.write(f.getvalue().strip(), 1)
    f.close()

def map(k, d, v, cx):
    reason, appName, appUpdateChannel, appVersion, appBuildID, submission_date = d
    appVersion = clean_version(appVersion)
    j = json.loads(v)
    if not 'info' in j:
        return
    i = j['info']
    os = i['OS']

    report(cx, appName, os, appVersion, appUpdateChannel, "Sessions")

    if not 'simpleMeasurements' in j:
        report(cx, appName, os, appVersion, appUpdateChannel, "No simpleMeasurements")
        return
    s = j['simpleMeasurements']

    # Make sure we have all our phase timestamps
    missing_stamp = "NONE"
    for stamp in stamps:
        if not stamp in s:
            missing_stamp = stamp
            break

    if not 'addonManager' in s:
        report(cx, appName, os, appVersion, appUpdateChannel, missing_stamp + ": No addonManager")
        return
    a = s['addonManager']

    if 'exception' in a:
        report(cx, appName, os, appVersion, appUpdateChannel, json.dumps(a['exception']))
    elif missing_stamp != "NONE":
        # missing stamp but no exception logged!
        report(cx, appName, os, appVersion, appUpdateChannel, missing_stamp + ": No exception")

def setup_reduce(cx):
    cx.field_separator = ","

def reduce(k, v, cx):
    cx.write(k, sum(v))
