# Analyze addonDetails sections from telemetry to extract addon details

import simplejson as json
import io
import unicodecsv as ucsv
from cStringIO import StringIO
import re
from collections import defaultdict, Counter
import math


stamps = ['AMI_startup_begin',
          'XPI_startup_begin',
          'XPI_bootstrap_addons_begin',
          'XPI_bootstrap_addons_end',
          'XPI_startup_end',
          'AMI_startup_end']

section_regex = re.compile(r',"(?:info|addonDetails|slowSQL|ver|log|fileIOReports|histograms|lateWrites|addonHistograms|UIMeasurements|threadHangStats|simpleMeasurements|chromeHangs|slowSQLStartup)":')
# Extract a top level section out of the telemetry JSON packet by guessing at string boundaries
def jsonPart(j, section):
  # Find the start of the requested section
  startPattern = '"' + section + '":[[{]'
  secmatch = re.search(startPattern, j)
  if not secmatch:
    return None
  # Now find the first section start after that
  endmatch = section_regex.search(j, secmatch.end())
  if not endmatch:
    print "Can't find an ending tag after", section, "in", j
    return None
  # print j[secmatch.end() - 1 : endmatch.start()]
  return json.loads(j[secmatch.end() - 1 : endmatch.start()])


# Crudely convert a value to a log-scaled bucket
# Magic number 0.34 gives us a reasonable spread of buckets
# for things measured in milliseconds
def logBucket(v, spread = 0.34):
  if v < 1:
    return v
  return int(math.exp(int(math.log(v) / spread) * spread))

version_regex = re.compile(r'^([0-9]+).*$')

def clean_version(ver):
    m = version_regex.match(ver)
    if m:
        return m.group(1)
    return ver

def writeExc(cx, app, platform, version, channel, text):
    cx.write(("E", app, platform, version, channel, text), 1)

# Map the add-on manager exception data
def mapExc(cx, appName, os, appVersion, appUpdateChannel, j):
    s = jsonPart(j, 'simpleMeasurements')
    if not s:
        writeExc(cx, appName, os, appVersion, appUpdateChannel, "No simpleMeasurements")
        return

    # Make sure we have all our phase timestamps
    missing_stamp = "NONE"
    for stamp in stamps:
        if not stamp in s:
            missing_stamp = stamp
            break

    if not 'addonManager' in s:
        writeExc(cx, appName, os, appVersion, appUpdateChannel, missing_stamp + ": No addonManager")
        return
    a = s['addonManager']

    if 'exception' in a:
        writeExc(cx, appName, os, appVersion, appUpdateChannel, json.dumps(a['exception']))
    elif missing_stamp != "NONE":
        # missing stamp but no exception logged!
        writeExc(cx, appName, os, appVersion, appUpdateChannel, missing_stamp + ": No exception")

def map(k, d, v, cx):
    reason, appName, appUpdateChannel, appVersion, appBuildID, submission_date = d
    appVersion = clean_version(appVersion)
    i = jsonPart(v, 'info')
    if not i:
        return
    os = i['OS']

    writeExc(cx, appName, os, appVersion, appUpdateChannel, "Sessions")
    mapExc(cx, appName, os, appVersion, appUpdateChannel, v)

    # Now report the per-add-on measurements
    try:
      d = jsonPart(v, 'addonDetails')
      if not d:
          return
      x = d['XPI']
    except KeyError:
      return
    for addonID, details in x.iteritems():
      result = {}
      send = False
      for measure, val in details.iteritems():
        if measure.endswith('_MS'):
          result[measure] = {logBucket(val): 1}
          send = True
        if measure == 'scan_items':
          # counting individual files, so use narrower buckets
          result[measure] = {logBucket(val, 0.2): 1}
          send = True
      addonName = None
      if 'name' in details:
        addonName = details['name']
      if addonName is None:
        addonName = "?"
      if send:
        try:
          cx.write(("A", appName, os, appVersion, appUpdateChannel, addonID, addonName),
            {measure: dict(hist) for measure, hist in result.iteritems()})
        except TypeError:
          print key, addonName, details


def combine(k, v, cx):
    if k[0] == "E":
        cx.write(k, sum(v))
        return

    # else it's an addon performance record
    result = defaultdict(Counter);
    for val in v:
      for field, counts in val.iteritems():
          result[field].update(counts)

    cx.write(k, result)

def reduce(k, v, cx):
    if k[0] == "E":
        cx.write(json.dumps(k), sum(v))
        return

    # else it's an addon performance record
    result = defaultdict(Counter);
    for val in v:
      for field, counts in val.iteritems():
          result[field].update(counts)

    cx.write(json.dumps(k).replace("\t", " "),
             json.dumps({measure: dict(hist) for measure, hist in result.iteritems()}))
