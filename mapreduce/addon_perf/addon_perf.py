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
def stringPart(j, section):
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
  return j[secmatch.end() - 1 : endmatch.start()]

def jsonPart(j, section):
  sect = stringPart(j, section)
  if sect:
      return json.loads(sect)
  return None


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

# Match the time stamps out of the simpleMeasurements field

# ts_regex = re.compile(r'"simpleMeasurements":{([^{]*?"(AMI_startup_begin|XPI_startup_begin|XPI_bootstrap_addons_begin|XPI_bootstrap_addons_end|XPI_startup_end|AMI_startup_end)":(\d*)){0,6}')
ts_regex = re.compile(r'"(AMI_startup_begin|XPI_startup_begin|XPI_bootstrap_addons_begin|XPI_bootstrap_addons_end|XPI_startup_end|AMI_startup_end)":(\d*)')
exc_regex = re.compile(r'"addonManager":{[^}]*"exception":({[^}]*})')

# Map the add-on manager exception data
def mapExc(cx, appName, os, appVersion, appUpdateChannel, j):
    simple = stringPart(j, 'simpleMeasurements')
    if not simple:
        writeExc(cx, appName, os, appVersion, appUpdateChannel, "No simpleMeasurements")
        return
    matches = ts_regex.findall(simple)
    if (not matches) or (len(matches) == 0):
        writeExc(cx, appName, os, appVersion, appUpdateChannel, "No simpleMeasurements")
        return

    s = dict(matches)
    # Make sure we have all our phase timestamps
    missing_stamp = "NONE"
    for stamp in stamps:
        if not stamp in s:
            missing_stamp = stamp
            break

    excmatch = exc_regex.search(simple);
    if excmatch:
        print excmatch.groups()
        writeExc(cx, appName, os, appVersion, appUpdateChannel, excmatch.group(1))
    elif missing_stamp != "NONE":
        # missing stamp but no exception logged!
        writeExc(cx, appName, os, appVersion, appUpdateChannel, missing_stamp + ": No exception")

# Assorted regexes to match fields we want from the telemetry data
OS_regex = re.compile(r'"OS":"([^"]+)"')

def map(k, d, v, cx):
    reason, appName, appUpdateChannel, appVersion, appBuildID, submission_date = d
    appVersion = clean_version(appVersion)

    osm = OS_regex.search(v)
    if not osm:
        return
    os = osm.group(1)

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
          # sanity check the measure; drop the whole entry if the duration seems crazy
          # twenty minutes is a long time to wait for startup...
          if val > (20 * 60 * 1000):
            print "Unusual", measure, "value", val, "in entry", k, addonID
            return
          result[measure] = {'sum': val, logBucket(val): 1}
          send = True
        if measure == 'scan_items':
          # counting individual files, so use narrower buckets
          result[measure] = {'sum': val, logBucket(val, 0.2): 1}
          send = True
      addonName = None
      if 'name' in details:
        addonName = details['name']
      if addonName is None:
        addonName = "?"
      # Make a pseudo-histogram of the number of times we see each name for an addon
      result['name'] = {addonName: 1}
      if send:
        try:
          cx.write(("A", appName, os, appVersion, appUpdateChannel, addonID),
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
