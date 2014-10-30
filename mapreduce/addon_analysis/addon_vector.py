import simplejson as json
import numpy
import io
import csv
import os
from string import maketrans

import numpy as np
import scipy as sp
import scipy.stats

top_addons = []

with open(os.environ["FINAL_ADDON_FILE"]) as f:
    lines = f.readlines()
    for line in lines:
        line = line.split(',')
        top_addons.append(line[0])

def clean(s):
    try:
        s = s.decode('ascii').strip()
        return s if len(s) > 0 else None
    except:
        return None

def map(k, d, v, cx):
    parsed = json.loads(v)
    reason, appName, appUpdateChannel, appVersion, appBuildID, submission_date = d
    info = parsed['info']
    simple = parsed['simpleMeasurements']

    os = info['OS']
    version = info['version']
    cpucount = info['cpucount']
    memsize = info['memsize'] / 1000

    AMI_startup = simple.get('AMI_startup_begin', None)
    firstPaint = simple.get('firstPaint', None)
    startup = firstPaint - AMI_startup if firstPaint and AMI_startup else None
    shutdown = simple.get('shutdownDuration', None)

    # Let's remove machines with older configurations or with suspect startup times
    if not startup or startup < 0 or not version.startswith("6") or os != "WINNT" \
       or cpucount < 2 or memsize < 2 or startup > 60000 or not shutdown:
        return

    # Build a list of add-ons
    addons = parsed['addonDetails'].get('XPI', {})
    addon_names = set()

    for addon, desc in addons.iteritems():
        if "name" in desc:
            name = clean(desc["name"])

            if name is not None:
                addon_names.add(name.replace(",", "-"))

    # Remove incorrect pings
    if "Default" not in addon_names:
        return

    # Let's count Default only when no other add-on is enabled
    if len(addon_names) > 1:
        addon_names.remove("Default")

    addon_names = tuple(addon_names.intersection(top_addons))
    cx.write(tuple([k]) + addon_names, [startup, shutdown, cpucount, memsize])

def setup_reduce(cx):
    cx.field_separator = ","

def reduce(k, v, cx):
    if len(v) != 1:
        return

    map = __builtins__['map']
    addon_vector = ""
    addons = set(k[1:])
    addons = map(lambda x: "1" if x in addons else "0", top_addons)

    cx.write(",".join(map(str, v[0])), ",".join(addons))
