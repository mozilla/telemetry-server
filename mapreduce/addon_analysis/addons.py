import simplejson as json
import numpy
import io
import csv
import os
from string import maketrans

import numpy as np
import scipy as sp
import scipy.stats

addons = set()

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

    # Let's remove machines with older configurations
    if not version.startswith("6") or os != "WINNT" or cpucount < 2 or memsize < 2:
        return

    # Build a list of add-ons
    addons = parsed['addonDetails'].get('XPI', {})
    addon_names = set()

    for addon, desc in addons.iteritems():
        if "name" in desc:
            name = clean(desc["name"])

            if name is not None:
                addon_names.add(name.replace(",", "-"))

    for addon in addon_names:
        cx.write(addon, 1)

def setup_reduce(cx):
    cx.field_separator = ","

def reduce(k, v, cx):
    cx.write(k, sum(v))
