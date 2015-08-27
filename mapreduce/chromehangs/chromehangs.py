
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# ChromeHangs export, ported from:
#   https://github.com/mozilla-metrics/telemetry-toolbox

try:
    import simplejson as json
except ImportError:
    import json

def check_obj(key, o):
    if key in o:
        mm = o[key].get("memoryMap", [])
        if len(mm) > 0:
            return True
    return False

def map(k, v, cx):
    try:
        o = v["payload"]
        if check_obj("chromeHangs", o) or check_obj("lateWrites", o):
            for f in ["fileIOReports", "histograms", "slowSQL", "threadHangStats"]:
                if f in o:
                    del o[f]
            cx.write(k, json.dumps(o))
    except:
        print str(e)
