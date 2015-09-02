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
    return len(o.get(key, {}).get("memoryMap", [])) > 0

def map(k, v, cx):
    try:
        o = v["payload"]
        if check_obj("chromeHangs", o) or check_obj("lateWrites", o):
            # see https://github.com/mozilla/python_moztelemetry/issues/8
            cx.write(k, json.dumps({"chromeHangs": dict(o.get("chromeHangs", {}).items()),
                                    "lateWrites": dict(o.get("lateWrites", {}).items())}))
    except Exception as e:
        print str(e)
