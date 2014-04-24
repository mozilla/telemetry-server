# ChromeHangs export, ported from:
#   https://github.com/mozilla-metrics/telemetry-toolbox

try:
    import simplejson as json
except ImportError:
    import json

def check(needle, haystack):
    return needle in haystack and not needle + '{"memoryMap":[]' in haystack

def check_obj(key, o):
    if key in o:
        mm = o[key].get("memoryMap", [])
        if len(mm) > 0:
            return True
    return False

def map(k, d, v, cx):
    # We just do "string in" checks for speed. We can check more
    # carefully later, when we parse the json.
    if check('"chromeHangs":', v) or check('"lateWrites":', v):
        try:
            o = json.loads(v)
            if check_obj("chromeHangs", o) or check_obj("lateWrites", o):
                for f in ["fileIOReports", "histograms", "slowSQL", "threadHangStats"]:
                    if f in o:
                        del o[f]
                cx.write(k, json.dumps(o))
        except:
            print "Error parsing json"
