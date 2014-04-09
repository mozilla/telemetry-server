# ChromeHangs export, ported from:
#   https://github.com/mozilla-metrics/telemetry-toolbox

def check(needle, haystack):
    return needle in haystack and not needle + '{"memoryMap":[]' in haystack

def map(k, d, v, cx):
    # We just do "string in" checks for speed. We can check more
    # carefully later, when we parse the json.
    if check('"chromeHangs":', v) or check('"lateWrites":', v):
        cx.write(k, v)
