import BaseHTTPServer
import re
import urlparse
from revision_cache import RevisionCache
import histogram_tools
import simplejson as json
# For compatibility with python 2.6
try:
    from collections import OrderedDict
except ImportError:
    from simplejson import OrderedDict

# This code is from the Firefox Source:
#   toolkit/components/telemetry/gen-histogram-bucket-ranges.py
# Keep this in sync with TelemetryPing.
startup_histogram_re = re.compile("SQLITE|HTTP|SPDY|CACHE|DNS")

HIST_PATH="/histograms"
REVISION_FIELD="revision"
HIST_VALID_PREFIX=HIST_PATH + "?" + REVISION_FIELD + "="
MINIMAL_JSON=True

# + 1 to skip the "?"
HIST_QUERY_OFFSET=len(HIST_PATH) + 1
revision_cache = RevisionCache("./histogram_cache", "hg.mozilla.org")

def send_HEAD(s, code):
    s.send_response(code)
    s.send_header("Content-type", "text/plain")
    s.end_headers()

def ranges_from_histograms(histograms):
    all_histograms = OrderedDict()
    parsed = json.loads(histograms, object_pairs_hook=OrderedDict)
    for (name, definition) in parsed.iteritems():
        histogram = histogram_tools.Histogram(name, definition)
        parameters = OrderedDict()
        table = {
            'boolean': '2',
            'flag': '3',
            'enumerated': '1',
            'linear': '1',
            'exponential': '0'
            }
        # Use __setitem__ because Python lambdas are so limited.
        histogram_tools.table_dispatch(histogram.kind(), table,
                                       lambda k: parameters.__setitem__('kind', k))
        if histogram.low() == 0:
            parameters['min'] = 1
        else:
            parameters['min'] = histogram.low()

        try:
            buckets = histogram.ranges()
            parameters['buckets'] = buckets
            parameters['max'] = buckets[-1]
            parameters['bucket_count'] = len(buckets)
        except histogram_tools.DefinitionException:
            continue

        all_histograms.update({ name: parameters });

        if startup_histogram_re.search(name) is not None:
            all_histograms.update({ "STARTUP_" + name: parameters })
    if MINIMAL_JSON:
        result = json.dumps({'histograms': all_histograms}, separators=(',', ':'))
    else:
        result = json.dumps({'histograms': all_histograms})
    return result


class MyHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    def do_HEAD(s):
        send_HEAD(s, 200)

    def do_GET(s):
        if not s.path.startswith(HIST_VALID_PREFIX):
            send_HEAD(s, 404)
            s.wfile.write("Not Found")
            return
        
        params = urlparse.parse_qs(s.path[HIST_QUERY_OFFSET:])
        revisions = params[REVISION_FIELD]
        if len(revisions) != 1:
            send_HEAD(s, 400)
            s.wfile.write("Must provide a revision URL")
            return
        revision = revisions[0]
        # TODO: get revision from cache
        try:
            histograms = revision_cache.get_histograms_for_revision(revision, False)
        except Exception, e:
            send_HEAD(s, 500)
            s.wfile.write(e.message)
            return

        if histograms is None:
            send_HEAD(s, 404)
            s.wfile.write("Not Found: " + str(revision))
            return

        # Convert to bucket ranges
        ranges = ranges_from_histograms(histograms)
        # Write out bucket ranges
        send_HEAD(s, 200)
        s.wfile.write(ranges)

if __name__ == '__main__':
    httpd = BaseHTTPServer.HTTPServer(("localhost", 9898), MyHandler)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    httpd.server_close()
