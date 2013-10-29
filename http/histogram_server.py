import BaseHTTPServer
import argparse
import sys
import re
import urlparse
from telemetry.revision_cache import RevisionCache
import telemetry.histogram_tools as histogram_tools
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

HIST_PATH = "/histograms"
HIST_BUCKET_PATH = "/histogram_buckets"
REVISION_FIELD = "revision"
HIST_VALID_PREFIX = HIST_PATH + "?" + REVISION_FIELD + "="
HIST_BUCKET_VALID_PREFIX = HIST_BUCKET_PATH + "?" + REVISION_FIELD + "="
MINIMAL_JSON = True

# + 1 to skip the "?"
HIST_QUERY_OFFSET = len(HIST_PATH) + 1
HIST_BUCKET_QUERY_OFFSET = len(HIST_BUCKET_PATH) + 1


class MyHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    def send_HEAD(self, code, message=None):
        self.send_response(code)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        if message is not None:
            self.wfile.write(message)

    def do_HEAD(self):
        self.send_HEAD(200)

    def do_GET(self):
        if self.path.startswith(HIST_VALID_PREFIX):
            self.send_histograms(self.path[HIST_QUERY_OFFSET:], False)
        elif self.path.startswith(HIST_BUCKET_VALID_PREFIX):
            self.send_histograms(self.path[HIST_BUCKET_QUERY_OFFSET:], True)
        else:
            return self.send_HEAD(404, "Not Found")

    def ranges_from_histograms(self, histograms):
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

    def send_histograms(self, query_string, get_buckets):
        params = urlparse.parse_qs(query_string)
        if REVISION_FIELD not in params:
            return self.send_HEAD(400, "Must provide a revision URL")
        revisions = params[REVISION_FIELD]
        if len(revisions) < 1:
            return self.send_HEAD(400, "Must provide a revision URL")
        # Use the first one, ignore any others
        revision = revisions[0]
        # Get revision from cache
        try:
            histograms = self.revision_cache.get_histograms_for_revision(revision, False)
        except Exception, e:
            return self.send_HEAD(500, e.message)

        if histograms is None:
            return self.send_HEAD(404, "Not Found: " + str(revision))

        if get_buckets:
            # Convert to bucket ranges
            ranges = self.ranges_from_histograms(histograms)
            # Write out bucket ranges
            self.send_HEAD(200)
            self.wfile.write(ranges)
        else:
            # Send raw Histograms.json
            self.send_HEAD(200)
            self.wfile.write(histograms)

def main():
    parser = argparse.ArgumentParser(description='Start a caching histogram server', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-p", "--port", help="Server Port", type=int, default=9898)
    parser.add_argument("-c", "--cache-dir", help="Directory to cache Histograms.json revisions", default="./histogram_cache")
    args = parser.parse_args()
    # This is ugly, but seems the easiest way to get this into MyHandler
    MyHandler.revision_cache = RevisionCache(args.cache_dir, "hg.mozilla.org")

    httpd = BaseHTTPServer.HTTPServer(("localhost", args.port), MyHandler)
    try:
        print "Server running on port", args.port
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    httpd.server_close()
    return 0;

if __name__ == '__main__':
    sys.exit(main())
