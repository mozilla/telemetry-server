#!/usr/bin/env python
# encoding: utf-8
"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""

import re
import sys
import getopt
try:
    import simplejson as json
except ImportError:
    import json
import urllib2
import gzip
import revision_cache
from histogram_tools import Histogram
import traceback
import persist


class Converter:
    """A class for converting incoming payloads to a more compact form"""

    def __init__(self, cache):
        self._cache = cache
        self._valid_revisions = re.compile('^(http://.*)/([^/]+)/rev/([0-9a-f]+)/?$')

    def map_key(self, histograms, key):
        return key

    def map_value(self, histogram, val):
        rewritten = []
        try:
            bucket_count = int(histogram.n_buckets())
            #sys.stderr.write("Found %d buckets for %s\n" % (bucket_count, histogram.name()))
            rewritten = [0] * bucket_count
            value_map = val["values"]
            try:
                # TODO: this is horribly inefficient
                allowed_ranges = histogram.ranges()
                range_map = dict()
                for index, allowed_range in enumerate(allowed_ranges):
                    range_map[allowed_range] = index

                for bucket in value_map.keys():
                    ib = int(bucket)
                    try:
                        #sys.stderr.write("Writing %s.values[%s] to buckets[%d] (size %d)\n" % (histogram.name(), bucket, range_map[ib], bucket_count))
                        rewritten[range_map[ib]] = value_map[bucket]
                    except KeyError:
                        sys.stderr.write("Found bogus bucket %s.values[%s]\n" % (histogram.name(), str(bucket)))
            except:
                sys.stderr.write("Could not find ranges for histogram: %s: %s\n" % (histogram.name(), sys.exc_info()))
                traceback.print_exc(file=sys.stderr)
        except ValueError:
            # TODO: what should we do for non-numeric bucket counts?
            #   - output buckets based on observed keys?
            #   - skip this histogram
            pass

        for k in ("sum", "log_sum", "log_sum_squares"):
            rewritten.append(val.get(k, -1))
        return rewritten

    # Returns (repository name, revision)
    def revision_url_to_parts(self, revision_url):
        m = self._valid_revisions.match(revision_url)
        if m:
            #sys.stderr.write("Matched\n")
            return (m.group(2), m.group(3))
        else:
            #sys.stderr.write("Did not Match: %s\n" % revision_url)
            raise ValueError("Invalid revision URL: %s" % revision_url)
        #return (None, None)

    def get_histograms_for_revision(self, revision_url):
        # revision_url is like
        #    http://hg.mozilla.org/releases/mozilla-aurora/rev/089956e907ed
        # and path should be like
        #    toolkit/components/telemetry/Histograms.json
        # to produce a full URL like
        #    http://hg.mozilla.org/releases/mozilla-aurora/raw-file/089956e907ed/toolkit/components/telemetry/Histograms.json
        repo, revision = self.revision_url_to_parts(revision_url)
        sys.stderr.write("Getting histograms for %s/%s\n" % (repo, revision))
        histograms = self._cache.get_revision(repo, revision)
        return histograms

    def rewrite_hists(self, revision_url, histograms):
        histogram_defs = self.get_histograms_for_revision(revision_url)
        rewritten = dict()
        for key, val in histograms.iteritems():
            real_histogram_name = key
            if key in histogram_defs:
                real_histogram_name = key
            elif key.startswith("STARTUP_") and key[8:] in histogram_defs:
                # chop off leading "STARTUP_" per http://mxr.mozilla.org/mozilla-central/source/toolkit/components/telemetry/TelemetryPing.js#532
                real_histogram_name = key[8:]
            else:
                # TODO: collect these to be returned
                sys.stderr.write("ERROR: no histogram definition found for %s\n" % key)
                continue

            histogram_def = histogram_defs[real_histogram_name]
            histogram = Histogram(key, histogram_def)
            new_key = self.map_key(histogram_defs, key)
            new_value = self.map_value(histogram, val)
            rewritten[new_key] = new_value
        return rewritten

    def convert_json(self, jsonstr, date):
        json_dict = json.loads(jsonstr)
        if "info" not in json_dict:
            raise ValueError("Missing in payload: info")
        if "histograms" not in json_dict:
            raise ValueError("Missing in payload: histograms")

        info = json_dict.get("info")
        if "revision" not in info:
            raise ValueError("Missing in payload: info.revision")

        revision = info.get("revision")

        try:
            json_dict["histograms"] = self.rewrite_hists(revision, json_dict["histograms"])
        except KeyError:
            raise ValueError("Missing in payload: histograms")

        reason = self.get_dimension(info, "reason")
        appname = self.get_dimension(info, "appName")
        channel = self.get_dimension(info, "appUpdateChannel")
        appver = self.get_dimension(info, "appVersion")
        buildid = self.get_dimension(info, "appBuildID")
        # TODO: get dimensions in order from schema (field_name)
        dimensions = [date, reason, appname, channel, appver, buildid]
        return json_dict, dimensions

    def get_dimension(self, info, key):
        result = "UNKNOWN"
        if info and key in info:
            result = info.get(key)
        return result

help_message = '''
    Takes a list of raw telemetry pings and writes them back out in a more
    compact form
    Required:
        -i, --input <input_file>
        -o, --output <output_file>
    Optional:
        -h, --help
'''

def process(input_file, output_file, storage, converter):
    line = 0
    if input_file == '-':
        fin = sys.stdin
    else:
        fin = open(input_file, "rb")

    if output_file == '-':
       fout = sys.stdout
    else:
       fout = gzip.open(output_file, "wb")


    while True:
        line += 1
        first_byte = fin.read(1)
        if len(first_byte) == 0:
            break;
        assert len(first_byte) == 1

        date = fin.read(8)
        assert len(date) == 8

        uuid = fin.read(36)
        assert len(uuid) == 36

        first_uuid_byte = int(uuid[0:2], 16) - 128
        if first_uuid_byte < 0:
            first_uuid_byte += 256
        assert first_uuid_byte == ord(first_byte)

        tab = fin.read(1)
        assert tab == '\t'

        jsonstr = fin.readline()
        json_dict, dimensions = converter.convert_json(jsonstr, date)
        storage.write(uuid, json_dict, dimensions)
        fout.write(uuid)
        fout.write("\t")
        fout.write(json.dumps(json_dict, separators=(',', ':')))
        fout.write("\n")

    fin.close()
    fout.close()

class Usage(Exception):
    def __init__(self, msg):
        self.msg = msg

def main(argv=None):
    if argv is None:
        argv = sys.argv
    try:
        try:
            opts, args = getopt.getopt(argv[1:], "hi:o:v", ["help", "input=", "output="])
        except getopt.error, msg:
            raise Usage(msg)
        
        input_file = None
        output_file = None
        cache_dir = None
        server = None
        # option processing
        for option, value in opts:
            if option == "-v":
                verbose = True
            elif option in ("-h", "--help"):
                raise Usage(help_message)
            elif option in ("-i", "--input"):
                input_file = value
            elif option in ("-o", "--output"):
                output_file = value
            elif option in ("-c", "--cache"):
                cache_dir = value
            elif option in ("-s", "--server"):
                server = value
        if cache_dir is None:
            cache_dir = "./histogram_cache"

        if server is None:
            server = "hg.mozilla.org"
        cache = revision_cache.RevisionCache(cache_dir, server)
        schema_data = open("./telemetry_schema.json")
        schema = json.load(schema_data)
        storage = persist.StorageLayout(schema, "./data")
        converter = Converter(cache)
        process(input_file, output_file, storage, converter)

    except Usage, err:
        print >> sys.stderr, sys.argv[0].split("/")[-1] + ": " + str(err.msg)
        print >> sys.stderr, " for help use --help"
        return 2

if __name__ == "__main__":
    sys.exit(main())
