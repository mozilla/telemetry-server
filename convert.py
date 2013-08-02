#!/usr/bin/env python
# encoding: utf-8
"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""

import argparse
import re
import sys
import getopt
try:
    import simplejson as json
except ImportError:
    import json
import urllib2
import revision_cache
from histogram_tools import Histogram, DefinitionException
from telemetry_schema import TelemetrySchema
import traceback
import persist
from datetime import date
import time


class BadPayloadError(Exception):
    def __init__(self, msg):
        self.msg = msg


class Converter:
    """A class for converting incoming payloads to a more compact form"""
    VERSION_UNCONVERTED = 1
    VERSION_CONVERTED = 2

    def __init__(self, cache, schema):
        self._histocache = {}
        self._cache = cache
        self._schema = schema
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
                try:
                    # Materialize the ranges (since we're caching Histograms)
                    allowed_ranges = histogram.allowed_ranges
                except AttributeError:
                    histogram.allowed_ranges = histogram.ranges()
                    allowed_ranges = histogram.allowed_ranges

                try:
                    # Materialize the range_map too.
                    range_map = histogram.range_map
                except AttributeError:
                    range_map = {}
                    for index, allowed_range in enumerate(allowed_ranges):
                        range_map[allowed_range] = index
                    histogram.range_map = range_map

                for bucket in value_map.keys():
                    ib = int(bucket)
                    try:
                        #sys.stderr.write("Writing %s.values[%s] to buckets[%d] (size %d)\n" % (histogram.name(), bucket, range_map[ib], bucket_count))
                        bucket_val = value_map[bucket]
                        # Make sure it's a number:
                        if isinstance(bucket_val, (int, long)):
                            rewritten[range_map[ib]] = bucket_val
                        else:
                            raise BadPayloadError("Found non-integer bucket value: %s.values[%s] = '%s'" % (histogram.name(), str(bucket), str(bucket_val)))
                    except KeyError:
                        raise BadPayloadError("Found invalid bucket %s.values[%s]" % (histogram.name(), str(bucket)))
            except DefinitionException:
                sys.stderr.write("Could not find ranges for histogram: %s: %s\n" % (histogram.name(), sys.exc_info()))
        except ValueError:
            # TODO: what should we do for non-numeric bucket counts?
            #   - output buckets based on observed keys?
            #   - skip this histogram
            pass

        for k in ("sum", "log_sum", "log_sum_squares"):
            rewritten.append(val.get(k, -1))
        return rewritten

    # Memoize building the Histogram object from the histogram definition.
    def histocache(self, revision_url, name, definition):
        key = "%s.%s" % (name, revision_url)
        if key not in self._histocache:
            hist = Histogram(name, definition)
            self._histocache[key] = hist
        return self._histocache[key]

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
        #sys.stderr.write("Getting histograms for %s/%s\n" % (repo, revision))
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
            histogram = self.histocache(revision_url, key, histogram_def)
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

        # Check if the payload is already converted:
        if "ver" in json_dict:
            if json_dict["ver"] == Converter.VERSION_UNCONVERTED:
                # Convert it and update the version
                if "revision" not in info:
                    raise ValueError("Missing in payload: info.revision")
                revision = info.get("revision")
                try:
                    json_dict["histograms"] = self.rewrite_hists(revision, json_dict["histograms"])
                    json_dict["ver"] = Converter.VERSION_CONVERTED
                except KeyError:
                    raise ValueError("Missing in payload: histograms")
            elif json_dict["ver"] != Converter.VERSION_CONVERTED:
                raise ValueError("Unknown payload version: " + str(json_dict["ver"]))
            # else it's already converted.
        else:
            raise ValueError("Missing payload version")

        # Get dimensions in order from schema (field_name)
        dimensions = self._schema.dimensions_from(info, date)
        return json_dict, dimensions

    def get_dimension(self, info, key):
        result = "UNKNOWN"
        if info and key in info:
            result = info.get(key)
        return result

def process(converter, target_date=None):
    line_num = 0
    bytes_read = 0;
    if target_date is None:
        target_date = date.today().strftime("%Y%m%d")

    start = time.clock()
    while True:
        line_num += 1
        line = sys.stdin.readline()
        if line == '':
            break
        bytes_read += len(line)

        if "\t" not in line:
            sys.stderr.write("Error on line %d: no tab found\n" % (line_num))
            continue

        uuid, jsonstr = line.split("\t", 1)
        try:
            json_dict, dimensions = converter.convert_json(jsonstr, target_date)
            sys.stdout.write(uuid)
            sys.stdout.write("\t")
            sys.stdout.write(json.dumps(json_dict, separators=(',', ':')))
            sys.stdout.write("\n")
        except BadPayloadError, e:
            sys.stderr.write("Payload Error on line %d: %s\n%s\n" % (line_num, e.msg, jsonstr))
        except Exception, e:
            sys.stderr.write("Error converting line %d: %s\n" % (line_num, e))

    duration = time.clock() - start
    mb_read = float(bytes_read) / 1024.0 / 1024.0
    if duration > 0:
        sys.stderr.write("Elapsed time: %.02fs (%.02fMB/s)\n" % (duration, mb_read / duration))
    else:
        sys.stderr.write("Elapsed time: %.02fs (??? MB/s)\n" % (duration))

def main(argv=None):
    parser = argparse.ArgumentParser(description="Convert Telemetry data")
    parser.add_argument("-c", "--config-file", help="Read configuration from this file", default="./telemetry_server_config.json")
    parser.add_argument("-d", "--date", help="Use specified date for dimensions")
    args = parser.parse_args()

    try:
        server_config = open(args.config_file, "r")
        config = json.load(server_config)
        server_config.close()
    except IOError:
        config = {}

    cache_dir = config.get("revision_cache_path", "./histogram_cache")
    server = config.get("revision_cache_server", "hg.mozilla.org")
    schema_filename = config.get("schema_filename", "./telemetry_schema.json")
    schema_data = open(schema_filename)
    schema = TelemetrySchema(json.load(schema_data))
    schema_data.close()

    cache = revision_cache.RevisionCache(cache_dir, server)
    converter = Converter(cache, schema)
    process(converter, args.date)

if __name__ == "__main__":
    sys.exit(main())
