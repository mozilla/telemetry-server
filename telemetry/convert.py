#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import argparse
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
from infoFieldsMap import envFieldMap, adapterFieldMap, appFieldMap
from datetime import date
import time
try:
    import geoip2.database
    from geoip2.errors import AddressNotFoundError
    geo_available = True
except ImportError:
    geo_available = False


class BadPayloadError(Exception):
    def __init__(self, msg):
        self.msg = msg

class Converter:
    """A class for converting incoming payloads to a more compact form"""
    VERSION_UNCONVERTED = 1
    VERSION_CONVERTED = 2
    VERSION_FXOS_1_3 = 3
    # a raw unified ping (main, saved-session, activation, etc)
    VERSION_UNIFIED = 4
    # A unified ping with a ping.info field added (at a minimum)
    VERSION_UNIFIED_CONVERTED = 5
    GEOIP_COUNTRY_PATH = "/usr/local/var/GeoIP/GeoLite2-Country.mmdb"

    def __init__(self, cache, schema):
        self._histocache = {}
        self._cache = cache
        self._schema = schema
        if geo_available:
            self._geoip = geoip2.database.Reader(Converter.GEOIP_COUNTRY_PATH)
        else:
            self._geoip = None

    def map_key(self, histograms, key):
        return key

    def get_geo_country(self, ip):
        country = None
        err = None
        if ip is not None and self._geoip:
            for candidate in str(ip).split(","):
                candidate = candidate.strip()
                if candidate == "":
                    continue
                try:
                    country = self._geoip.country(candidate).country.iso_code
                except AddressNotFoundError, e:
                    pass
                except Exception, e:
                    err = e

                # Return the first known country.
                if country is not None:
                    return country

        if err is not None:
            raise err

        return country

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

        for k in ("sum", "log_sum", "log_sum_squares", "sum_squares_lo", "sum_squares_hi"):
            rewritten.append(val.get(k, -1))
        return rewritten

    # Memoize building the Histogram object from the histogram definition.
    def histocache(self, revision_url, name, definition):
        key = "%s.%s" % (name, revision_url)
        if key not in self._histocache:
            hist = Histogram(name, definition)
            self._histocache[key] = hist
        return self._histocache[key]

    def rewrite_hists(self, revision_url, histograms):
        histogram_defs = self._cache.get_histograms_for_revision(revision_url)
        if histogram_defs is None:
            raise ValueError("Failed to fetch histograms for URL: %s" % revision_url)
        rewritten = dict()
        for key, val in histograms.iteritems():
            real_histogram_name = key
            if key in histogram_defs:
                real_histogram_name = key
            elif key.startswith("STARTUP_") and key[8:] in histogram_defs:
                # chop off leading "STARTUP_"
                # See http://mxr.mozilla.org/mozilla-central/source/toolkit/components/telemetry/TelemetryPing.jsm
                #     in the `gatherStartupHistograms` function.
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

    def convert_json(self, jsonstr, date, ip=None):
        json_dict = json.loads(jsonstr)
        return self.convert_obj(json_dict, date, ip)

    def add_info_fields(self, info, srcSection, rules, dstSuffix=""):
        for dstField, srcFields in rules.iteritems():
            dstField += dstSuffix
            if dstField in info:
                # Ping already has this field in info!
                # TODO: Make this throw?
                dstField = "environment." + dstField
            # Recurse into the unified ping's structure according to the map
            val = srcSection
            for field in srcFields:
                if field in val:
                   val = val[field]
                else:
                    # this unified ping doesn't report this particular field
                    val = None
                    break
            # Copy the value into ping.info[field name]
            if val != None:
                info[dstField] = val

    def convert_saved_session(self, json_dict):
        # Step 1 of conversion:
        #   ping.payload.* => ping.* (histograms, info, etc)
        payload = json_dict["payload"]
        for field in payload:
            if field == "info" or field == "ver":
                # info field needs to be merged, ping.ver has different meaning
                continue
            elif field in json_dict:
                # TODO: Make this throw?
                json_dict["payload." + str(field)] = payload[field]
                continue
            else:
                json_dict[field] = payload[field]

        # Merge ping.payload.info.* fields to ping.info.*
        payload_info = payload["info"]
        for field in payload_info:
            if field not in json_dict["info"]:
                json_dict["info"][field] = payload_info[field]
        # Back up payload.ver
        json_dict["payload.ver"] = payload["ver"]

        # Get rid of duplicated data in the payload field
        del json_dict["payload"]

        # Step 2 of conversion:
        #   Recreate the old-style ping.info section from fields that are
        #   now in ping.environment
        envFields = json_dict["environment"]
        info = json_dict["info"]
        self.add_info_fields(info, envFields, envFieldMap)

        # WINNT is reported as Windows_NT in the unified ping
        if info.get("OS") == "Windows_NT":
            info["OS"] = "WINNT"

        adapters = None
        try:
            adapters = envFields["system"]["gfx"]["adapters"]
        except KeyError, TypeError:
            pass

        if type(adapters) == list and len(adapters):
            self.add_info_fields(info, adapters[0], adapterFieldMap)
            if len(adapters) > 1:
                # Copy details of the second GPU
                self.add_info_fields(info, adapters[1], adapterFieldMap, "2")
                info["isGPU2Active"] = bool(adapters[1].get("GPUActive"))

        # Fix up clientID
        json_dict["clientID"] = json_dict.get("clientId")

        # Step 3:
        #   Add a "ver" field to make it look like a classic ping to
        #   existing analysis jobs
        json_dict["ver"] = Converter.VERSION_UNIFIED_CONVERTED

    def convert_histograms(self, json_dict):
        info = json_dict.get("info", None)
        if info is None:
            raise ValueError("Missing in payload: info")

        # Convert it and update the version
        if "revision" not in info:
            # We need "revision" to correctly convert histograms. If
            # we don't have histograms in the payload, we don't care
            # about revision (since we don't need to convert anything)
            if "histograms" in json_dict:
                raise ValueError("Missing in payload: info.revision")
        else:
            revision = info.get("revision")
            if "histograms" not in json_dict:
                raise ValueError("Missing in payload: histograms")
            try:
                json_dict["histograms"] = self.rewrite_hists(revision, json_dict["histograms"])
            except DefinitionException, e:
                raise ValueError("Bad Histogram definition for revision {0}: {1}".format(revision, e))
            except KeyError, e:
                raise ValueError("Bad Histogram key for revision {0}: {1}".format(revision, e))

    def convert_obj(self, json_dict, date, ip=None):
        if "ver" in json_dict:
            # This looks like a classic ping (from before Telemetry/FHR unification)
            info = json_dict.get("info", None)
            if (json_dict["ver"] == Converter.VERSION_UNCONVERTED):
                self.convert_histograms(json_dict)
                json_dict["ver"] = Converter.VERSION_CONVERTED
            elif json_dict["ver"] == Converter.VERSION_FXOS_1_3:
                info = {
                    "reason": "ftu",
                    "appUpdateChannel": self.get_dimension(json_dict, "deviceinfo.update_channel"),
                    "appBuildID": self.get_dimension(json_dict, "deviceinfo.platform_build_id"),
                    "appName": "FirefoxOS",
                    "appVersion": self.get_dimension(json_dict, "deviceinfo.platform_version")
                }
                json_dict["info"] = info

                # Remove the pingID field if present.
                if "pingID" in json_dict:
                    del json_dict["pingID"]
                json_dict["ver"] = Converter.VERSION_CONVERTED
            elif (json_dict["ver"] != Converter.VERSION_CONVERTED and
                  json_dict["ver"] != Converter.VERSION_UNIFIED_CONVERTED):
                raise ValueError("Unknown payload version: " + str(json_dict["ver"]))
            # else it's already converted.

        elif "version" in json_dict:
            # This is a unified (FHR+Telemetry) ping
            # https://ci.mozilla.org/job/mozilla-central-docs/Tree_Documentation/toolkit/components/telemetry/telemetry/index.html
            pingType = json_dict.get("type")
            pingVersion = json_dict["version"]

            # Verify some basic assumptions
            if (pingVersion != Converter.VERSION_UNIFIED and
                pingVersion != Converter.VERSION_UNIFIED_CONVERTED):
                raise ValueError("Unknown unified ping version: " + str(json_dict["version"]))
            elif not pingType:
                raise ValueError("Unified ping has no top-level 'type' field")
            elif "application" not in json_dict:
                raise ValueError("Unified ping has no top-level 'application' field")
            elif "payload" not in json_dict:
                raise ValueError("Unified ping has no top-level 'payload' field")

            if pingVersion == Converter.VERSION_UNIFIED:
                # Create a ping.info field and copy select ping.application fields.
                # This is the minimum required for compatibility with the calling
                # process_incoming* scripts
                if "info" in json_dict:
                    raise ValueError("Raw unified ping has an existing ping.info field")

                info = json_dict["info"] = {}
                self.add_info_fields(info, json_dict["application"], appFieldMap)
                info["reason"] = pingType
                json_dict["version"] = Converter.VERSION_UNIFIED_CONVERTED

                if pingType == "saved-session" or pingType == "main":
                    # This is a unified ping in the "main" ping format:
                    # https://ci.mozilla.org/job/mozilla-central-docs/Tree_Documentation/toolkit/components/telemetry/telemetry/main-ping.html
                    json_payload = json_dict["payload"]
                    if "info" not in json_payload:
                        raise ValueError("Unified " + pingType + " missing payload.info")
                    elif "histograms" not in json_payload:
                        raise ValueError("Unified " + pingType + " missing payload.histograms")
                    # Convert the histograms section to the more compact representation
                    self.convert_histograms(json_payload)

                    # Make unified saved-session pings look like classic pings
                    if pingType == "saved-session":
                        if not "environment" in json_dict:
                            raise ValueError("Unified saved-session ping missing 'environment' field")
                        self.convert_saved_session(json_dict)
                else:
                    # Future ping type (activation, upgrade, deletion)
                    # Don't alter it further
                    pass
            else:
                # pingVersion == Converter.VERSION_UNIFIED_CONVERTED:
                # This unified ping was already converted. Do nothing
                info = json_dict["info"]
                pass
        else:
            raise ValueError("Missing payload version")

        if info is None:
            raise ValueError("Missing in payload: info")

        # Look up the country if needed:
        if ip is not None and info.get("appName") == "FirefoxOS":
            country = None
            try:
                country = self.get_geo_country(ip)
            except Exception, e:
                sys.stderr.write("WARN: GeoIP Country lookup failed for " \
                    "IP '{0}': {1}\n".format(ip, e))
            if country is None:
                country = "??"
            json_dict["info"]["geoCountry"] = country
        # Get dimensions in order from schema (field_name)
        dimensions = self._schema.dimensions_from(info, date)
        return json_dict, dimensions

    def get_dimension(self, info, key):
        result = "UNKNOWN"
        if info and key in info:
            result = info.get(key)
        return result

    # Serialize to minimal JSON
    def serialize(self, json_dict):
        return json.dumps(json_dict, separators=(',', ':'))

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
            sys.stdout.write(converter.serialize(json_dict))
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
