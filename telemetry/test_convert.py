# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import os
import revision_cache
import shutil
import simplejson as json
import unittest
from telemetry_schema import TelemetrySchema
from convert import Converter, BadPayloadError
import telemetry.util.files as fu

# python -m unittest telemetry.test_convert
#   - or -
# coverage run -m telemetry.test_convert; coverage html

class ConvertTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cache_dir = "/tmp/histogram_revision_cache"
        cls.schema_filename = "./telemetry/telemetry_schema.json"
        assert not os.path.exists(cls.cache_dir)

        schema_file = open(cls.schema_filename, "r")
        cls.schema = TelemetrySchema(json.load(schema_file))
        schema_file.close()
        cls.cache = revision_cache.RevisionCache(cls.cache_dir, 'hg.mozilla.org')
        cls.converter = Converter(cls.cache, cls.schema)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.cache_dir)

    def get_revision(self):
        return "http://hg.mozilla.org/mozilla-central/rev/26cb30a532a1"

    def get_payload(self, desc):
        if desc == "anr":
            return {
                "androidLogcat": "...logcat...",
                "androidANR": "...snip...",
                "info": {
                    "hardware": "tuna",
                    "appUpdateChannel": "default",
                    "appBuildID": "20130225174321",
                    "appName": "Fennec",
                    "appVersion": "22.0a1",
                    "appID": "{aa3c5121-dab2-40e2-81ca-7ea25febc110}",
                    "version": "17",
                    "OS": "Android",
                    "reason": "android-anr-report",
                    "platformBuildID": "20130225174321",
                    "locale": "en-US",
                    "cpucount": 2,
                    "memsize": 694,
                    "arch": "armv7l",
                    "kernel_version": "3.0.31-gd5a18e0",
                    "device": "Galaxy Nexus",
                    "manufacturer": "samsung"
                },
                "simpleMeasurements": {
                    "uptime": 0
                },
                "ver": 1
            }
        if desc == "fxos":
            return {
                "ver": 3,
                "activationTime": 1395769944966,
                "devicePixelRatio": 1,
                "deviceinfo.firmware_revision": "",
                "deviceinfo.hardware": "qcom",
                "deviceinfo.os": "1.5.0.0-prerelease",
                "deviceinfo.platform_build_id": "20140325104133",
                "deviceinfo.platform_version": "31.0a1",
                "deviceinfo.product_model": "ALCATEL ONE TOUCH FIRE",
                "deviceinfo.software": "Boot2Gecko 1.5.0.0-prerelease",
                "deviceinfo.update_channel": "default",
                "icc": {
                    "mcc": "310",
                    "mnc": "410",
                    "spn": None
                },
                "locale": "en-US",
                "network": {
                    "mcc": "310",
                    "mnc": "410",
                    "operator": "AT&T"
                },
                "pingID": "e426da9f-2a29-4e09-895b-c883903956cb",
                "pingTime": 1395852542588,
                "screenHeight": 480,
                "screenWidth": 320
            }
        if desc == "normal":
            return {
                "info": {
                    "flashVersion": "11,2,202,327",
                    "addons": "tabcount%403greeneggs.com:1.1",
                    "adapterDriverVersion": "3.0 Mesa 9.2.1",
                    "adapterDeviceID": "Mesa DRI Intel(R) Sandybridge Mobile ",
                    "adapterVendorID": "Intel Open Source Technology Center",
                    "adapterDescription": "Intel Open Source Technology Center -- Mesa DRI Intel(R) Sandybridge Mobile ",
                    "hasNEON": False,  "hasARMv7": False, "hasARMv6": False, "hasEDSP": False,
                    "hasSSE4_2": True, "hasSSE4_1": True, "hasSSE4A": False, "hasSSSE3": True,
                    "hasSSE3": True,   "hasSSE2": True,   "hasMMX": True,    "hasSSE": True,
                    "platformBuildID": "20131112030204",
                    "appUpdateChannel": "nightly",
                    "appBuildID": "20131112030204",
                    "appName": "Firefox",
                    "appVersion": "28.0a1",
                    "appID": "{ec8030f7-c20a-464f-9b0e-13a3a9e97384}",
                    "OS": "Linux",
                    "reason": "saved-session",
                    "revision": self.get_revision(),
                    "locale": "en-US",
                    "cpucount": 4,
                    "memsize": 7872,
                    "arch": "x86-64",
                    "version": "3.11.0-13-generic"
                },
                "ver": 1,
                "simpleMeasurements": {
                    "savedPings": 2,
                    "maximalNumberOfConcurrentThreads": 40,
                    "shutdownDuration": 525,
                    "js": {
                        "customIter": 20,
                        "setProto": 0
                    },
                    "debuggerAttached": 0,
                    "startupInterrupted": 0,
                    "sessionRestoreRestoring": 3077,
                    "delayedStartupFinished": 2985,
                    "delayedStartupStarted": 2860,
                    "sessionRestoreInitialized": 790,
                    "XPI_finalUIStartup": 742,
                    "AMI_startup_end": 670,
                    "XPI_startup_end": 670,
                    "startupCrashDetectionEnd": 32930,
                    "startupCrashDetectionBegin": 475,
                    "afterProfileLocked": 369,
                    "selectProfile": 368,
                    "main": 301,
                    "start": 6,
                    "addonManager": {
                        "XPIDB_parseDB_MS": 2,
                        "XPIDB_decode_MS": 0,
                        "XPIDB_asyncRead_MS": 1,
                        "XPIDB_async_load": "BeforeFinalUIStartup"
                    },
                    "uptime": 1834,
                    "firstPaint": 2860,
                    "sessionRestored": 3118,
                    "createTopLevelWindow": 795,
                    "firstLoadURI": 3088,
                    "AMI_startup_begin": 480,
                    "XPI_startup_begin": 485,
                    "XPI_bootstrap_addons_begin": 594,
                    "XPI_bootstrap_addons_end": 670
                },
                "slowSQL": {
                    "otherThreads": {
                        "UPDATE ...": [ 1, 313 ],
                        "SELECT ...": [ 8, 2584 ],
                        "COMMIT TRANSACTION": [ 1, 187 ]
                    },
                    "mainThread": {}
                },
                "chromeHangs": { "durations": [], "stacks": [], "memoryMap": [] },
                "lateWrites": { "stacks": [], "memoryMap": [] },
                "addonHistograms": {},
                "addonDetails": {
                    "XPI": {
                        "tabcount@3greeneggs.com": {
                            "location": "app-profile",
                            "unpacked": 0
                        }
                    }
                },
                "histograms": self.get_raw_histograms()
            }

    def get_raw_histograms(self):
        return {
            "STARTUP_CRASH_DETECTED": {
                "sum_squares_hi": 0, # what about log_sum and log_sum_squares?
                "sum_squares_lo": 0,
                "sum": 0,
                "values": {
                    "1": 0,
                    "0": 1
                },
                "histogram_type": 3,
                "bucket_count": 3,
                "range": [
                    1,
                    2
                ]
            },
            "DOM_TIMERS_FIRED_PER_NATIVE_TIMEOUT": {
                "log_sum_squares": 624.027626294358,
                "log_sum": 873.474196434021,
                "sum": 1279,
                "values": {
                    "1114": 0,
                    "414": 1,
                    "8": 2,
                    "3": 7,
                    "1": 1232,
                    "0": 0
                },
                "histogram_type": 0,
                "bucket_count": 10,
                "range": [
                    1,
                    3000
                ]
            },
            # This one uses the "DNS_LOOKUP_TIME" definition
            "STARTUP_DNS_LOOKUP_TIME": {
                "log_sum_squares": 18.980913162231445,
                "log_sum": 4.356709003448486,
                "sum": 77,
                "values": {
                    "95": 0,
                    "77": 1,
                    "62": 0
                },
                "histogram_type": 0,
                "bucket_count": 50,
                "range": [
                    1,
                    60000
                ]
            }
        }

    def get_converted_histograms(self):
        return {
            "STARTUP_CRASH_DETECTED": [1, 0, 0, 0, -1, -1, 0, 0],
            "DOM_TIMERS_FIRED_PER_NATIVE_TIMEOUT": [0,1232,7,2,0,0,0,1,0,0,1279,873.474196434021,624.027626294358,-1,-1],
            "STARTUP_DNS_LOOKUP_TIME": [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,77,4.356709003448486,18.980913162231445,-1,-1]
        }

    def test_histograms(self):
        histograms = self.get_raw_histograms()
        expected_converted_histograms = self.get_converted_histograms()
        revision = self.get_revision()
        rewritten = ConvertTest.converter.rewrite_hists(revision, histograms)
        for h in expected_converted_histograms.keys():
            self.assertEqual(rewritten[h], expected_converted_histograms[h])

    def convert(self, raw, submission_date="20131114", ip=None):
        return ConvertTest.converter.convert_json(json.dumps(raw), submission_date, ip)

    def test_anr(self):
        raw = self.get_payload("anr")
        self.assertEqual(raw["ver"], Converter.VERSION_UNCONVERTED)
        # use "convert_json" so we don't modify the object being passed in.
        converted, dimensions = self.convert(raw)
        self.assertEqual(dimensions[0], "android-anr-report")
        self.assertEqual(raw["ver"], Converter.VERSION_UNCONVERTED)
        self.assertEqual(converted["ver"], Converter.VERSION_CONVERTED)
        self.assertIs(converted["info"].get("geoCountry"), None)

    def test_fxos(self):
        raw = self.get_payload("fxos")
        converted, dimensions = self.convert(raw)
        self.assertEqual(dimensions[0], "ftu")
        self.assertEqual(raw["ver"], Converter.VERSION_FXOS_1_3)
        self.assertEqual(converted["ver"], Converter.VERSION_CONVERTED)
        self.assertEqual(converted["info"]["reason"], "ftu")
        self.assertEqual(converted["info"]["appName"], "FirefoxOS")
        self.assertEqual(converted["info"]["appVersion"], raw["deviceinfo.platform_version"])
        # Make sure we removed the pingID:
        self.assertEqual(raw["pingID"], "e426da9f-2a29-4e09-895b-c883903956cb")
        self.assertIs(converted.get("pingID"), None)

    def test_normal(self):
        raw = self.get_payload("normal")
        self.assertEqual(raw["ver"], Converter.VERSION_UNCONVERTED)
        converted, dimensions = self.convert(raw)
        self.assertEqual(dimensions[0], "saved-session")
        self.assertEqual(raw["ver"], Converter.VERSION_UNCONVERTED)
        self.assertEqual(converted["ver"], Converter.VERSION_CONVERTED)

        expected_converted_histograms = self.get_converted_histograms()
        for h in expected_converted_histograms.keys():
            self.assertEqual(converted["histograms"][h], expected_converted_histograms[h])
        self.assertIs(converted["info"].get("geoCountry"), None)

    def print_byte_range(self, data, start=None, end=None):
        if start is None:
            start = 0
        if end is None:
            end = len(data)
        for i in range(start, end):
            pieces = {
                "i": i,
                "o": ord(data[i]),
                "c": data[i]
            }
            print "0x%(i)04x %(i)04d = 0x%(o)02x %(o)3d %(c)s" % pieces
        print "----"

    def test_utf8(self):
        count = 0
        # This packed file contains the same record twice, once gzipped, once
        # raw. The record contains the UTF-8 encoded string "Wikipédia"
        for r in fu.unpack('test/unicode.v1.packed', file_version="v1"):
            self.assertIs(r.error, None)
            self.assertTrue(len(r.data) > 0)
            self.assertEqual(r.data[0], '{')
            count += 1

            # self.print_byte_range(r.data, 3323, 3362)

            # Make sure that the raw bytes are correct:
            # Incoming is UTF-8, so we expect
            #   W i k i p 0xc3 0xa9 d i a
            self.assertEqual(ord(r.data[0xd16]), 0x70)
            self.assertEqual(ord(r.data[0xd17]), 0xc3)
            self.assertEqual(ord(r.data[0xd18]), 0xa9)
            self.assertEqual(ord(r.data[0xd19]), 0x64)

            # Convert the data the wrong way:
            bad = unicode(r.data, errors="replace")
            # self.print_byte_range(bad, 3323, 3362)

            # Verify that we see the replacement chars in the expected places:
            #   W i k i p 0xfffd 0xfffd d i a
            self.assertEqual(ord(bad[0xd16]), 0x70)
            self.assertEqual(ord(bad[0xd17]), 0xfffd)
            self.assertEqual(ord(bad[0xd18]), 0xfffd)
            self.assertEqual(ord(bad[0xd19]), 0x64)

            # Now convert properly:
            good = fu.to_unicode(r.data)
            # self.print_byte_range(good, 3322, 3360)

            # Now we have unicode, so we expect
            #   W i k i p 0xe9 d i a
            self.assertEqual(ord(good[0xd15]), 0x70)
            self.assertEqual(ord(good[0xd16]), 0xe9)
            self.assertEqual(ord(good[0xd17]), 0x64)

            converted, dimensions = ConvertTest.converter.convert_json(good, "20131114", None)

            engine = converted["simpleMeasurements"]["UITelemetry"]["toolbars"]["currentSearchEngine"]
            # print engine

            self.assertEqual(ord(engine[4]), 0x70)
            self.assertEqual(ord(engine[5]), 0xe9)
            self.assertEqual(ord(engine[6]), 0x64)

            serialized = ConvertTest.converter.serialize(converted, sort_keys=True)

            # self.print_byte_range(serialized, 4007, 4049)
            # Now we have escaped unicode, so we expect
            #   W i k i p \ u 0 0 e 9 d i a
            self.assertEqual(ord(serialized[0xfc2]), 0x70)
            self.assertEqual(ord(serialized[0xfc3]), 0x5c)
            self.assertEqual(ord(serialized[0xfc4]), 0x75)
            self.assertEqual(ord(serialized[0xfc5]), 0x30)
            self.assertEqual(ord(serialized[0xfc6]), 0x30)
            self.assertEqual(ord(serialized[0xfc7]), 0x65)
            self.assertEqual(ord(serialized[0xfc8]), 0x39)
            self.assertEqual(ord(serialized[0xfc9]), 0x64)

        self.assertEqual(count, 2)

    def test_plain_geo(self):
        self.assertEqual(ConvertTest.converter.get_geo_country("8.8.8.8"), "US")
        self.assertEqual(ConvertTest.converter.get_geo_country("2001:4860:4860::8888"), "US")
        self.assertIs(ConvertTest.converter.get_geo_country("127.0.0.1"), None)
        self.assertIs(ConvertTest.converter.get_geo_country("::1"), None)

    def test_convert_geo(self):
        # Google public DNS
        google_ipv4 = "8.8.8.8"
        google_ipv6 = "2001:4860:4860::8888"
        local_ipv4 = "127.0.0.1"
        local_ipv6 = "::1"

        # First, test a normal Firefox payload
        raw = self.get_payload("normal")
        for ip in [google_ipv4, google_ipv6, local_ipv4, local_ipv6]:
            converted, dimensions = self.convert(raw, ip=ip)
            self.assertEqual(converted["ver"], Converter.VERSION_CONVERTED)
            # Firefox payloads should not be geocoded
            self.assertNotIn("geoCountry", converted["info"])

        # Now, test a FirefoxOS payload
        raw = self.get_payload("fxos")
        for ip in [google_ipv4, google_ipv6]:
            converted, dimensions = self.convert(raw, ip=ip)
            self.assertEqual(converted["ver"], Converter.VERSION_CONVERTED)
            self.assertEqual(converted["info"]["appName"], "FirefoxOS")
            self.assertEqual(converted["info"].get("geoCountry"), "US")

        for ip in [local_ipv4, local_ipv6]:
            converted, dimensions = self.convert(raw, ip=ip)
            self.assertEqual(converted["ver"], Converter.VERSION_CONVERTED)
            self.assertEqual(converted["info"]["appName"], "FirefoxOS")
            self.assertEqual(converted["info"].get("geoCountry"), "??")

    def test_convert_bad_geo(self):
        raw = self.get_payload("fxos")
        for ip in ["0.0.0.0", "bogus", "", 100]:
            converted, dimensions = self.convert(raw, ip=ip)
            self.assertEqual(converted["ver"], Converter.VERSION_CONVERTED)
            self.assertEqual(converted["info"].get("geoCountry"), "??")

        # if ip is None, we skip the lookup altogether.
        converted, dimensions = self.convert(raw, ip=None)
        self.assertEqual(converted["ver"], Converter.VERSION_CONVERTED)
        # Firefox payloads should not be geocoded
        self.assertNotIn("geoCountry", converted["info"])

    def test_csv_geo(self):
        test_invalid = {
            "bla": None,
            "123": None,
            "500.500.500.500": None,
        }

        test_valid = {
            ",,,,": None,
            "": None,
            "8.8.8.8": "US",
            "142.68.238.79": "CA",
            "2001:4860:4860::8888": "US",
            "127.0.0.1": None,
            "127.0.0.1, 8.8.8.8": "US",
            "127.0.0.1, 10.0.0.1": None,
            "127.0.0.1, 8.8.8.8": "US",
            "127.0.0.1, 142.68.238.79": "CA",
            "8.8.8.8, 142.68.238.79": "US",
            "142.68.238.79, 8.8.8.8": "CA",
            "142.68.238.79 ,,127.0.0.1,, 8.8.8.8": "CA",
        }

        self.assertIs(ConvertTest.converter.get_geo_country(None), None)

        for ip, expected_country in test_invalid.iteritems():
            with self.assertRaises(ValueError):
                ConvertTest.converter.get_geo_country(ip)

        for ip, expected_country in test_valid.iteritems():
            actual_country = ConvertTest.converter.get_geo_country(ip)
            if expected_country is None:
                self.assertIs(actual_country, None)
            else:
                self.assertEqual(actual_country, expected_country)

    def test_bad_payload_bogus_bucket_value(self):
        raw = self.get_payload("normal")
        raw["histograms"]["STARTUP_CRASH_DETECTED"]["values"][0] = "two"
        with self.assertRaises(BadPayloadError):
            converted, dimensions = self.convert(raw)
        try:
            converted, dimensions = self.convert(raw)
        except BadPayloadError as e:
            self.assertTrue(e.msg.startswith("Found non-integer bucket value: "))

    def check_conversion_error(self, payload, message, prefix=False):
        with self.assertRaises(ValueError):
            converted, dimensions = self.convert(payload)
        try:
            converted, dimensions = self.convert(payload)
        except ValueError as e:
            if prefix:
                self.assertTrue(e.message.startswith(message))
            else:
                self.assertEqual(e.message, message)

    def test_bad_payload_missing_info(self):
        raw = self.get_payload("normal")
        del raw["info"]
        self.check_conversion_error(raw, "Missing in payload: info")

    def test_bad_payload_missing_revision(self):
        raw = self.get_payload("normal")
        del raw["info"]["revision"]
        self.check_conversion_error(raw, "Missing in payload: info.revision")

    def test_bad_payload_bogus_bucket(self):
        raw = self.get_payload("normal")
        raw["histograms"]["STARTUP_CRASH_DETECTED"]["values"][999] = 1
        with self.assertRaises(BadPayloadError):
            converted, dimensions = self.convert(raw)
        try:
            converted, dimensions = self.convert(raw)
        except BadPayloadError as e:
            self.assertTrue(e.msg.startswith("Found invalid bucket "))

    def test_serialize(self):
        t = {"foo": 1, "bar": 2}
        serialized = ConvertTest.converter.serialize(t)
        self.assertEqual(serialized, '{"foo":1,"bar":2}')

    def test_bad_revision_url(self):
        bad_revision = self.get_revision().replace("532a1", "00000")
        histograms = self.get_raw_histograms()
        with self.assertRaises(ValueError):
            ConvertTest.converter.rewrite_hists(bad_revision, histograms)
        try:
            ConvertTest.converter.rewrite_hists(bad_revision, histograms)
        except ValueError as e:
            self.assertTrue(e.message.startswith("Failed to fetch histograms for URL: "))

    def test_unknown_payload_version(self):
        largest_known_version = max(Converter.VERSION_UNCONVERTED,
            Converter.VERSION_CONVERTED, Converter.VERSION_FXOS_1_3)
        raw = self.get_payload("normal")
        raw["ver"] = largest_known_version + 1
        self.check_conversion_error(raw, "Unknown payload version: ", prefix=True)

    def test_missing_payload_version(self):
        raw = self.get_payload("normal")
        del raw["ver"]
        self.check_conversion_error(raw, "Missing payload version")

    def test_invalid_histogram_name(self):
        histograms = self.get_raw_histograms()
        bogus_name = "I_DO_NOT_EXIST"
        histograms[bogus_name] = histograms["STARTUP_DNS_LOOKUP_TIME"]
        revision = self.get_revision()
        rewritten = ConvertTest.converter.rewrite_hists(revision, histograms)
        # The bogus histogram should be skipped.
        self.assertNotIn(bogus_name, rewritten)

        # But everything else should have been translated properly.
        expected_converted_histograms = self.get_converted_histograms()
        for h in expected_converted_histograms.keys():
            self.assertEqual(rewritten[h], expected_converted_histograms[h])

    def test_map_key(self):
        for k in ["hello", 5, {"foo": "bar"}]:
            self.assertEqual(k, ConvertTest.converter.map_key(None, k))

    def test_unicode(self):
        with open('test/unicode_payload.json') as f:
            payload = f.read()
        upayload = fu.to_unicode(payload)
        converted, dimensions = ConvertTest.converter.convert_json(upayload,'20140101')
        parsed = json.loads(payload)
        self.assertEqual(parsed["data"], converted["data"])


if __name__ == "__main__":
    unittest.main()
