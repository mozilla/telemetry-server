# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import unittest
from telemetry_schema import TelemetrySchema

class TelemetrySchemaTest(unittest.TestCase):
    def setUp(self):
        self.schema = TelemetrySchema(self.get_schema_spec())
        self.allowed_values = self.schema.sanitize_allowed_values()

    def get_schema_spec(self):
        return {
            "version": 1,
            "dimensions": [
                {
                    "field_name": "reason",
                    "allowed_values": ["saved-session"]
                },
                {
                    "field_name": "appName",
                    "allowed_values": "*"
                },
                {
                    "field_name": "appUpdateChannel",
                    "allowed_values": ["nightly"]
                },
                {
                    "field_name": "appVersion",
                    "allowed_values": "*"
                },
                {
                    "field_name": "appBuildID",
                    "allowed_values": "*"
                },
                {
                    "field_name": "submission_date",
                    "allowed_values": ["20130908"]
                }
            ]
        }
    def get_file_list(self):
        return [
            "/foo/bar/baz/bla.txt",
            "processed/idle_daily/Firefox/nightly/26.0a1/20130826074752.20130903.log.e0c7ff434e474c8aa745763eed408b9c.lzma",
            "garbage/idle_daily/Firefox/nightly/26.0a1/20130826074752.20130903.log.e0c7ff434e474c8aa745763eed408b9c.lzma",
            "processed/idle_daily/Firefox/nightly/26.0a1/20130901030218.20130903.log.17ff07fda8994e23baf983550246a94b.lzma",
            "processed/idle_daily/Firefox/nightly/26.0a1/20130819030205.20130907.log.ec6f22acd37349b3b5ef03da1cc150da.lzma",
            "processed/idle_daily/Firefox/nightly/26.0a1/20130807161117.20130903.log.64478ac84a734677bc14cbcf6cc114b7.lzma",
            "processed/idle_daily/Firefox/nightly/26.0a1/20130814030204.20130906.log.a382f1337d1f47ef8aad08f8fb14a79a.lzma",
            "processed/idle_daily/Firefox/nightly/26.0a1/20130830030205.20130903.log.c86e2c3f31c043ac8fc311d5dd1abc28.lzma",
            "processed/idle_daily/Firefox/nightly/26.0a1/20130823151250.20130907.log.939bec39c3d24c89a09834463b220d9a.lzma",
            "processed/idle_daily/Firefox/nightly/26.0a1/20130830030205.20130906.log.0bf2c1edf2634ca5bdc865a54957a690.lzma",
            "processed/idle_daily/Firefox/nightly/26.0a1/20130826074752.20130903.log.8e33cc0f130849dfbb8afe7331123be3.lzma",
            "processed/idle_daily/Firefox/nightly/26.0a1/20130826074752.20130902.log.2349f0434be64c6684f91eccabf9b3e6.lzma",
            "processed/idle_daily/Firefox/nightly/26.0a1/20130826030203.20130902.log.57a017a3378b420cbbfb666532606b16.lzma",
            "processed/saved_session/Firefox/nightly/26.0a1/20130902030220.20130908.log.7e9556a5e32b4990a9d378eea65f57a9.lzma",
            "processed/saved_session/Firefox/nightly/26.0a1/20130829030201.20130909.log.c227775e57e24854b1aac7c21c59f85c.lzma",
            "processed/saved_session/Firefox/nightly/26.0a1/20130826030203.20130906.log.88620da62e77482285f28d5ea69beb1e.lzma",
            "processed/saved_session/Firefox/nightly/26.0a1/20130826030203.20130908.log.cc1b0c52365947c38ac2636f3384503c.lzma",
            "processed/saved_session/Firefox/nightly/26.0a1/20130830030205.20130906.log.77905bb7503a4a98aa7231b10073f47e.lzma",
            "processed/saved_session/Firefox/nightly/26.0a1/20130823030204.20130908.log.55f4ab6ada3c4e1d939f24b5da7f8dc2.lzma",
            "processed/saved_session/Firefox/nightly/26.0a1/20130902030220.20130908.log.f213918c08804d449d30e1aaec70089a.lzma",
            "processed/idle_daily/Firefox/nightly/26.0a1/20130813030205.20130907.log.24c445d3d2c241bcb5001a63a78e98fa.lzma",
            "processed/idle_daily/Firefox/nightly/26.0a1/20130831030224.20130902.log.ebe3cd20fa264cd19aab02b8ffe8cbf1.lzma",
            "processed/idle_daily/Firefox/nightly/26.0a1/20130821030213.20130906.log.778737ad596d43e4a5e9e59c38428b61.lzma",
            "processed/idle_daily/Firefox/nightly/26.0a1/20130809030203.20130903.log.43ae292120ca475589b20be24fa70171.lzma",
            "processed/idle_daily/Firefox/nightly/26.0a1/20130814141812.20130907.log.7c6c5d65b702443cac2768eb6f0e3c91.lzma",
            "processed/idle_daily/Firefox/nightly/26.0a1/20130823030204.20130903.log.f73682ebc57a4661a6f48a2a5cf2629c.lzma",
            "processed/idle_daily/Firefox/nightly/26.0a1/20130821050136.20130904.log.2d423ec779e04113996914ce81e27bfe.lzma"
        ]

    def test_filtering(self):
        all_files = self.get_file_list()
        error_files = []
        included_files = []
        excluded_files = []
        for f in all_files:
            include = True
            try:
                dims = self.schema.get_dimensions("processed", f)
                for i in range(len(self.allowed_values)):
                    if not self.schema.is_allowed(dims[i], self.allowed_values[i]):
                        include = False
                        break
            except ValueError:
                include = False
                error_files.append(f)
            if include:
                included_files.append(f)
            else:
                excluded_files.append(f)

        #print "Found", len(excluded_files), "excluded files:"
        #for f in excluded_files:
        #    print " - ", f
        #print "Found", len(included_files), "included files:"
        #for f in included_files:
        #    print " + ", f
        #print "Found", len(error_files), "invalid files"
        #for f in error_files:
        #    print " x ", f

        self.assertEqual(len(included_files), 4)
        self.assertEqual(len(error_files), 2)
        self.assertEqual(len(all_files), (len(excluded_files) + len(included_files)))

    def test_minimal_schema(self):
        minimal_schema_spec = {
            "version": 1,
            "dimensions": [
                {
                    "field_name": "submission_date",
                    "allowed_values": ["20140602", "20140604"]
                }
            ]
        }
        minimal_schema = TelemetrySchema(minimal_schema_spec)
        allowed_values = minimal_schema.sanitize_allowed_values()
        minimal_test_files = [
            "prefix/20140601.v1.log.17ff07fda8994e23baf983550246a94b.lzma",
            "prefix/20140602.v1.log.ec6f22acd37349b3b5ef03da1cc150da.lzma",
            "prefix/20140603.v1.log.64478ac84a734677bc14cbcf6cc114b7.lzma",
        ]
        error_files = []
        included_files = []
        excluded_files = []
        for f in minimal_test_files:
            include = True
            try:
                dims = minimal_schema.get_dimensions("prefix", f)
                print "dims for", f, "=", dims
                for i in range(len(allowed_values)):
                    if not minimal_schema.is_allowed(dims[i], allowed_values[i]):
                        include = False
                        break
            except ValueError:
                include = False
                error_files.append(f)
            if include:
                included_files.append(f)
            else:
                excluded_files.append(f)

        self.assertEqual(len(included_files), 1)
        self.assertEqual(included_files[0], "prefix/20140602.v1.log.ec6f22acd37349b3b5ef03da1cc150da.lzma")
        self.assertEqual(len(error_files), 0)
        self.assertEqual(len(excluded_files), 2)
        self.assertIn("prefix/20140601.v1.log.17ff07fda8994e23baf983550246a94b.lzma", excluded_files)
        self.assertIn("prefix/20140603.v1.log.64478ac84a734677bc14cbcf6cc114b7.lzma", excluded_files)

        self.assertEqual("prefix/20140602.v1.log", minimal_schema.get_filename("prefix", ["20140602"]))

    def test_safe_filename(self):
        tests = {
            "Hello World!": "Hello_World_",
            "what\nam\ni": "what_am_i",
            "saved-session": "saved_session"
        }
        for key, value in tests.iteritems():
            self.assertEqual(self.schema.safe_filename(key), value)

    def test_sanitize_allowed_values(self):
        self.assertEqual(self.allowed_values[0][0], "saved_session")

    def test_allowed_values(self):
        allowed = "saved_session"
        not_allowed = "anything_else"
        self.assertEqual(self.schema.get_allowed_value(allowed, self.allowed_values[0]), allowed)
        self.assertEqual(self.schema.get_allowed_value(not_allowed, self.allowed_values[0]), TelemetrySchema.DISALLOWED_VALUE)

    def test_apply_schema(self):
        test_inputs = []
        expected_ot = [] # <-- bad name, convenient indenting.
        other = TelemetrySchema.DISALLOWED_VALUE
        # fields:            reason           appName        channel      appVersion      appBuildID     submission_date
        #                    -------------    -----------    -------    --------------    -----------    ---------------
        # allowed:           saved-session        *          nightly          *               *          20130908
        test_inputs.append(["saved-session", "someAppName", "nightly", "someAppVersion", "someBuildID", "20130908"])
        expected_ot.append(["saved-session", "someAppName", "nightly", "someAppVersion", "someBuildID", "20130908"])

        test_inputs.append(["saved-session", "another",     "nightly", "anything is ok", "wooooo",      "20130908"])
        expected_ot.append(["saved-session", "another",     "nightly", "anything is ok", "wooooo",      "20130908"])

        test_inputs.append(["bogus",         "someAppName", "nightly", "someAppVersion", "someBuildID", "20130908"])
        expected_ot.append([other,           "someAppName", "nightly", "someAppVersion", "someBuildID", "20130908"])

        test_inputs.append(["bogus",         "someAppName", "aurora",  "someAppVersion", "someBuildID", "20140428"])
        expected_ot.append([other,           "someAppName", other,     "someAppVersion", "someBuildID", other])

        test_inputs.append(["saved-session", "someAppName", "nightly", "someAppVersion", "someBuildID", "20130908", "more", "bonus", "dimensions!"])
        expected_ot.append(["saved-session", "someAppName", "nightly", "someAppVersion", "someBuildID", "20130908"])

        for i in range(len(test_inputs)):
            actual = self.schema.apply_schema(test_inputs[i])
            self.assertEqual(actual, expected_ot[i])

    def test_get_current_file(self):
        # everything but "submission_date":
        dims = ["saved-session", "someAppName", "nightly", "someAppVersion", "someBuildID"]
        filename = self.schema.get_current_file("foo", dims, "20130908", 1)
        self.assertEqual(filename, "foo/saved_session/someAppName/nightly/someAppVersion/someBuildID.20130908.v1.log")

    def test_get_filename(self):
        dims = ["saved-session", "someAppName", "nightly", "someAppVersion", "someBuildID", "20130908"]
        filename = self.schema.get_filename("foo", dims, 99)
        self.assertEqual(filename, "foo/saved_session/someAppName/nightly/someAppVersion/someBuildID.20130908.v99.log")

    def test_dimensions_from(self):
        test_inputs = []
        expected_ot = []

        test_inputs.append({"reason": "saved-session", "appName": "Firefox", "appUpdateChannel": "release", "appVersion": "28.0", "appBuildID": "20140401001122"})
        expected_ot.append(["saved-session", "Firefox", "release", "28.0", "20140401001122", "20130908"])

        test_inputs.append({"reason": "idle-daily", "appUpdateChannel": "release", "appVersion": "28.0", "appBuildID": "20140401001122"})
        expected_ot.append(["idle-daily", "UNKNOWN", "release", "28.0", "20140401001122", "20130908"])

        test_inputs.append({})
        expected_ot.append(["UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN", "20130908"])
        for i in range(len(test_inputs)):
            actual = self.schema.dimensions_from(test_inputs[i], "20130908")
            self.assertEqual(actual, expected_ot[i])

    def test_get_field(self):
        dims = ["saved-session", "Firefox", "release", "28.0", "20130908010101", "20130908"]
        # Basic functionality
        self.assertEqual(self.schema.get_field(dims, "reason"),           "saved-session")
        self.assertEqual(self.schema.get_field(dims, "appName"),          "Firefox")
        self.assertEqual(self.schema.get_field(dims, "appUpdateChannel"), "release")
        self.assertEqual(self.schema.get_field(dims, "appVersion"),       "28.0")
        self.assertEqual(self.schema.get_field(dims, "appBuildID"),       "20130908010101")
        self.assertEqual(self.schema.get_field(dims, "submission_date"),  "20130908")

        other = TelemetrySchema.DISALLOWED_VALUE
        allowed = True
        sanitize = True
        # T, T
        self.assertEqual(self.schema.get_field(dims, "reason", limit_to_allowed=allowed, sanitize=sanitize),           "saved_session")
        self.assertEqual(self.schema.get_field(dims, "appName", limit_to_allowed=allowed, sanitize=sanitize),          "Firefox")
        self.assertEqual(self.schema.get_field(dims, "appUpdateChannel", limit_to_allowed=allowed, sanitize=sanitize), other)
        self.assertEqual(self.schema.get_field(dims, "appVersion", limit_to_allowed=allowed, sanitize=sanitize),       "28.0")
        self.assertEqual(self.schema.get_field(dims, "appBuildID", limit_to_allowed=allowed, sanitize=sanitize),       "20130908010101")
        self.assertEqual(self.schema.get_field(dims, "submission_date", limit_to_allowed=allowed, sanitize=sanitize),  "20130908")
        sanitize = False
        # T, F
        self.assertEqual(self.schema.get_field(dims, "reason", limit_to_allowed=allowed, sanitize=sanitize),           "saved-session")
        self.assertEqual(self.schema.get_field(dims, "appName", limit_to_allowed=allowed, sanitize=sanitize),          "Firefox")
        self.assertEqual(self.schema.get_field(dims, "appUpdateChannel", limit_to_allowed=allowed, sanitize=sanitize), other)
        self.assertEqual(self.schema.get_field(dims, "appVersion", limit_to_allowed=allowed, sanitize=sanitize),       "28.0")
        self.assertEqual(self.schema.get_field(dims, "appBuildID", limit_to_allowed=allowed, sanitize=sanitize),       "20130908010101")
        self.assertEqual(self.schema.get_field(dims, "submission_date", limit_to_allowed=allowed, sanitize=sanitize),  "20130908")
        allowed = False
        sanitize = True
        # F, T
        self.assertEqual(self.schema.get_field(dims, "reason", limit_to_allowed=allowed, sanitize=sanitize),           "saved_session")
        self.assertEqual(self.schema.get_field(dims, "appName", limit_to_allowed=allowed, sanitize=sanitize),          "Firefox")
        self.assertEqual(self.schema.get_field(dims, "appUpdateChannel", limit_to_allowed=allowed, sanitize=sanitize), "release")
        self.assertEqual(self.schema.get_field(dims, "appVersion", limit_to_allowed=allowed, sanitize=sanitize),       "28.0")
        self.assertEqual(self.schema.get_field(dims, "appBuildID", limit_to_allowed=allowed, sanitize=sanitize),       "20130908010101")
        self.assertEqual(self.schema.get_field(dims, "submission_date", limit_to_allowed=allowed, sanitize=sanitize),  "20130908")
        sanitize = False
        # F, F
        self.assertEqual(self.schema.get_field(dims, "reason", limit_to_allowed=allowed, sanitize=sanitize),           "saved-session")
        self.assertEqual(self.schema.get_field(dims, "appName", limit_to_allowed=allowed, sanitize=sanitize),          "Firefox")
        self.assertEqual(self.schema.get_field(dims, "appUpdateChannel", limit_to_allowed=allowed, sanitize=sanitize), "release")
        self.assertEqual(self.schema.get_field(dims, "appVersion", limit_to_allowed=allowed, sanitize=sanitize),       "28.0")
        self.assertEqual(self.schema.get_field(dims, "appBuildID", limit_to_allowed=allowed, sanitize=sanitize),       "20130908010101")
        self.assertEqual(self.schema.get_field(dims, "submission_date", limit_to_allowed=allowed, sanitize=sanitize),  "20130908")

        with self.assertRaises(ValueError):
            v = self.schema.get_field(dims, "oranges")

        # Remove the last dimension:
        dims.pop()
        with self.assertRaises(ValueError):
            v = self.schema.get_field(dims, "submission_date")

    def test_more_allowed(self):
        spec = {
            "version": 1,
            "dimensions": [
                {
                    "field_name": "reason",
                    "allowed_values": ["saved-session"]
                },
                {
                    "field_name": "appName",
                    "allowed_values": "*"
                },
                {
                    "field_name": "appUpdateChannel",
                    "allowed_values": ["nightly"]
                },
                {
                    "field_name": "appVersion",
                    "allowed_values": "*"
                },
                {
                    "field_name": "appBuildID",
                    "allowed_values": "one_specific_build"
                },
                {
                    "field_name": "submission_date",
                    "allowed_values": {
                        "min": "20130908",
                        "max": "20140401"
                    }
                }
            ]
        }
        schema = TelemetrySchema(spec)
        allowed = schema.sanitize_allowed_values()
        self.assertTrue(schema.is_allowed("20130908", allowed[5]))
        self.assertTrue(schema.is_allowed("20140401", allowed[5]))
        self.assertTrue(schema.is_allowed("20130909", allowed[5]))
        self.assertTrue(schema.is_allowed("20140101", allowed[5]))
        self.assertFalse(schema.is_allowed("20130907", allowed[5]))
        self.assertFalse(schema.is_allowed("20000000", allowed[5]))
        self.assertFalse(schema.is_allowed("20140402", allowed[5]))
        self.assertFalse(schema.is_allowed("99999999", allowed[5]))

        self.assertTrue(schema.is_allowed("one_specific_build", allowed[4]))
        self.assertFalse(schema.is_allowed("two_specific_build", allowed[4]))
        self.assertFalse(schema.is_allowed("*", allowed[4]))
        self.assertFalse(schema.is_allowed("one_specific_build ", allowed[4]))
        self.assertFalse(schema.is_allowed("one-specific-build", allowed[4]))

if __name__ == "__main__":
    unittest.main()
