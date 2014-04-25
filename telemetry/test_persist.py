# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import os
import shutil
import unittest
from telemetry.persist import StorageLayout
from telemetry.telemetry_schema import TelemetrySchema
import telemetry.util.files as fileutil

class TestPersist(unittest.TestCase):
    def setUp(self):
        test_dir = self.get_test_dir()
        self.schema = TelemetrySchema(self.get_schema_spec())
        self.storage = StorageLayout(self.schema, test_dir, 10000)
        assert not os.path.exists(test_dir)
        os.makedirs(test_dir)

    def tearDown(self):
        shutil.rmtree(self.get_test_dir())


    def get_test_dir(self):
        return "/tmp/test_telemetry_persist"

    def get_schema_spec(self):
        return {
            "version": 1,
            "dimensions": [
                {
                    "field_name": "reason",
                    "allowed_values": ["r1","r2"]
                },
                {
                    "field_name": "appName",
                    "allowed_values": ["a1"]
                },
                {
                    "field_name": "appUpdateChannel",
                    "allowed_values": ["c1", "c2", "c3"]
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
                    "allowed_values": {
                          "min": "20130101",
                          "max": "20131231"
                    }
                }
            ]
        }

    def test_storage(self):
        test_file_1 = os.path.join(self.get_test_dir(), "test.log")
        self.storage.write_filename("foo", '{"bar": "baz"}', test_file_1)
        test_file_1_md5, test_file_1_size = fileutil.md5file(test_file_1)
        self.assertEqual(test_file_1_md5, "206dd2d33a04802c31d2c74f10cc472b")

    def test_clean_newlines(self):
        self.assertEqual(self.storage.clean_newlines("ab\n\ncd\r\n"), "ab  cd  ")

if __name__ == "__main__":
    unittest.main()
