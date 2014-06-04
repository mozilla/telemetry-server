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

    def test_write_filename(self):
        test_file = os.path.join(self.get_test_dir(), "test.log")
        self.storage.write_filename("foo", '{"bar":"baz"}', test_file)
        test_file_md5, test_file_size = fileutil.md5file(test_file)
        self.assertEqual(test_file_md5, "0ea91df239ea79ed2ebab34b46d455fc")

        test_file = os.path.join(self.get_test_dir(), "test2.log")
        # Now test writing an object
        self.storage.write_filename("foo", {"bar":"baz"}, test_file)
        test_file_md5, test_file_size = fileutil.md5file(test_file)
        self.assertEqual(test_file_md5, "0ea91df239ea79ed2ebab34b46d455fc")

    def test_write(self):
        dims = ["r1", "a1", "c1", "v1", "b1", "20130102"]
        test_dir = self.get_test_dir()
        test_file = self.schema.get_filename(test_dir, dims)
        self.assertEquals(test_file, test_dir + "/r1/a1/c1/v1/b1.20130102.v1.log")

        self.storage.write("foo", '{"bar":"baz"}', dims)
        md5, size = fileutil.md5file(test_file)
        self.assertEqual(md5, "0ea91df239ea79ed2ebab34b46d455fc")

    def test_clean_newlines(self):
        self.assertEqual(self.storage.clean_newlines("ab\n\ncd\r\n"), "ab  cd  ")

    def test_minimal_schema(self):
        minimal_schema_spec = {
            "version": 1,
            "dimensions": [
                {
                    "field_name": "submission_date",
                    "allowed_values": "*"
                }
            ]
        }
        test_dir = self.get_test_dir()
        minimal_schema = TelemetrySchema(minimal_schema_spec)
        storage = StorageLayout(minimal_schema, test_dir, 10000)
        dims = ["20140604"]
        test_file = minimal_schema.get_filename(test_dir, dims)
        self.assertEquals(test_file, test_dir + "/" + dims[0] + ".v1.log")
        storage.write("foo", '{"bar":"baz"}', dims)
        md5, size = fileutil.md5file(test_file)
        self.assertEqual(md5, "0ea91df239ea79ed2ebab34b46d455fc")
        self.assertEqual(size, 18)

    def test_rotate(self):
        test_file = os.path.join(self.get_test_dir(), "test.log")
        key = "01234567890123456789012345678901234567890123456789"
        value = '{"some filler stuff here":"fffffffffffffffffff"}'
        # each iteration should be 100 bytes.
        for i in range(99):
            result = self.storage.write_filename(key, value, test_file)
            self.assertEquals(result, test_file)

        # The 100th iteration should cause the file to rotate
        rolled = self.storage.write_filename(key, value, test_file)
        # rolled should be <test_dir>/test.log.<pid>.<timestamp><suffix>
        self.assertNotEqual(rolled, test_file)
        self.assertTrue(rolled.startswith(test_file))
        self.assertTrue(rolled.endswith(StorageLayout.PENDING_COMPRESSION_SUFFIX))

if __name__ == "__main__":
    unittest.main()
