# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import os
import shutil
import unittest
from telemetry.util.compress import CompressedFile

class TestCompressedFile(unittest.TestCase):
    def get_test_dir(self):
        return "test"

    def get_test_data(self):
        return ["line 1", "line 2", "line 3", "another \tline", "line 5"]

    def test_open_now(self):
        with self.assertRaises(IOError):
            c = CompressedFile("dummy.gz", open_now=True)

        c = CompressedFile("dummy.gz", open_now=False)
        self.assertEqual(0, c.line_num)

    def test_open_bad_mode(self):
        with self.assertRaises(ValueError):
            c = CompressedFile("dummy.lzma", mode="bogus",
                open_now=True, force_popen=True)

    def test_no_extension(self):
        # we can't auto-detect with no file extension
        with self.assertRaises(ValueError):
            c = CompressedFile("dummy", compression_type="auto")

    def test_no_extension_manual(self):
        # we don't need to auto-detect if we specify the type.
        c = CompressedFile("dummy", compression_type="gz")
        self.assertEqual("gz", c.compression_type)

    def test_detect_compression_type(self):
        c = CompressedFile("dummy.gz")
        for t in ["gz", "lzma", "xz"]:
            example_file = "/path/to/some.compressed.file." + t
            #print "Checking", example_file
            self.assertEqual(t, c.detect_compression_type(example_file))

            # Check "auto":
            c2 = CompressedFile(example_file, compression_type="foo")
            self.assertEqual("foo", c2.compression_type)

            c3 = CompressedFile(example_file, compression_type="auto")
            self.assertEqual(t, c3.compression_type)

    def test_decompress_types(self):
        for t in ["gz", "lzma", "xz"]:
            self.decompress_one_file(t, force_popen=False)

    def decompress_one_file(self, filetype, force_popen):
        base_dir = self.get_test_dir()
        c = CompressedFile(os.path.join(base_dir, "test.txt." + filetype),
                           force_popen=force_popen)
        lines = []
        for line in c:
            lines.append(line.strip())

        expected = self.get_test_data()
        self.assertEqual(len(expected), c.line_num)
        c.close()
        for i in range(c.line_num):
            self.assertEqual(expected[i], lines[i])

    def test_decompress_lzma_popen(self):
        base_dir = self.get_test_dir()
        for t in ["lzma", "xz"]:
            self.decompress_one_file(t, force_popen=True)

    def test_compress_lzma(self):
        self.assertEqual(1, 1)
        # TODO: write data to file.lzma
        #       compress it
        #       read it back
        #       make sure it looks ok

    def test_compress_xz(self):
        self.assertEqual(1, 1)
        # TODO: write data to file.xz
        #       compress it
        #       read it back
        #       make sure it looks ok

    def test_compress_gz(self):
        self.assertEqual(1, 1)
        # TODO: write data to file.gz
        #       compress it
        #       read it back
        #       make sure it looks ok

if __name__ == "__main__":
    unittest.main()
