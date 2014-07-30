# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import os
import shutil
import unittest
from telemetry.util.compress import CompressedFile

class TestCompressedFile(unittest.TestCase):
    def setUp(self):
        test_file = self.get_raw_test_file()
        assert not os.path.exists(test_file)
        with open(test_file, 'w') as raw:
            for line in self.get_test_data():
                raw.write(line + "\n")

    def tearDown(self):
        test_file = self.get_raw_test_file()
        if os.path.exists(test_file):
            os.remove(test_file)

    def get_raw_test_file(self):
        return os.path.join(self.get_test_dir(), "test.txt")

    def get_test_dir(self):
        return "test"

    def get_test_data(self):
        return ["line 1", "line 2", "line 3", "another \tline", "line 5"]

    def get_supported_compression_types(self):
        return ["xz", "lzma", "gz"]

    def get_supported_popen_compression_types(self):
        return ["xz", "lzma"]

    def test_unknown_compression_type(self):
        with self.assertRaises(ValueError):
            c = CompressedFile("dummy.gz", compression_type="foo",
                    open_now=True)

    def test_open_now(self):
        with self.assertRaises(IOError):
            c = CompressedFile("dummy.gz", open_now=True)

        c = CompressedFile("dummy.gz", open_now=False)
        self.assertEqual(0, c.line_num)

    def test_open_bad_mode(self):
        with self.assertRaises(ValueError):
            c = CompressedFile("dummy.lzma", mode="bogus",
                open_now=True, force_popen=True)

    def test_write_to_ro_file(self):
        write_test_file = "dummy.lzma"
        assert not os.path.exists(write_test_file)
        with self.assertRaises(IOError):
            c = CompressedFile(write_test_file, mode="r")
            c.write("testing...")
        assert not os.path.exists(write_test_file)

    def test_read_from_wo_file(self):
        with self.assertRaises(IOError):
            c = CompressedFile("dummy.lzma", mode="w")
            last_line = None
            for line in c:
                last_line = line
                break

    def test_missing_executable(self):
        with self.assertRaises(RuntimeError):
            c = CompressedFile("dummy.lzma", open_now=False)
            CompressedFile.SEARCH_PATH = []
            path = c.get_executable()

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
        for t in self.get_supported_compression_types():
            example_file = "/path/to/some.compressed.file." + t
            #print "Checking", example_file
            self.assertEqual(t, c.detect_compression_type(example_file))

            # Check "auto":
            c2 = CompressedFile(example_file, compression_type="foo")
            self.assertEqual("foo", c2.compression_type)

            c3 = CompressedFile(example_file, compression_type="auto")
            self.assertEqual(t, c3.compression_type)

    def test_decompress_types(self):
        for t in self.get_supported_compression_types():
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

    def test_decompress_popen(self):
        base_dir = self.get_test_dir()
        for t in self.get_supported_popen_compression_types():
            self.decompress_one_file(t, force_popen=True)

    def compress_one_file(self, filetype, force_popen):
        base_dir = self.get_test_dir()
        write_test_file = os.path.join(base_dir, "write_test." + filetype)
        assert not os.path.exists(write_test_file)
        c = CompressedFile(write_test_file, mode="w", force_popen=force_popen)

        # Write data to file.lzma
        lines = self.get_test_data()
        for line in lines:
            c.write(line + "\n")
        c.close()

        # Read it back
        after = []
        c = CompressedFile(write_test_file, mode="r", force_popen=force_popen)
        for line in c:
            after.append(line.strip())

        # make sure it looks ok
        self.assertEqual(len(lines), len(after))
        for i in range(len(lines)):
            self.assertEqual(lines[i], after[i])

        # all is well, remove the file.
        os.remove(write_test_file)

    def light_versus_heavy(self, file_type, force_popen):
        base_dir = self.get_test_dir()
        test_lines = []
        for i in range(2000):
            test_lines.append("Hello there {0}!".format(i))
        test_contents = "\n".join(test_lines)
        # Write it with a little compression.
        write_light = os.path.join(base_dir, "clevel_test.1.{}.{}".format(
                                   force_popen, file_type))
        assert not os.path.exists(write_light)
        c = CompressedFile(write_light, mode="w", force_popen=force_popen,
                           compression_level=1)
        c.write(test_contents)
        c.close()

        # Check the resulting size
        light_compression_size = os.stat(write_light).st_size
        os.remove(write_light)

        # Write it with max compression.
        write_heavy = os.path.join(base_dir, "clevel_test.9.{}.{}".format(
                                   force_popen, file_type))
        assert not os.path.exists(write_heavy)
        c = CompressedFile(write_heavy, mode="w", force_popen=force_popen,
                           compression_level=9)
        c.write(test_contents)
        c.close()

        # Check the size again.
        heavy_compression_size = os.stat(write_heavy).st_size
        os.remove(write_heavy)

        common_msg = "size should be less than raw size for type {0} " \
                     "(popen={1})".format(file_type, force_popen)

        self.assertTrue(light_compression_size < len(test_contents),
                        msg="Lightly Compressed " + common_msg)
        self.assertTrue(heavy_compression_size < len(test_contents),
                        msg="Heavily Compressed " + common_msg)

        #print "{0}, popen={1} - raw: {2}, light: {3}, heavy: {4}".format(
        #    t, popen, len(test_contents), light_compression_size,
        #    heavy_compression_size)
        self.assertTrue(light_compression_size > heavy_compression_size,
                        msg="Light compression ({0}) should be larger " \
                            "than heavy compression ({1}) for type {2} " \
                            "(popen={3})".format(light_compression_size,
                                heavy_compression_size, file_type, force_popen))

    def test_compression_level(self):
        for t in ["xz", "gz", "lzma"]:
            for popen in [True, False]:
                self.light_versus_heavy(t, popen)


    def verify_contents(self, filename, force_popen):
        # Read it back
        expected = self.get_test_data()
        actual = []
        c = CompressedFile(filename, mode="r", force_popen=force_popen)
        for line in c:
            actual.append(line.strip())

        # make sure it looks ok
        self.assertEqual(len(expected), len(actual))
        for i in range(len(expected)):
            self.assertEqual(expected[i], actual[i])

    def compress_from(self, filetype, force_popen):
        base_dir = self.get_test_dir()
        raw_test_file = self.get_raw_test_file()
        comp_test_file = os.path.join(base_dir, "from_test." + filetype)
        assert not os.path.exists(comp_test_file)
        c = CompressedFile(comp_test_file, mode="w", force_popen=force_popen)
        c.compress_from(raw_test_file)
        c.close()
        self.verify_contents(comp_test_file, force_popen)
        os.remove(comp_test_file)

    def test_compress_popen(self):
        for t in self.get_supported_popen_compression_types():
            self.compress_one_file(t, force_popen=True)

    def test_compress_types(self):
        for t in self.get_supported_compression_types():
            self.compress_one_file(t, force_popen=False)

    def test_compress_from_types(self):
        for t in self.get_supported_compression_types():
            self.compress_from(t, force_popen=False)

    def test_compress_from_popen(self):
        for t in self.get_supported_popen_compression_types():
            self.compress_from(t, force_popen=True)

    def test_compress_from_cleanup(self):
        base_dir = self.get_test_dir()
        comp_test_file = os.path.join(base_dir, "cleanup_test.gz")
        raw_test_file = self.get_raw_test_file()
        assert os.path.exists(raw_test_file)
        c = CompressedFile(comp_test_file, mode="w")
        c.compress_from(raw_test_file, remove_original=True)
        c.close()
        assert not os.path.exists(raw_test_file)


if __name__ == "__main__":
    unittest.main()
