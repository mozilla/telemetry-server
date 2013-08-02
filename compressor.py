"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""

###########################################################################
# Use this script to validate, convert, and compress raw telemetry
# log files.
#
# A list of files to be compressed is read from stdin.
#
# Example:
#  python get_compressibles.py /path/to/telemetry | python compressor.py
#
###########################################################################

import sys, os, re, glob
import logging
from persist import StorageLayout
from datetime import datetime
from subprocess import Popen, PIPE

dry_run = False

python_path = "/usr/bin/python"
convert_script = "convert.py"
convert_dir = ""
try:
    convert_dir = os.path.dirname(__file__)
except NameError:
    # __file__ is not availble
    pass
convert_path = os.path.join(convert_dir, convert_script)

compress_path = StorageLayout.COMPRESS_PATH
compression_args = StorageLayout.COMPRESSION_ARGS
compress_cmd = [compress_path] + compression_args

if len(sys.argv) > 1 and sys.argv[1] == "--dry-run":
    dry_run = True

# TODO: dedupe these with get_compressibles.py
acs = StorageLayout.COMPRESSED_SUFFIX
acs_len = len(acs)
log_date_pattern = re.compile("^.*\.([0-9]{8})\.log$")

while True:
    filename = sys.stdin.readline()
    if filename == '':
        break
    else:
        filename = filename.rstrip()
    logging.info("Compressing " + filename)

    # Don't actually do anything.
    if dry_run:
        continue

    base_ends = filename.find(".log") + 4
    if base_ends < 4:
        logging.warn("Bad filename encountered, skipping: " + filename)
        continue
    basename = filename[0:base_ends]

    conversion_args = []
    m = log_date_pattern.match(basename)
    if m:
        conversion_args = ["--date", m.group(1)]

    existing_logs = glob.glob(basename + ".[0-9]*" + acs)
    suffixes = [ int(s[len(basename) + 1:-acs_len]) for s in existing_logs ]

    if len(suffixes) == 0:
        next_log_num = 1
    else:
        next_log_num = sorted(suffixes)[-1] + 1

    # TODO: handle race condition?
    #   http://stackoverflow.com/questions/82831/how-do-i-check-if-a-file-exists-using-python
    while os.path.exists(basename + "." + str(next_log_num) + acs):
        logging.warn("Another challenger appears!")
        next_log_num += 1

    comp_name = basename + "." + str(next_log_num) + acs
    # reserve it!
    f_comp = open(comp_name, "wb")
    # TODO: open f_comp with same buffer size as below?

    # Rename uncompressed file to a temp name
    tmp_name = comp_name + ".compressing"
    logging.debug("Moving %s to %s" % (filename, tmp_name))
    os.rename(filename, tmp_name)

    # Read input file as text (line-buffered)
    f_raw = open(tmp_name, "r", 1)

    logging.debug("compressing %s to %s" % (filename, comp_name))
    start = datetime.now()

    # Now set up our processing pipeline:
    # - read from filename (line-buffered, convert, write to pipe
    # - read from pipe, compress, write to comp_name
    p_convert = Popen([python_path, convert_path] + conversion_args, bufsize=1, stdin=f_raw, stdout=PIPE, stderr=sys.stderr)
    p_compress = Popen(compress_cmd, bufsize=65536, stdin=p_convert.stdout, stdout=f_comp, stderr=sys.stderr)
    p_convert.stdout.close()

    # Note: it looks like p_compress.wait() is what we want, but the docs
    #       warn of a deadlock, so we use communicate() instead.
    p_compress.communicate()

    raw_mb = float(f_raw.tell()) / 1024.0 / 1024.0
    comp_mb = float(f_comp.tell()) / 1024.0 / 1024.0
    f_raw.close()
    f_comp.close()

    # Remove raw file
    os.remove(tmp_name)
    delta = (datetime.now() - start)
    sec = float(delta.seconds) + float(delta.microseconds) / 1000000.0
    logging.info("Compressed %s as #%d in %.2fs. Size before: %.2fMB, after: %.2fMB (r: %.2fMB/s, w: %.2fMB/s)" % (filename, next_log_num, sec, raw_mb, comp_mb, (raw_mb/sec), (comp_mb/sec)))

