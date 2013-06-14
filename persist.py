#!/usr/bin/env python
# encoding: utf-8
"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""

import os
import sys
import getopt
import json
import sys
import urllib2
import re
import gzip
import revision_cache
import histogram_tools
import traceback

help_message = '''
    Takes a list of raw telemetry pings and writes them back out in a more
    compact form
    Required:
        -i, --input <input_file>
        -o, --output <output_file>
    Optional:
        -h, --help
'''

def write(uuid, obj, dimensions):
   filename = get_filename(dimensions)
   sys.stderr.write("Writing %s to %s" % (uuid, filename))
   try:
      fout = open(filename, "a")
   except IOError:
      os.makedirs(os.path.dirname(filename))
      fout = open(filename, "a")
   fout.write(uuid)
   fout.write("\t")
   fout.write(json.dumps(obj))
   fout.write("\n")
   fout.close()

def get_filename(dimensions):
   dirname = os.path.join(*dimensions)
   # TODO: get files in order, find newest non-full one
   return os.path.join("./data", re.sub(r'[^a-zA-Z0-9_/]', "_", dirname))
