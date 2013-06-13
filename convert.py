#!/usr/bin/env python
# encoding: utf-8
"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.
"""

import re
import sys
import getopt
import json
import sys
import urllib2
import re
import gzip

help_message = '''
    Takes a list of raw telemetry pings and writes them back out in a more
    compact form
    Required:
        -i, --input <input_file>
        -o, --output <output_file>
    Optional:
        -h, --help
'''

# TODO:
# - fetch (and cache) all revisions of Histograms.json using something like:
#   http://hg.mozilla.org/mozilla-central/log/tip/toolkit/components/telemetry/Histograms.json
#   http://hg.mozilla.org/releases/mozilla-aurora/log/tip/toolkit/components/telemetry/Histograms.json
#   http://hg.mozilla.org/releases/mozilla-beta/log/tip/toolkit/components/telemetry/Histograms.json
#   http://hg.mozilla.org/releases/mozilla-release/log/tip/toolkit/components/telemetry/Histograms.json

# cache the full details of Histograms.json
hist_details = dict()

# cache just the name -> id mapping (by revision)
hist_name_to_id = dict()

valid_revisions = re.compile('^(http://.*)/rev/([0-9a-f]+)/?$')

def map_name(revision, histogram_name):
   if revision not in hist_name_to_id:
      get_file_from_revision(revision, "toolkit/components/telemetry/Histograms.json")

   rev_cache = hist_name_to_id[revision]
   if histogram_name not in rev_cache:
      sys.stderr.write("Histogram %s not found in revision %s\n" % (histogram_name, revision))
   return rev_cache[histogram_name]

def map_value(revision, val):
   rewritten = []
   for k in ("sum", "log_sum", "log_sum_squares"):
      rewritten.append(val.get(k, -1))
   rewritten.append(val["values"])
   return rewritten

def get_file_from_revision(revision, file_path):
   # revision is like
   #   http://hg.mozilla.org/releases/mozilla-aurora/rev/089956e907ed
   # and path should be like
   #   toolkit/components/telemetry/Histograms.json
   # to produce a full URL like
   #   http://hg.mozilla.org/releases/mozilla-aurora/raw-file/089956e907ed/toolkit/components/telemetry/Histograms.json
   global hist_name_to_id
   global hist_details
   global valid_revisions
   if revision not in hist_name_to_id:
      hist_name_to_id[revision] = dict()
   m = valid_revisions.match(revision)
   if m:
      url = "/".join((m.group(1), "raw-file", m.group(2), file_path))
      sys.stderr.write("Fetching '%s'\n" % url)
      # TODO: cache locally
      response = urllib2.urlopen(url)
      histograms_json = response.read()
      histograms = json.loads(histograms_json)
      hist_details[revision] = histograms
      cache = hist_name_to_id[revision]
      current_id = 0
      for hist_name in iter(sorted(histograms.keys())):
         sys.stderr.write("mapping %s to %d\n" % (hist_name, current_id))
         cache[hist_name] = current_id
         histograms[hist_name]["id"] = current_id
         current_id += 1
      sys.stderr.write("Histograms.%s.json" % m.group(2))
      sys.stderr.write(json.dumps(histograms))
   else:
      sys.stderr.write("Invalid revision: '%s'\n" % (revision))

   
def rewrite_hists(revision, histograms):
   rewritten = dict()
   for key, val in histograms.iteritems():
      rewritten[map_name(revision, key)] = map_value(revision, val)
   return rewritten

def process(input_file, output_file):
    line = 0
    if input_file == '-':
        fin = sys.stdin
    else:
        fin = open(input_file, "rb")

    if output_file == '-':
       fout = sys.stdout
    else:
       fout = gzip.open(output_file, "wb")

    while True:
        line += 1
        first_byte = fin.read(1)
        if len(first_byte) == 0:
            break;
        assert len(first_byte) == 1

        date = fin.read(8)
        assert len(date) == 8

        uuid = fin.read(36)
        assert len(uuid) == 36

        first_uuid_byte = int(uuid[0:2], 16) - 128
        if first_uuid_byte < 0:
            first_uuid_byte += 256
        assert first_uuid_byte == ord(first_byte)

        tab = fin.read(1)
        assert tab == '\t'

        jsonstr = fin.readline()
        json_dict = json.loads(jsonstr)

        info = json_dict.get("info")
        if "revision" not in info:
           sys.stderr.write("no revision found on line %d: %s\n" % (line, json.dumps(info)))
           continue

        revision = info.get("revision")

        try:
           json_dict["histograms"] = rewrite_hists(revision, json_dict["histograms"])
        except KeyError:
           sys.stderr.write("Missing histogram on line %d: %s\n" % (line, json.dumps(info)))

        fout.write(date)
        fout.write("\t")
        fout.write(uuid)
#        fout.write("\t")
#        fout.write(revision)
        fout.write("\t")
        fout.write(json.dumps(json_dict))
        fout.write("\n")

    fin.close()
    fout.close()  

class Usage(Exception):
   def __init__(self, msg):
      self.msg = msg


def main(argv=None):
   if argv is None:
      argv = sys.argv
   try:
      try:
         opts, args = getopt.getopt(argv[1:], "hi:o:v", ["help", "input=", "output="])
      except getopt.error, msg:
         raise Usage(msg)
      
      input_file = None
      output_file = None
      # option processing
      for option, value in opts:
         if option == "-v":
            verbose = True
         elif option in ("-h", "--help"):
            raise Usage(help_message)
         elif option in ("-i", "--input"):
             input_file = value
         elif option in ("-o", "--output"):
            output_file = value
      
      process(input_file, output_file)

   except Usage, err:
       print >> sys.stderr, sys.argv[0].split("/")[-1] + ": " + str(err.msg)
       print >> sys.stderr, " for help use --help"
       return 2

if __name__ == "__main__":
   sys.exit(main())
