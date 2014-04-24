#!/usr/bin/env python
# encoding: utf-8
"""
symbolicate.py
Copyright (c) 2012 Mozilla Foundation. All rights reserved.
"""

import sys
import getopt
import json
import sys
import urllib2
import re
import gzip

help_message = '''
    Takes chrome hangs list of memory addresses from JSON dumps and converts them to stack traces
    Required:
        -i, --input <input_file>
        -o, --output <output_file>
        -d, --date yyyymmdd
    Optional:
        -h, --help
'''

SYMBOL_SERVER_URL = "http://symbolapi.mozilla.org:80/"

# Pulled this method from Vladan's code
def symbolicate(chromeHangsObj):
    if isinstance(chromeHangsObj, list):
        version = 1
        requestObj = chromeHangsObj
        numStacks = len(chromeHangsObj)
        if numStacks == 0:
            return []
    else:
        numStacks = len(chromeHangsObj["stacks"])
        if numStacks == 0:
            return []
        if len(chromeHangsObj["memoryMap"][0]) == 2:
            version = 3
        else:
            assert len(chromeHangsObj["memoryMap"][0]) == 4
            version = 2
        requestObj = {"stacks"    : chromeHangsObj["stacks"],
                      "memoryMap" : chromeHangsObj["memoryMap"],
                      "version"   : version}
    try:
        requestJson = json.dumps(requestObj)
        headers = { "Content-Type": "application/json" }
        requestHandle = urllib2.Request(SYMBOL_SERVER_URL, requestJson, headers)
        response = urllib2.urlopen(requestHandle, timeout=1)
    except Exception as e:
        sys.stderr.write("Exception while forwarding request: " + str(e) + "\n")
        sys.stderr.write(requestJson)
        return []
    try:
        responseJson = response.read()
    except Exception as e:
        sys.stderr.write("Exception while reading server response to symbolication request: " + str(e) + "\n")
        return []

    try:
        responseSymbols = json.loads(responseJson)
        # Sanity check
        if numStacks != len(responseSymbols):
            sys.stderr.write(str(len(responseSymbols)) + " hangs in response, " + str(numStacks) + " hangs in request!\n")
            return []

        # Sanity check
        for hangIndex in range(0, numStacks):
            if version == 1:
                stack = chromeHangsObj[hangIndex]["stack"]
            else:
                stack = chromeHangsObj["stacks"][hangIndex]
            requestStackLen = len(stack)
            responseStackLen = len(responseSymbols[hangIndex])
            if requestStackLen != responseStackLen:
                sys.stderr.write(str(responseStackLen) + " symbols in response, " + str(requestStackLen) + " PCs in request!\n")
                return []
    except Exception as e:
        sys.stderr.write("Exception while parsing server response to forwarded request: " + str(e) + "\n")
        return []

    return responseSymbols

def process(input_file, output_file, submission_date):
    if input_file == '-':
        fin = sys.stdin
    else:
        fin = open(input_file, "rb")

    fout = gzip.open(output_file, "wb")

    symbolication_errors = 0
    symbolication_requests = 0
    line_num = 0

    while True:
        line_num += 1
        uuid = fin.read(36)
        if len(uuid) == 0:
            break
        assert len(uuid) == 36

        tab = fin.read(1)
        assert tab == '\t'

        jsonstr = fin.readline()
        try:
            json_dict = json.loads(jsonstr)
        except Exception, e:
            print >> sys.stderr, "Error parsing json on line", line_num, ":", e
            continue

        hang_stacks = []
        hangs = json_dict.get("chromeHangs")
        if hangs:
          del json_dict["chromeHangs"]
          hang_stacks = symbolicate(hangs)
          symbolication_requests += 1
          if hang_stacks == []:
              symbolication_errors += 1

        late_writes_stacks = []
        writes = json_dict.get("lateWrites")
        if writes:
          del json_dict["lateWrites"]
          late_writes_stacks = symbolicate(writes)
          symbolication_requests += 1
          if late_writes_stacks == []:
              symbolication_errors += 1

        if "histograms" in json_dict:
            del json_dict["histograms"]
        fout.write(submission_date)
        fout.write("\t")
        fout.write(uuid)
        fout.write("\t")
        fout.write(json.dumps(json_dict))

        for stack in hang_stacks:
            fout.write("\n----- BEGIN HANG STACK -----\n")
            fout.write("\n".join(stack))
            fout.write("\n----- END HANG STACK -----\n")

        for stack in late_writes_stacks:
            fout.write("\n----- BEGIN LATE WRITE STACK -----\n")
            fout.write("\n".join(stack))
            fout.write("\n----- END LATE WRITE STACK -----\n")

    fin.close()
    fout.close()
    sys.stderr.write("Requested %s symbolications. Got %s errors." % (symbolication_requests, symbolication_errors))

class Usage(Exception):
    def __init__(self, msg):
        self.msg = msg


def main(argv=None):
    if argv is None:
        argv = sys.argv
    try:
        try:
            opts, args = getopt.getopt(argv[1:], "hi:o:d:v", ["help", "input=", "output=", "date="])
        except getopt.error, msg:
            raise Usage(msg)

        input_file = None
        output_file = None
        submission_date = None
        # option processing
        for option, value in opts:
            if option == "-v":
                verbose = True
            if option in ("-h", "--help"):
                raise Usage(help_message)
            if option in ("-i", "--input"):
                input_file = value
            if option in ("-o", "--output"):
                output_file = value
            if option in ("-d", "--date"):
                submission_date = value
        process(input_file, output_file, submission_date)
    except Usage, err:
        print >> sys.stderr, sys.argv[0].split("/")[-1] + ": " + str(err.msg)
        print >> sys.stderr, " for help use --help"
        return 2

if __name__ == "__main__":
    sys.exit(main())
