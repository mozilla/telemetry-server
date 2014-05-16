#!/usr/bin/env python
# encoding: utf-8
"""
symbolicate.py
Copyright (c) 2012 Mozilla Foundation. All rights reserved.
"""

import sys
import getopt
try:
    import simplejson as json
except ImportError:
    import json
import sys
import urllib2
import re
import gzip
import time
import traceback
from datetime import datetime

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

# We won't bother symbolicating file+offset pairs that occur less than
# this many times.
MIN_HITS = 0

NUM_RETRIES = 3
RETRY_DELAY = 5 # seconds

MIN_FRAMES = 15
irrelevantSignatureRegEx = re.compile('|'.join([
  'mozilla::ipc::RPCChannel::Call',
  '@-*0x[0-9a-fA-F]{2,}',
  '@-*0x[1-9a-fA-F]',
  'ashmem',
  'app_process@0x.*',
  'core\.odex@0x.*',
  '_CxxThrowException',
  'dalvik-heap',
  'dalvik-jit-code-cache',
  'dalvik-LinearAlloc',
  'dalvik-mark-stack',
  'data@app@org\.mozilla\.fennec-\d\.apk@classes\.dex@0x.*',
  'framework\.odex@0x.*',
  'google_breakpad::ExceptionHandler::HandleInvalidParameter.*',
  'KiFastSystemCallRet',
  'libandroid_runtime\.so@0x.*',
  'libbinder\.so@0x.*',
  'libc\.so@.*',
  'libc-2\.5\.so@.*',
  'libEGL\.so@.*',
  'libdvm\.so\s*@\s*0x.*',
  'libgui\.so@0x.*',
  'libicudata.so@.*',
  'libMali\.so@0x.*',
  'libutils\.so@0x.*',
  'libz\.so@0x.*',
  'linux-gate\.so@0x.*',
  'mnt@asec@org\.mozilla\.fennec-\d@pkg\.apk@classes\.dex@0x.*',
  'MOZ_Assert',
  'MOZ_Crash',
  'mozcrt19.dll@0x.*',
  'mozilla::ipc::RPCChannel::Call\(',
  '_NSRaiseError',
  '(Nt|Zw)?WaitForSingleObject(Ex)?',
  '(Nt|Zw)?WaitForMultipleObjects(Ex)?',
  'nvmap@0x.*',
  'org\.mozilla\.fennec-\d\.apk@0x.*',
  'RaiseException',
  'RtlpAdjustHeapLookasideDepth',
  'system@framework@*\.jar@classes\.dex@0x.*',
  '___TERMINATING_DUE_TO_UNCAUGHT_EXCEPTION___',
  'WaitForSingleObjectExImplementation',
  'WaitForMultipleObjectsExImplementation',
  'RealMsgWaitFor.*'
  '_ZdlPv',
  'zero'
]))
rawAddressRegEx = re.compile("-*0x[0-9a-fA-F]{1,}")
jsFrameRegEx = re.compile("^js::")
interestingLibs = ["xul.dll", "firefox.exe", "mozjs.dll"]
boringEventHandlingFrames = set([
  "NS_ProcessNextEvent_P(nsIThread *,bool) (in xul.pdb)",
  "mozilla::ipc::MessagePump::Run(base::MessagePump::Delegate *) (in xul.pdb)",
  "MessageLoop::RunHandler() (in xul.pdb)",
  "MessageLoop::Run() (in xul.pdb)",
  "nsBaseAppShell::Run() (in xul.pdb)",
  "nsAppShell::Run() (in xul.pdb)",
  "nsAppStartup::Run() (in xul.pdb)",
  "XREMain::XRE_mainRun() (in xul.pdb)",
  "XREMain::XRE_main(int,char * * const,nsXREAppData const *) (in xul.pdb)"
])

def delta_sec(start, end=None):
    if end is None:
        end = datetime.now()
    delta = end - start
    sec = delta.seconds + float(delta.microseconds) / 1000.0 / 1000.0
    return sec

def new_request():
    return {"stacks": [], "memoryMap": [], "version": 3}

def process_request(request_obj):
    request_json = min_json(request_obj)
    response_json = fetch_symbols(request_json)
    if response_json is None:
        # Symbolication failed.
        sys.stderr.write("Server response was None for request:\n" + request_json + "\n")
        return None
    response_obj = json.loads(response_json)
    return get_symbol_cache(request_obj, response_obj)

def fetch_symbols(requestJson):
    attempts = 0
    last_exception = None
    for r in range(NUM_RETRIES):
        attempts += 1
        try:
            # send the request
            headers = { "Content-Type": "application/json" }
            requestHandle = urllib2.Request(SYMBOL_SERVER_URL, requestJson, headers)
            response = urllib2.urlopen(requestHandle, timeout=60)
            responseJson = response.read()
            #sys.stderr.write("Request Bytes: {}, Response Bytes: {}\n".format(len(requestJson), len(responseJson)))
            print responseJson
            return responseJson
        except Exception as e:
            last_exception = e
            sys.stderr.write("Request attempt {} failed. Waiting {} seconds " \
                "to retry. Error was: {}.\n".format(attempts, RETRY_DELAY, e))
            time.sleep(RETRY_DELAY)

    # We've retried the max number of times.
    raise last_exception

def get_symbol_cache(request, response):
    numStacks = len(request["stacks"])
    # Sanity check #1: same number of stacks as memoryMap entries
    if numStacks != len(request["memoryMap"]):
        sys.stderr.write("Bad request: len(stacks) was {}, len(memoryMap) " \
                " was {} (they should be the same)\n".format(numStacks,
                len(request["memoryMap"])))
        return None
    # Sanity check #2: same number of symbolicated stacks in response as
    # stacks in request.
    if numStacks != len(response):
        sys.stderr.write("Bad response: len(stacks) was {}, " \
                "len(responseStacks) was {} (they should be the same)\n".format(
                numStacks, len(response)))
        return None

    cache = {}
    for i in range(0, numStacks):
        stack = request["stacks"][i]
        symbolicatedStack = response[i]
        numStackEntries = len(stack)
        # Sanity check #3: stacks should be the same length.
        if numStackEntries != len(symbolicatedStack):
            sys.stderr.write("Bad stack: len(stacks[{0}]) was {1}, " \
                    "len(responseStacks[{0}]) was {2} (they should be " \
                    "the same)\n".format(i, numStackEntries,
                    len(symbolicatedStack)))
            continue

        for s in range(0, numStackEntries):
            key = get_stack_key(stack[s], request["memoryMap"])
            symbol = symbolicatedStack[s]
            cache[key] = symbol
    return cache

def symbolicate(combined_stacks):
    sys.stderr.write("About to symbolicate {} libs\n".format(len(combined_stacks.keys())))
    # TODO: batch small ones, split up large ones.
    # combined_stacks is
    #  {
    #    (lib1, debugid1): [offset11, offset12, ...]
    #    (lib2, debugid2): [offset21, offset22, ...]
    #    ...
    #  }
    symbolicated = {}
    current_request = new_request()
    current_frame_count = 0
    # Sort in descending order by number of stack items
    for k, v in sorted(combined_stacks.iteritems(),
                       key=lambda a: len(a[1]), reverse=True):
        lib, debugid = k
        if current_frame_count > 1000:
            # Now send it.
            try:
                new_symbols = process_request(current_request)
                if new_symbols is not None:
                    symbolicated.update(new_symbols)
                else:
                    sys.stderr.write("Failed to get symbols for: " + json.dumps(current_request) + "\n")
            except Exception as e:
                sys.stderr.write("Exception while processing symbolication request: " + str(e) + "\n")
            current_request = new_request()
            current_frame_count = 0

        # Combine stack into current request
        current_frame_count += len(v)
        stack_idx = len(current_request["memoryMap"])
        current_request["memoryMap"].append([lib, debugid])
        current_request["stacks"].append([[stack_idx, offset] for offset in v])

    if current_frame_count > 0:
        # Send final request (if there is one)
        try:
            new_symbols = process_request(current_request)
            if new_symbols is not None:
                symbolicated.update(new_symbols)
            else:
                sys.stderr.write("Failed to get symbols for final request: " + json.dumps(current_request) + "\n")
        except Exception as e:
            sys.stderr.write("Exception while processing final symbolication request: " + str(e) + "\n")
    return symbolicated

def is_interesting(frame):
    if is_irrelevant(frame):
        return False
    if is_raw(frame):
        return False
    if is_js(frame):
        return False
    return True

def is_irrelevant(frame):
    m = irrelevantSignatureRegEx.match(frame)
    if m:
        return True

def is_raw(frame):
    m = rawAddressRegEx.match(frame)
    if m:
        return True

def is_js(frame):
    m = jsFrameRegEx.match(frame)
    if m:
        return True

def is_boring(frame):
    return frame in boringEventHandlingFrames

def is_interesting_lib(frame):
    for lib in interestingLibs:
        if lib in frame:
            return True
    return False

def min_json(obj):
    return json.dumps(obj, separators=(',', ':'))

def get_signature(stack):
    # From Vladan:
    # 1. Remove uninteresting frames from the top of the stack
    # 2. Keep removing raw addresses and JS frames from the top of the stack
    #    until you hit a real frame
    # 3. Get remaining top N frames (I used N = 15), or more if no
    #    xul.dll/firefox.exe/mozjs.dll in the top N frames (up to first
    #    xul.dll/etc frame plus one extra frame)
    # 4. From this subset of frames, remove all XRE_Main or generic event-
    #    handling frames from the bottom of stack (until you hit a real frame)
    # 5. Remove any raw addresses and JS frames from the bottom of stack (same
    #    as step 2 but done to the other end of the stack)

    interesting = [ is_interesting(f) for f in stack ]
    try:
        first_interesting_frame = interesting.index(True)
    except ValueError as e:
        # No interesting frames in stack.
        return []

    signature = stack[first_interesting_frame:]
    libby = [ is_interesting_lib(f) for f in signature ]
    try:
        last_interesting_frame = libby.index(True) + 1
    except ValueError as e:
        # No interesting library frames in stack, include them all
        last_interesting_frame = len(libby) - 1

    if last_interesting_frame < MIN_FRAMES:
        last_interesting_frame = MIN_FRAMES

    signature = signature[0:last_interesting_frame]
    boring = [ is_raw(f) or is_js(f) or is_boring(f) for f in signature ]
    # Pop raw addresses, JS frames, and boring stuff from the end
    while boring and boring.pop():
        signature.pop()

    return signature

def load_pings(fin):
    line_num = 0
    while True:
        uuid = fin.read(36)
        if len(uuid) == 0:
            break
        assert len(uuid) == 36
        line_num += 1
        tab = fin.read(1)
        assert tab == '\t'
        jsonstr = fin.readline()
        try:
            json_dict = json.loads(jsonstr)
        except Exception, e:
            print >> sys.stderr, "Error parsing json on line", line_num, ":", e
            continue
        yield line_num, uuid, json_dict

def get_stack_key(stack_entry, memoryMap):
    print "getting stack key for:", stack_entry, memoryMap
    mm_idx, offset = stack_entry
    if mm_idx == -1:
        return None

    if mm_idx < len(memoryMap):
        mm = memoryMap[mm_idx]
        # cache on (dllname, debugid, offset)
        return (mm[0], mm[1], offset)

    return None

def combine_stacks(stacks):
    # Change from
    #   (lib, debugid, offset) => count
    # to
    #   (lib, debugid) => [ offsets ]
    combined = {}
    for lib, debugid, offset in stacks.keys():
        key = (lib, debugid)
        if key not in combined:
            combined[key] = []
        combined[key].append(offset)
    return combined

def process(input_file, output_file, submission_date, include_latewrites=False):
    # 1. First pass, extract (and count) all the unique stack elements
    if input_file == '-':
        fin = sys.stdin
    else:
        fin = open(input_file, "r")

    kinds = ["chromeHangs"]
    if include_latewrites:
        kinds.append("lateWrites")

    start = datetime.now()
    stack_cache = {}
    sys.stderr.write("Extracting unique stack elements...")
    for line_num, uuid, json_dict in load_pings(fin):
        cache_hits = 0
        cache_misses = 0
        for kind in kinds:
            hangs = json_dict.get(kind)
            if hangs:
                for stack in hangs["stacks"]:
                    for stack_entry in stack:
                        key = get_stack_key(stack_entry, hangs["memoryMap"])
                        if key is not None:
                            if key in stack_cache:
                                stack_cache[key] += 1
                            else:
                                stack_cache[key] = 1
    sys.stderr.write("Done after %.2f sec\n" % delta_sec(start))

    # 2. Filter out stack entries with fewer than MIN_HITS occurrences
    if MIN_HITS > 1:
        prev = datetime.now()
        sys.stderr.write("Filtering rare stack elements...")
        to_be_symbolicated = { k: v for k, v in stack_cache.iteritems() if v > MIN_HITS }
        sys.stderr.write("Done after %.2f sec\n" % delta_sec(prev))
    else:
        to_be_symbolicated = stack_cache

    # 3. Change from
    #      (lib, debugid, offset) => count
    #    to
    #      (lib, debugid) => [ offsets ]
    sys.stderr.write("Combining stacks...")
    prev = datetime.now()
    combined_stacks = combine_stacks(to_be_symbolicated)
    sys.stderr.write("Done after %.2f sec\n" % delta_sec(prev))

    # 4. For each library, try to fetch symbols for the given offsets.
    #      (lib, debugid, offset) => symbolicated string
    sys.stderr.write("Looking up stack symbols...")
    prev = datetime.now()
    symbol_cache = symbolicate(combined_stacks)
    sys.stderr.write("Done after %.2f sec\n" % delta_sec(prev))

    # Write out our symbol cache.
    #sys.stderr.write("Writing symbol cache\n")
    #fsym = open("symbol_cache.txt", "w")
    #for k, v in symbol_cache.iteritems():
    #    #a, b, c = k
    #    #fsym.write("\t".join((a, b, c, v)))
    #    fsym.write(json.dumps([k,v]))
    #    fsym.write("\n")
    #fsym.close()

    # 5. Use these symbolicated stacks to symbolicate the actual data
    #    go back to the beginning the input file.
    sys.stderr.write("Symbolicating data...")
    prev = datetime.now()
    fin.seek(0)
    fout = gzip.open(output_file, "wb")
    for line_num, uuid, json_dict in load_pings(fin):
        for kind in kinds:
            hangs = json_dict.get(kind)
            if hangs:
                hangs["stacksSymbolicated"] = []
                hangs["stacksSignatures"] = []
                for stack in hangs["stacks"]:
                    sym = []
                    for stack_entry in stack:
                        key = get_stack_key(stack_entry, hangs["memoryMap"])
                        if key is None:
                            sym.append("-0x1")
                        elif key in symbol_cache:
                            sym.append(symbol_cache[key])
                        else:
                            #print "Missed key:", key
                            # This should only happen if some of our
                            # symbolication requests failed
                            sym.append(hex(stack_entry[1]))
                    hangs["stacksSymbolicated"].append(sym)
                    hangs["stacksSignatures"].append(get_signature(sym))
        fout.write(submission_date)
        fout.write("\t")
        fout.write(uuid)
        fout.write("\t")
        fout.write(min_json(json_dict))
        fout.write("\n")
    sys.stderr.write("Done after %.2f sec\n" % delta_sec(prev))
    fin.close()
    fout.close()
    sys.stderr.write("All done in %.2f sec\n" % delta_sec(start))

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
        try:
            process(input_file, output_file, submission_date)
            return 0
        except Exception as e:
            traceback.print_exc()
            return 1
    except Usage, err:
        print >> sys.stderr, sys.argv[0].split("/")[-1] + ": " + str(err.msg)
        print >> sys.stderr, " for help use --help"
        return 2

if __name__ == "__main__":
    sys.exit(main())
