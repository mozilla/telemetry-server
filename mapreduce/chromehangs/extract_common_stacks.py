try:
    import simplejson as json
except ImportError:
    import json
import argparse
import csv
import sys
import symbolicate

def stack_to_string(stack):
    return "|".join(stack)

def get_common_stacks(stacks):
    counter = {}
    for stack in stacks:
        key = stack_to_string(stack)
        counter[key] = counter.get(key, 0) + 1
    return counter

def log(message, verbose=True):
    if verbose:
        print message

def get_pings(some_file, parse=True):
    line_num = 0
    for line in some_file:
        submission_date, uuid, payload = line.split("\t", 2)
        line_num += 1
        if parse:
            try:
                payload = json.loads(payload)
            except Exception, e:
                print >> sys.stderr, "Error parsing json on line", line_num, ":", e
                continue
        yield line_num, submission_date, uuid, payload

def safe_arr_get(arr, index, default=None):
    if arr is None:
        return default

    if index < len(arr):
        return arr[index]
    return default

def median(v, already_sorted=False):
    ls = len(v)
    if ls == 0:
        return 0
    if already_sorted:
        s = v
    else:
        s = sorted(v)
    middle = int(ls / 2)
    if ls % 2 == 1:
        return s[middle]
    else:
        return (s[middle] + s[middle-1]) / 2.0

def main(argv=None):
    if argv is None:
        argv = sys.argv[1:]
    parser = argparse.ArgumentParser(description='Extract common stacks')
    parser.add_argument("-i", "--input-file", required=True,
            type=argparse.FileType('r'),
            help="Read input from this file")
    parser.add_argument("-o", "--output-file", required=True,
            type=argparse.FileType('w'),
            help="Write output to this file")
    parser.add_argument("-v", "--verbose", action="store_true",
            help="Print more detailed output")
    args = parser.parse_args(argv)

    stack_tracker = {}
    combined_stacks = {}
    for line, submission_date, uuid, payload in get_pings(args.input_file):
        if line % 5000 == 0:
            log("Processing line {}...".format(line), args.verbose)
        if "chromeHangs" not in payload:
            continue
        hangs = payload["chromeHangs"]
        if "stacksSignatures" not in hangs:
            continue
        sigs = [ symbolicate.get_signature(s) for s in hangs["stacksSymbolicated"] ]
        durations = hangs.get("durations")
        app_uptimes = hangs.get("firefoxUptime")
        sys_uptimes = hangs.get("systemUptime")
        dims = [submission_date]
        for dim in ["appName", "appVersion", "appUpdateChannel"]:
            dims.append(payload.get("info", {}).get(dim, "UNKNOWN"))
        for i in range(len(sigs)):
            sig = sigs[i]
            if sig:
                if len(sig) > 15:
                    print "Found a long signature:", sig
                key = stack_to_string(sig)
                stack_tracker[key] = stack_tracker.get(key, 0) + 1
                duration   = safe_arr_get(durations,   i, -1)
                app_uptime = safe_arr_get(app_uptimes, i, -1)
                sys_uptime = safe_arr_get(sys_uptimes, i, -1)
                # So we want to get the following for each stack:
                # - stack itself
                # - hang duration
                # - firefox uptime
                # - system uptime
                # - filtering dimensions:
                #    - application (should always be "Firefox")
                #    - channel (should always be "nightly")
                #    - version
                combined_key = "\t".join([key] + dims)
                if combined_key not in combined_stacks:
                    combined_stacks[combined_key] = {
                        "durations": [],
                        "firefoxUptime": [],
                        "systemUptime": []
                    }
                combined_stacks[combined_key]["durations"].append(duration)
                combined_stacks[combined_key]["firefoxUptime"].append(app_uptime)
                combined_stacks[combined_key]["systemUptime"].append(sys_uptime)

    log("Found {} total stacks".format(len(stack_tracker.keys())))
    log("Found {} dim+stacks".format(len(combined_stacks.keys())))
    writer = csv.writer(args.output_file)
    writer.writerow([
        "hang_stack",
        "submission_date",
        "app_name",
        "app_version",
        "app_update_channel",
        "ping_count",
        "total_duration",
        "median_duration",
        "app_uptime",
        "system_uptime",
        "submission_date",
        "app_name"
        "app_version",
        "app_update_channel"])
    for k, v in combined_stacks.iteritems():
        writer.writerow(k.split("\t") + [
            len(v["durations"]), sum(v["durations"]), median(v["durations"]),
            median(v["firefoxUptime"]), median(v["systemUptime"])])
    args.output_file.close()
    return 0

if __name__ == "__main__":
    sys.exit(main())
