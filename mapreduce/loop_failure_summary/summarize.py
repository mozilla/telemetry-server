import argparse
import fileinput
import json
import sys

def main():
    parser = argparse.ArgumentParser(description='Summarize daily loop failures.')
    parser.add_argument("-i", "--input-file", help="Filename to read from", required=True, type=file)
    parser.add_argument("-o", "--summary-output", help="Filename to save day's data", required=True, type=argparse.FileType('w'))
    parser.add_argument("-c", "--combined-input", help="Filename to read combined daily data", type=file)
    parser.add_argument("-O", "--combined-output", help="Filename to save combined daily data", required=True, type=argparse.FileType('w'))
    args = parser.parse_args()

    headers = None
    date_idx = -1
    err_idx = -1
    date_map = {}
    for line in args.input_file:
        fields = line.split("\t")
        if headers is None:
            headers = fields
            try:
                date_idx = headers.index("submission_date")
                err_idx = headers.index("failure_type")
            except ValueError as e:
                print "Error: required field missing. We need 'submission_date' " \
                      "and 'failure_type' to generate a summary"
                return 2
        else:
            submission_date = fields[date_idx]
            failure_type = fields[err_idx]
            if submission_date not in date_map:
                date_map[submission_date] = {}

            if failure_type not in date_map[submission_date]:
                date_map[submission_date][failure_type] = 1
            else:
                date_map[submission_date][failure_type] += 1

    json.dump(date_map, args.summary_output)
    try:
        combined = json.load(args.combined_input)
    except:
        combined = []

    current_index = 0
    # Insert each date into the correct spot in the array.
    for d in sorted(date_map.keys()):
        date_map[d]["date"] = d
        while current_index < len(combined) and d > combined[current_index]["date"]:
            current_index += 1

        # if the date is already there, overwrite with new values
        if len(combined) > current_index and combined[current_index]["date"] == d:
            for k in date_map[d].keys():
                combined[current_index][k] = date_map[d][k]
        else:
            combined.insert(current_index, date_map[d])
        # Output last 180 days
        json.dump(combined[-180:], args.combined_output)
    return 0

if __name__ == "__main__":
    sys.exit(main())
