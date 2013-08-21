import sys, struct, re, os, argparse, zlib
import simplejson as json
from persist import StorageLayout
from telemetry_schema import TelemetrySchema
from datetime import date, datetime
import util.timer as timer

filename_timestamp_pattern = re.compile("^telemetry.log.([0-9]+).([0-9]+)(.finished)?$")

def main():
    parser = argparse.ArgumentParser(description='Split raw logs into partitioned files.', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-m", "--max-output-size", metavar="N", help="Rotate output files after N bytes", type=int, default=500000000)
    parser.add_argument("-i", "--input-file", help="Filename to read from", required=True)
    parser.add_argument("-o", "--output-dir", help="Base directory to store split files", required=True)
    parser.add_argument("-t", "--telemetry-schema", help="Filename of telemetry schema spec", required=True)
    parser.add_argument("-b", "--bucket", help="S3 Bucket name")
    parser.add_argument("-k", "--aws-key", help="AWS Key")
    parser.add_argument("-s", "--aws-secret-key", help="AWS Secret Key")
    args = parser.parse_args()

    schema_data = open(args.telemetry_schema)
    schema = TelemetrySchema(json.load(schema_data))
    schema_data.close()

    storage = StorageLayout(schema, args.output_dir, args.max_output_size)

    expected_dim_count = len(schema._dimensions)

    if not os.path.isdir(args.output_dir):
        os.makedirs(args.output_dir)

    record_count = 0;
    fin = open(args.input_file, "rb")
    m = filename_timestamp_pattern.match(os.path.basename(args.input_file))
    submission_date = date.today().strftime("%Y%m%d")
    if m:
        timestamp = int(m.group(2)) / 1000
        submission_date = date.fromtimestamp(timestamp).strftime("%Y%m%d")

    bytes_read = 0
    start = datetime.now()
    while True:
        record_count += 1
        # Read 2 * 4 bytes
        lengths = fin.read(8)
        if lengths == '':
            break
        len_path, len_data = struct.unpack("II", lengths)
        path = unicode(fin.read(len_path), errors="replace")

        print "Path for record", record_count, path, "length of data:", len_data
        # TODO: detect and handle gzipped data.
        data = fin.read(len_data)
        try:
            data = unicode(zlib.decompress(data, -1), errors="replace")
        except zlib.error, e:
            print e
            data = unicode(data, errors="replace")

        bytes_read += 8 + len_path + len_data

        #print "Path for record", record_count, path, "length of data:", len_data, "data:", data[0:5] + "..."

        path_components = path.split("/")
        if len(path_components) != expected_dim_count:
            # We're going to pop the ID off, but we'll also add the submission,
            # so it evens out.
            print "Found an invalid path in record", record_count, path
            continue

        key = path_components.pop(0)
        info = {}
        info["reason"] = path_components.pop(0)
        info["appName"] = path_components.pop(0)
        info["appVersion"] = path_components.pop(0)
        info["appUpdateChannel"] = path_components.pop(0)
        info["appBuildID"] = path_components.pop(0)
        dimensions = schema.dimensions_from(info, submission_date)
        
        print "  Converted path to filename", schema.get_filename(args.output_dir, dimensions)
        storage.write(key, data, dimensions)
    duration = timer.delta_sec(start)
    mb_read = bytes_read / 1024.0 / 1024.0
    print "Read %.2fMB in %.2fs (%.2fMB/s)" % (mb_read, duration, mb_read / duration)

if __name__ == "__main__":
    sys.exit(main())



