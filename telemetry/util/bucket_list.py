import argparse
import socket
import sys
import traceback
import boto
from boto.s3.connection import S3Connection
from datetime import datetime
import telemetry.util.timer as timer

def s3obj_to_string(key):
    return u"\t".join((key.name, str(key.size), key.etag[1:-1]))

# Update all files on or after submission date.
def list_files(bucket_name, output_file, output_func=s3obj_to_string, prefix=''):
    s3 = S3Connection()
    bucket = s3.get_bucket(bucket_name)
    total_count = 0
    start_time = datetime.now()
    done = False
    last_key = ''
    while not done:
        try:
            for k in bucket.list(prefix=prefix, marker=last_key):
                last_key = k.name
                total_count += 1
                if total_count % 5000 == 0:
                    print "Looked at", total_count, "total records in", timer.delta_sec(start_time), "seconds. Last key was", last_key
                try:
                    output_file.write(str(output_func(k)) + "\n")
                except Exception, e:
                    print "Error writing key", k.name, ":", e
                    traceback.print_exc()
            done = True
        except socket.error, e:
            print "Error listing keys:", e
            traceback.print_exc()
            print "Continuing from last seen key:", last_key

    output_file.close()
    print "Overall, listed", total_count, "in", timer.delta_sec(start_time), "seconds"

def main():
    parser = argparse.ArgumentParser(description="List S3 contents (with retry) to a file")
    parser.add_argument("--output-file", type=argparse.FileType('w'))
    parser.add_argument("--bucket", default="telemetry-published-v2")
    parser.add_argument("--prefix", default="")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    
    if args.debug:
        boto.set_stream_logger('boto')

    list_files(args.bucket, args.output_file, prefix=args.prefix)

if __name__ == "__main__":
    sys.exit(main())
