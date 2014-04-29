from boto.s3.connection import S3Connection
from datetime import datetime
import socket
import telemetry.util.timer as timer
import traceback

conn = S3Connection()
bucket = conn.get_bucket("telemetry-published-v1")

last_key = ''
done = False
total_count = 0
start_time = datetime.now()
list_file = open('telemetry_published_v1.20140425.txt', 'w')
while not done:
    try:
        for k in bucket.list(marker=last_key):
            total_count += 1
            if total_count % 5000 == 0:
                print "Looked at", total_count, "total records in", timer.delta_sec(start_time), "seconds. Last key was", k.name
            try:
                list_file.write("{}\n".format(k.name))
                last_key = k.name
            except Exception, e:
                print "Error with key", k.name, ":", e
                traceback.print_exc()
        done = True
    except socket.error, e:
        print "Error listing keys:", e
        traceback.print_exc()
        print "Continuing from last seen key:", last_key
list_file.close()
print "Overall, listed", total_count, "files in", timer.delta_sec(start_time), "seconds."
