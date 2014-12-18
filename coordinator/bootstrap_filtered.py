import argparse
import psycopg2
import socket
import sys
import traceback
from boto.s3.connection import S3Connection
from datetime import datetime
from telemetry.telemetry_schema import TelemetrySchema
import simplejson as json
import telemetry.util.s3 as s3u
import telemetry.util.timer as timer

sql_file_exists = "SELECT count(*) FROM published_files WHERE file_name = %s;"

sql_insert_published_file = '''
INSERT INTO published_files (
 bucket_name        ,
 reason             ,
 app_name           ,
 app_update_channel ,
 app_version        ,
 app_build_id       ,
 submission_date    ,
 log_version        ,
 file_name          ,
 file_size          ,
 file_md5
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
'''


# Update all files on or after submission date.
def update_published_files(conn, spec=None, commit_batch_size=200, verbose=False):
    s3 = S3Connection()
    bucket_name = "telemetry-published-v2"
    bucket = s3.get_bucket(bucket_name)
    schema_key = bucket.get_key("telemetry_schema.json")
    schema_string = schema_key.get_contents_as_string()
    schema = TelemetrySchema(json.loads(schema_string))
    new_count = 0
    total_count = 0
    start_time = datetime.now()
    c = conn.cursor()
    # It's faster just to list everything than it is to do "list_partitions" on
    # such an unselective filter
    done = False
    last_key = ''
    while not done:
        try:
            for k in s3u.list_partitions(bucket, schema=spec, include_keys=True):
                last_key = k.name
                total_count += 1
                if total_count % 5000 == 0:
                    print "Looked at", total_count, "total records in", timer.delta_sec(start_time), "seconds, added", new_count, ". Last key was", k.name
                try:
                    try:
                        dims = schema.get_dimension_map(schema.get_dimensions(".", k.name))
                    except ValueError, e:
                        print "Skipping file with invalid dimensions:", k.name, "-", e
                        continue

                    c.execute(sql_file_exists, (k.name,))
                    result = c.fetchone()
                    if result[0] == 0:
                        print "Adding new file:", k.name
                        insert_one_published_file(schema, bucket_name, k, c, dims)
                        new_count += 1

                    if new_count > 0 and new_count % commit_batch_size == 0:
                        print "Inserted", new_count, "records, last one was", k.name
                        conn.commit()
                except Exception, e:
                    print "Error with key", k.name, ":", e
                    traceback.print_exc()
            done = True
        except socket.error, e:
            print "Error listing keys:", e
            traceback.print_exc()
            print "Continuing from last seen key:", last_key
    conn.commit()
    print "Overall, added", new_count, "of", total_count, "in", timer.delta_sec(start_time), "seconds"

def insert_one_published_file(schema, bucket_name, key, cursor, dims=None):
    if dims is None:
        dims = schema.get_dimension_map(schema.get_dimensions(".", key.name))
    cursor.execute(sql_insert_published_file, (
        bucket_name,
        dims.get("reason"),
        dims.get("appName"),
        dims.get("appUpdateChannel"),
        dims.get("appVersion"),
        dims.get("appBuildID"),
        dims.get("submission_date"),
        "v2", # FIXME: extract this from the filename
        key.name,
        key.size,
        key.etag[1:-1]))


def main():
    parser = argparse.ArgumentParser(description="Populate/update the S3 file cache")
    parser.add_argument("--db-host", default="localhost")
    parser.add_argument("--db-user", default="telemetry")
    parser.add_argument("--db-port", type=int, default=5432)
    parser.add_argument("--db-pass")
    parser.add_argument("--db-name", default="telemetry")
    parser.add_argument("--filter", type=file, required=True)
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    schema = TelemetrySchema(json.load(args.filter))

    connection_string = ""
    if hasattr(args, "db_name"):
        connection_string += "dbname={0} ".format(args.db_name)
    if hasattr(args, "db_host"):
        connection_string += "host={0} ".format(args.db_host)
    if hasattr(args, "db_port"):
        connection_string += "port={0} ".format(args.db_port)
    if hasattr(args, "db_user"):
        connection_string += "user={0} ".format(args.db_user)
    if hasattr(args, "db_pass"):
        connection_string += "password={0} ".format(args.db_pass)

    conn = psycopg2.connect(connection_string)
    c = conn.cursor()

    update_published_files(conn, schema)

    # Save the changes
    conn.commit()

    # All done
    conn.close()

if __name__ == "__main__":
    sys.exit(main())
