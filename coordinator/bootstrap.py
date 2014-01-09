import sqlite3
from boto.s3.connection import S3Connection
from telemetry.telemetry_schema import TelemetrySchema
import simplejson as json
import telemetry.util.s3 as s3u

# Update all files on or after submission date.
def update_published_files(conn, submission_date):
    s3 = S3Connection()
    bucket_name = "telemetry-published-v1"
    sql_check = "SELECT count(*) FROM published_files WHERE file_name = ?;"
    bucket = s3.get_bucket(bucket_name)
    schema_key = bucket.get_key("telemetry_schema.json")
    schema_string = schema_key.get_contents_as_string()
    schema_obj = json.loads(schema_string)
    schema = TelemetrySchema(json.loads(schema_string))
    filter_today_obj = json.loads(schema_string)
    for dim in filter_today_obj["dimensions"]:
        if dim["field_name"] == "submission_date":
            dim["allowed_values"] = { "min": submission_date }
        #elif dim["field_name"] == "reason":
        #    # FIXME: just for debugging (much faster):
        #    dim["allowed_values"] = ["android-anr-report"]
        else:
            dim["allowed_values"] = "*"
    filter_today = TelemetrySchema(filter_today_obj)
    new_count = 0
    total_count = 0
    # FIXME: It's faster just to list everything than it is to do "list_partitions"
    for k in s3u.list_partitions(bucket, schema=filter_today, include_keys=True):
        total_count += 1
        try:
            c.execute(sql_check, (k.name,))
            result = c.fetchone()
            if result[0] == 0:
                print "Missing:", k.name
                insert_one_published_file(schema, bucket_name, k, c)
                new_count += 1
                if new_count % 200 == 0:
                    print "Inserted", new_count, "records, last one was", k.name
                    conn.commit()
            else:
                print "Present:", k.name
        except Exception, e:
            print "Error with key", k.name, ":", e
        if total_count % 1000 == 0:
            print "Of", total_count, "total records, added", new_count, ". Last key was", k.name

def populate_published_files(conn):
    s3 = S3Connection()
    bucket_name = "telemetry-published-v1"
    bucket = s3.get_bucket(bucket_name)

    schema_key = bucket.get_key("telemetry_schema.json")
    schema_string = schema_key.get_contents_as_string()
    schema = TelemetrySchema(json.loads(schema_string))
    count = 0
    fails = []

    # Clear out existing records
    c.execute("DELETE FROM published_files")

    # Now fill the tabl
    for k in bucket.list():
        count += 1
        try:
            insert_one_published_file(schema, bucket_name, k, c)
        except Exception, e:
            print "error in record", count, "key was:", k.name, ":", e
            fails.append(k.name)
        if count % 1000 == 0:
            print "Inserted", count, "records, last one was", k.name
            conn.commit()
        #if count > 3000:
        #    break

    conn.commit()

def insert_one_published_file(schema, bucket_name, key, cursor):
    dims = schema.get_dimension_map(schema.get_dimensions(".", key.name))
    bla = c.execute(sql_insert_published_file, (
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


conn = sqlite3.connect('coordinator.db')
c = conn.cursor()

sql_create_table = '''
CREATE TABLE IF NOT EXISTS published_files (
 file_id            INTEGER PRIMARY KEY,
 bucket_name        TEXT        NOT NULL,
 reason             VARCHAR(50) NOT NULL,
 app_name           VARCHAR(50) NOT NULL,
 app_update_channel VARCHAR(50) NOT NULL,
 app_version        VARCHAR(50) NOT NULL,
 app_build_id       VARCHAR(50) NOT NULL,
 submission_date    VARCHAR(8)  NOT NULL,
 log_version        VARCHAR(10) NOT NULL,
 file_name          TEXT,
 file_size          BIGINT      NOT NULL,
 file_md5           TEXT,
 view_count         INT UNSIGNED DEFAULT 0,
 ref_count          INT UNSIGNED DEFAULT 0
);
'''

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
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
'''

sql_file_exists = "SELECT count(*) FROM published_files WHERE file_name = ?;"

# Create table
c.execute(sql_create_table)

c.execute("SELECT max(submission_date) FROM published_files;")

# TODO: this seems to be -1 for the above.
if c.rowcount == 1:
    submission_date = c.fetchone()
    # TODO: update based on a filter that sets min submission_date to ^^
    update_published_files(conn, submission_date)
else:
    # TODO: There aren't any rows in the table, do a mass populate.
    populate_published_files(conn)

# Save the changes
conn.commit()

# All done
conn.close()
