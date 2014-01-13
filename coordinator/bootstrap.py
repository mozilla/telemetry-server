import argparse
import psycopg2
import sys
import traceback
from boto.s3.connection import S3Connection
from datetime import datetime
from telemetry.telemetry_schema import TelemetrySchema
import simplejson as json
import telemetry.util.s3 as s3u
import telemetry.util.timer as timer

# Update all files on or after submission date.
def update_published_files(conn, submission_date):
    s3 = S3Connection()
    bucket_name = "telemetry-published-v1"
    bucket = s3.get_bucket(bucket_name)
    schema_key = bucket.get_key("telemetry_schema.json")
    schema_string = schema_key.get_contents_as_string()
    schema = TelemetrySchema(json.loads(schema_string))
    new_count = 0
    total_count = 0
    start_time = datetime.now()
    # It's faster just to list everything than it is to do "list_partitions" on
    # such an unselective filter
    for k in bucket.list():
        total_count += 1
        if total_count % 1000 == 0:
            print "Looked at", total_count, "total records in", timer.delta_sec(start_time), "seconds, added", new_count, ". Last key was", k.name
        try:
            try:
                dims = schema.get_dimension_map(schema.get_dimensions(".", k.name))
            except ValueError, e:
                print "Skipping file with invalid dimensions:", k.name, "-", e
                continue
            if dims["submission_date"] < submission_date:
                #print "Skipping old file:", k.name
                continue
            c.execute(sql_file_exists, (k.name,))
            result = c.fetchone()
            if result[0] == 0:
                print "Adding new file:", k.name
                insert_one_published_file(schema, bucket_name, k, c, dims)
                new_count += 1
                if new_count % 200 == 0:
                    print "Inserted", new_count, "records, last one was", k.name
                    conn.commit()
            else:
                print "Skipping new but already present file:", k.name
        except Exception, e:
            print "Error with key", k.name, ":", e
            traceback.print_exc()
    conn.commit()
    print "Overall, added", new_count, "of", total_count, "in", timer.delta_sec(start_time), "seconds"

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

    start_time = datetime.now()
    # Now fill the table
    for k in bucket.list():
        count += 1
        try:
            insert_one_published_file(schema, bucket_name, k, c)
        except Exception, e:
            print "error in record", count, "key was:", k.name, ":", e
            fails.append(k.name)
        if count % 1000 == 0:
            print "Inserted", count, "records in", timer.delta_sec(start_time), "seconds, last one was", k.name
            conn.commit()
    conn.commit()

def insert_one_published_file(schema, bucket_name, key, cursor, dims=None):
    if dims is None:
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


def main():
    parser = argparse.ArgumentParser(description="Populate/update the S3 file cache")
    parser.add_argument("--db-host")
    parser.add_argument("--db-user")
    parser.add_argument("--db-port", type=int, default=5432)
    parser.add_argument("--db-pass")
    parser.add_argument("--db-name")
    parser.add_argument("--create-indexes", action="store_true")
    args = parser.parse_args()

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

    conn = psycopg2.connect(connection_string.strip())
    # TODO: PRAGMA foreign_keys = ON;
    c = conn.cursor()

    sql_create_published_files = '''
    CREATE TABLE IF NOT EXISTS published_files (
     file_id            SERIAL PRIMARY KEY,
     bucket_name        TEXT        NOT NULL,
     reason             VARCHAR(50) NOT NULL,
     app_name           VARCHAR(50) NOT NULL,
     app_update_channel VARCHAR(50) NOT NULL,
     app_version        VARCHAR(50) NOT NULL,
     app_build_id       VARCHAR(50) NOT NULL,
     submission_date    VARCHAR(8)  NOT NULL,
     log_version        VARCHAR(10) NOT NULL,
     file_name          TEXT UNIQUE NOT NULL,
     file_size          BIGINT      NOT NULL,
     file_md5           TEXT,
     view_count         INT DEFAULT 0,
     ref_count          INT DEFAULT 0
    );
    '''
    sql_create_tasks = '''
    CREATE TABLE IF NOT EXISTS tasks (
     task_id            SERIAL PRIMARY KEY,
     name               TEXT        NOT NULL,
     code_uri           TEXT        NOT NULL,
     owner_email        VARCHAR(100) NOT NULL,
     status             VARCHAR(20) NOT NULL DEFAULT 'pending',
     claimed_until      TIMESTAMP WITH TIME ZONE, -- NULL means not claimed yet.
     retries_remaining  INT DEFAULT 5
    );
    '''
    sql_create_task_files = '''
    CREATE TABLE IF NOT EXISTS task_files (
     task_id            INTEGER NOT NULL REFERENCES tasks(task_id),
     file_id            INTEGER NOT NULL REFERENCES published_files(file_id)
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

    unique_indexed_fields = ["file_name"]
    #sql_create_file_name_index          = "CREATE UNIQUE INDEX published_files_file_name_idx ON published_files (file_name);"

    indexed_fields = ["reason", "app_name", "app_update_channel", "app_version", "app_build_id", "submission_date"]
    #sql_create_reason_index             = "CREATE INDEX published_files_reason_idx ON published_files (reason);"
    #sql_create_app_name_index           = "CREATE INDEX published_files_app_name_idx ON published_files (app_name);"
    #sql_create_app_update_channel_index = "CREATE INDEX published_files_app_update_channel_idx ON published_files (app_update_channel);"
    #sql_create_app_version_index        = "CREATE INDEX published_files_app_version_idx ON published_files (app_version);"
    #sql_create_app_build_id_index       = "CREATE INDEX published_files_app_build_id_idx ON published_files (app_build_id);"
    #sql_create_submission_date_index    = "CREATE INDEX published_files_submission_date_idx ON published_files (submission_date);"

    # Create table
    print "Creating table (if needed)..."
    c.execute(sql_create_published_files)
    c.execute(sql_create_tasks)
    c.execute(sql_create_task_files)

    if args.create_indexes:
        for field in indexed_fields:
            print "Creating index for", field
            c.execute("CREATE INDEX published_files_{0}_idx ON published_files ({0});".format(field))


    print "Checking if table is empty..."
    c.execute("SELECT count(*) FROM published_files;");
    countrow = c.fetchone();
    if countrow[0] == 0:
        print "Table was empty, populating from scratch"
        populate_published_files(conn)
    else:
        print "Table was not empty (contained", countrow[0], "rows). Checking last update date..."
        c.execute("SELECT max(submission_date) FROM published_files;")
        submissionrow = c.fetchone()
        submission_date = submissionrow[0]
        print "Updating everything on or after", submission_date
        update_published_files(conn, submission_date)

    # Save the changes
    conn.commit()

    # All done
    conn.close()

if __name__ == "__main__":
    sys.exit(main())
