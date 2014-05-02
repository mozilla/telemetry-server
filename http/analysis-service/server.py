#!/usr/bin/env python

from argparse import ArgumentParser
from flask import Flask, render_template, g, request, redirect, url_for
from flask.ext.login import LoginManager, login_required, current_user
from flask.ext.browserid import BrowserID
from user import User, AnonymousUser
from boto.ec2 import connect_to_region as ec2_connect
from boto.ses import connect_to_region as ses_connect
from boto.s3 import connect_to_region as s3_connect
from boto.ec2.blockdevicemapping import BlockDeviceType
from boto.ec2.blockdevicemapping import BlockDeviceMapping
from urlparse import urljoin
from uuid import uuid4
from sqlalchemy import create_engine, MetaData
from sqlalchemy.sql import select, func
from subprocess import check_output, CalledProcessError
from tempfile import mkstemp
import crontab
import json

# Create flask app
app = Flask(__name__)
app.config.from_object('config')

# Connect to AWS
ec2 = ec2_connect(app.config['AWS_REGION'])
ses = ses_connect('us-east-1') # only supported region!
s3  = s3_connect(app.config['AWS_REGION'])
bucket = s3.get_bucket(app.config['TEMPORARY_BUCKET'], validate = False)
code_bucket = s3.get_bucket(app.config['CODE_BUCKET'], validate = False)

# Create login manager
login_manager = LoginManager()
login_manager.anonymous_user = AnonymousUser

# Initialize browser id login
browser_id = BrowserID()

# Cron-related constants:
CRON_IDX_MIN  = 0
CRON_IDX_HOUR = 1
CRON_IDX_DOM  = 2
CRON_IDX_MON  = 3
CRON_IDX_DOW  = 4
CRON_IDX_CMD  = 5

def connect_db(db_url=None):
    if db_url is None:
        db_url = app.config['DB_URL']
    db = {}
    db['engine'] = create_engine(db_url)
    db['metadata'] = MetaData(bind=db['engine'])
    db['metadata'].reflect()
    initialize_db(db)
    db['conn'] = db['engine'].connect()
    return db

def initialize_db(db):
    from sqlalchemy import Table, Column, Integer, String, Sequence
    if 'scheduled_jobs' in db['metadata'].tables:
        # Table already exists. Nothing to do.
        return

    scheduled_jobs = Table('scheduled_jobs', db['metadata'],
        Column("id",                    Integer,
            Sequence('scheduled_jobs_id_seq', start=1000), primary_key=True),
        Column("owner",                 String(50),  nullable=False, index=True),
        Column("name",                  String(100), nullable=False, unique=True),
        Column("timeout_minutes",       Integer,     nullable=False),
        Column("code_uri",              String(300), nullable=False),
        Column("commandline",           String,      nullable=False),
        Column("data_bucket",           String(200), nullable=False),
        Column("output_dir",            String(100), nullable=False),
        Column("schedule_minute",       String(20),  nullable=False),
        Column("schedule_hour",         String(20),  nullable=False),
        Column("schedule_day_of_month", String(20),  nullable=False),
        Column("schedule_month",        String(20),  nullable=False),
        Column("schedule_day_of_week",  String(20),  nullable=False)
    )
    # Create the table
    db['metadata'].create_all(tables=[scheduled_jobs])
    # TODO: The above does not create the serial column properly in PostgreSQL.
    #       Use this SQL:
    # CREATE TABLE scheduled_jobs (
    #     id                    SERIAL PRIMARY KEY,
    #     owner                 VARCHAR(50) NOT NULL,
    #     name                  VARCHAR(100) UNIQUE NOT NULL,
    #     timeout_minutes       INT NOT NULL,
    #     code_uri              VARCHAR(300) NOT NULL,
    #     commandline           VARCHAR NOT NULL,
    #     data_bucket           VARCHAR(200) NOT NULL,
    #     output_dir            VARCHAR(100) NOT NULL,
    #     schedule_minute       VARCHAR(20) NOT NULL,
    #     schedule_hour         VARCHAR(20) NOT NULL,
    #     schedule_day_of_month VARCHAR(20) NOT NULL,
    #     schedule_month        VARCHAR(20) NOT NULL,
    #     schedule_day_of_week  VARCHAR(20) NOT NULL
    # );
    # -- Make job id start from 1000
    # ALTER SEQUENCE scheduled_jobs_id_seq RESTART WITH 1000;
    # CREATE INDEX scheduled_jobs_owner_idx on scheduled_jobs(owner);

def get_db():
    """Opens a new database connection if there is none yet for the
    current application context.
    """
    if not hasattr(g, 'database'):
        g.database = connect_db()
    return g.database

def insert_job(job):
    db = get_db()
    table = db['metadata'].tables['scheduled_jobs']
    insert = table.insert().values(job)
    return db['conn'].execute(insert)

def update_job(job):
    db = get_db()
    table = db['metadata'].tables['scheduled_jobs']
    update = table.update().where(table.c.id == job['id']).values(job)
    return db['conn'].execute(update)

def get_jobs(owner=None):
    db = get_db()
    table = db['metadata'].tables['scheduled_jobs']
    if owner is None:
        # Get all jobs
        query = select([table]).order_by(table.c.id)
    else:
        # Get jobs for the given owner
        query = select([table]).where(table.c.owner == owner).order_by(table.c.id)
    result = db['conn'].execute(query)
    try:
        for row in result:
            yield row
    except:
        result.close()
        raise
    result.close()

def get_job_logs(job):
    # Sort logs in descending order (oldest first)
    return sorted(get_job_files(job, "logs"), key=lambda f: f["url"], reverse=True)

def get_job_files(job, path_snippet):
    job_files = []
    try:
        # find the S3 bucket
        data_bucket = s3.get_bucket(job.data_bucket, validate = False)
        if data_bucket:
            # calculate the log path
            s3path = "{0}/{1}/".format(job.name, path_snippet)
            urlbase = "https://s3-us-west-2.amazonaws.com"
            urlprefixlen = len(urlbase) + len(job.data_bucket) + len(s3path) + 2
            for key in data_bucket.list(prefix=s3path):
                url = "{0}/{1}/{2}".format(urlbase, job.data_bucket, key.name)
                title = url[urlprefixlen:]
                job_files.append({"url": url, "title": title})
    except Exception, e:
        job_files.append({"url": "#", "title": "Error fetching job files: {}".format(e)})
    return job_files

def get_job(name=None, job_id=None):
    db = get_db()
    table = db['metadata'].tables['scheduled_jobs']
    if name is not None:
        query = select([table]).where(table.c.name == name)
    elif job_id is not None:
        query = select([table]).where(table.c.id == job_id)
    result = db['conn'].execute(query)
    job = result.fetchone()
    result.close()
    return job

def delete_job(job_id, owner):
    db = get_db()
    table = db['metadata'].tables['scheduled_jobs']
    result = db['conn'].execute(table.delete().where(table.c.id == job_id).where(table.c.owner == owner))
    return result

def job_exists(name=None, job_id=None):

    db = get_db()
    table = db['metadata'].tables['scheduled_jobs']
    if name is not None:
        query = select([func.count(table.c.id)]).where(table.c.name == name)
    elif job_id is not None:
        query = select([func.count(table.c.id)]).where(table.c.id == job_id)
    result = db['conn'].execute(query)
    count = result.fetchone()[0]
    result.close()
    if count > 0:
        return True
    return False

def update_configs(jobs=None):
    if jobs is None:
        jobs = []
        for j in get_jobs():
            jobs.append(j)
    for job in jobs:
        config = {
            "ssl_key_name": "mreid",
            "base_dir": "/mnt/telemetry",
            # TODO: move this to config.py
            "instance_type": "c3.2xlarge",
            "image": "ami-0e98fe3e",
            # TODO: ssh-only securiry group
            "security_groups": ["telemetry"],
            "iam_role": "telemetry-public-analysis-worker",
            "region": app.config['AWS_REGION'],
            "shutdown_behavior": "terminate",
            "name": "telemetry-analysis-{0}".format(job['name']),
            "default_tags": {
              "Owner": "mreid",
              "Application": "telemetry-server"
            },
            "ephemeral_map": {
              "/dev/xvdb": "ephemeral0",
              "/dev/xvdc": "ephemeral1"
            },
            "skip_ssh": True,
            "skip_bootstrap": True,
            "job_name": job['name'],
            "job_owner": job['owner'],
            "job_timeout_minutes": job['timeout_minutes'],
            "job_code_uri": job['code_uri'],
            "job_commandline": job['commandline'],
            "job_data_bucket": job['data_bucket'],
            "job_output_dir": job['output_dir']
        }
        #FIXME: find a better way than embedding the path all over the place.
        filename = "/home/ubuntu/telemetry_analysis/jobs/{0}.json".format(job['id'])
        if app.config['DEBUG']:
            print "Debug mode, would have written config to", filename
            print json.dumps(config)
        else:
            with open(filename, 'w') as f:
                json.dump(config, f)

def update_crontab(jobs=None):
    if jobs is None:
        jobs = []
        for job in get_jobs():
            jobs.append(job)
    new_crontab = crontab.update_crontab(jobs)
    if app.config['DEBUG']:
        print "Debug mode, would have updated crontab to:"
        print new_crontab
    else:
        crontab.save_crontab(new_crontab)

def upload_code(job_name, code_file):
    try:
        code_key = code_bucket.new_key("jobs/{0}/{1}".format(job_name, code_file.filename))
        code_key.set_contents_from_file(code_file)
    except Exception, e:
        return e.message
    return None

def get_required_int(request, field, label, min_value=0, max_value=100):
    value = request.form[field]
    if value is None or value.strip() == '':
        raise ValueError(label + " is required")
    else:
        try:
            value = int(value)
            if value < min_value or value > max_value:
                raise ValueError("{0} should be between {1} and {2}".format(label, min_value, max_value))
        except ValueError:
            raise ValueError("{0} should be an int between {1} and {2}".format(label, min_value, max_value))
    return value

def hour_to_time(hour):
    return "{0}:00 UTC".format(hour)

def display_dow(dow):
    if dow is None:
        return ''

    dayname = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"][dow % 7]
    return " every {0}".format(dayname)

def display_dom(dom):
    if dom is None:
        return ''
    nth = "{0}th".format(dom)
    if dom % 10 == 1:
        nth = "{0}st".format(dom)
    elif dom % 10 == 2:
        nth = "{0}nd".format(dom)
    elif dom % 10 == 3:
        nth = "{0}rd".format(dom)
    return " on the {0} day of each month".format(nth)

def job_to_form(job):
    form = {}
    field_map = {
        "name": "job-name",
        "id": "job-id",
        "timeout_minutes": "timeout",
        "owner": "job-owner",
        "code_uri": "code-uri",
        "commandline": "commandline",
        "data-bucket": "data-bucket",
        "output_dir": "output-dir",
        "schedule_hour": "schedule-time-of-day"
    }

    for k, v in field_map.iteritems():
        if k in job:
            form[v] = job[k]

    dow = job["schedule_day_of_week"]
    if dow != '*':
        form['schedule-frequency'] = 'weekly'
        form['schedule-day-of-week'] = dow
    dom = job["schedule_day_of_month"]
    if dom != '*':
        form['schedule-frequency'] = 'monthly'
        form['schedule-day-of-month'] = dom

    if dow == '*' and dom == '*':
        form['schedule-frequency'] = 'daily'
    return form

@app.before_first_request
def initialize_jobs():
    # We want to make sure that the server starts off with a full set
    # of config files and crontab entries. This will NOT be executed until
    # a request hits the server. The Load Balancer will hit the '/status'
    # endpoint frequently, so that should do the trick.
    update_configs()
    update_crontab()

@app.teardown_appcontext
def close_db(error):
    """Closes the database again at the end of the request."""
    if hasattr(g, 'database'):
        g.database['conn'].close()

def abs_url_for(rule, **options):
    return urljoin(request.url_root, url_for(rule, **options))

@browser_id.user_loader
def get_user(response):
    """Create User from BrowserID response"""
    if response['status'] == 'okay':
        return User(response['email'])
    return User(None)

@login_manager.user_loader
def load_user(email):
    """Create user from already authenticated email"""
    return User(email)

@login_manager.unauthorized_handler
def unauthorized():
    return render_template('index.html')

# Routes
@app.route('/', methods=["GET"])
def index():
    return render_template('index.html')

@app.route("/schedule", methods=["GET"])
@login_required
def schedule_job(errors=None, values=None):
    # Check that the user logged in is also authorized to do this
    if not current_user.is_authorized():
        return login_manager.unauthorized()

    jobs = []
    for job in get_jobs(current_user.email):
        jobs.append(job)

    return render_template('schedule.html', jobs=jobs, errors=errors, values=values)

@app.route("/schedule/new", methods=["POST"])
@login_required
def create_scheduled_job():
    # Check that the user logged in is also authorized to do this
    if not current_user.is_authorized():
        return login_manager.unauthorized()

    errors = {}
    for f in ['job-name', 'commandline', 'output-dir',
              'schedule-frequency', 'schedule-time-of-day', 'timeout']:
        val = request.form[f]
        if val is None or val.strip() == '':
            errors[f] = "This field is required"

    time_of_day = -1
    try:
        time_of_day = get_required_int(request, 'schedule-time-of-day',
                "Time of Day", max_value=23)
    except ValueError, e:
        errors['schedule-time-of-day'] = e.message

    frequency = request.form['schedule-frequency'].strip()
    # m h  dom mon dow   command
    cron_bits = [0, time_of_day]
    day_of_week = None
    day_of_month = None
    if frequency == 'weekly':
        # day of week is required
        try:
            day_of_week = get_required_int(request, 'schedule-day-of-week',
                    "Day of Week", max_value=6)
            cron_bits.extend(['*', '*', day_of_week])
        except ValueError, e:
            errors['schedule-day-of-week'] = e.message
    elif frequency == 'monthly':
        # day of month is required
        try:
            day_of_month = get_required_int(request, 'schedule-day-of-month',
                    "Day of Month", max_value=31)
            cron_bits.extend([day_of_month, '*', '*'])
        except ValueError, e:
            errors['schedule-day-of-month'] = e.message
    elif frequency != 'daily':
        # incoming value is bogus.
        errors['schedule-frequency'] = "Pick one of the values in the list"
    else:
        cron_bits.extend(['*', '*', '*'])

    try:
        timeout = get_required_int(request, 'timeout',
                "Job Timeout", max_value=24*60)
    except ValueError, e:
        errors['timeout'] = e.message

    # Check for code-tarball
    if request.files['code-tarball']:
        filename = request.files['code-tarball'].filename
        if not (filename.endswith(".tar.gz") or filename.endswith(".tgz")):
            errors['code-tarball'] = "Code file must be in .tar.gz or .tgz format"
    else:
        errors['code-tarball'] = "File is required (.tar.gz or .tgz)"

    # Check if job_name is already in use
    if job_exists(name=request.form['job-name']):
        errors['job-name'] = "The name '{}' is already in use. Choose another name.".format(request.form['job-name'])

    # If there were any errors, stop and re-display the form.
    # It's only polite to render the form with the previously-supplied
    # values filled in. Unfortunately doing so for files doesn't seem to be
    # worth the effort.
    if errors:
        return schedule_job(errors, request.form)

    err = upload_code(request.form["job-name"], request.files["code-tarball"])
    if err is not None:
        errors["code-tarball"] = err
        return schedule_job(errors, request.form)

    # Now do it!
    code_s3path = "s3://{0}/jobs/{1}/{2}".format(app.config['CODE_BUCKET'],
        request.form["job-name"], request.files["code-tarball"].filename)
    data_s3path = "s3://{0}/{1}/data/".format(app.config['PUBLIC_DATA_BUCKET'],
        request.form["job-name"])

    job = {
        "owner": current_user.email,
        "name": request.form['job-name'],
        "timeout_minutes": timeout,
        "code_uri": code_s3path,
        "commandline": request.form['commandline'],
        "data_bucket": app.config['PUBLIC_DATA_BUCKET'],
        "output_dir": request.form['output-dir'],
        "schedule_minute": cron_bits[CRON_IDX_MIN],
        "schedule_hour": cron_bits[CRON_IDX_HOUR],
        "schedule_day_of_month": cron_bits[CRON_IDX_DOM],
        "schedule_month": cron_bits[CRON_IDX_MON],
        "schedule_day_of_week": cron_bits[CRON_IDX_DOW]
    }

    result = insert_job(job)
    if result.inserted_primary_key > 0:
        print "Inserted job id", result.inserted_primary_key
        jobs = []
        for j in get_jobs():
            jobs.append(j)
        update_configs(jobs)
        update_crontab(jobs)

    # Last bit is the command to execute.
    # This is just a placeholder - the real command is added when we update
    # the crontab.
    cron_bits.append("jobs/run.sh '{0}'".format(result.inserted_primary_key))

    return render_template('schedule_create.html',
        result = result,
        code_s3path = code_s3path,
        data_s3path = data_s3path,
        commandline = request.form['commandline'],
        output_dir = request.form['output-dir'],
        job_frequency = frequency,
        job_time = hour_to_time(time_of_day),
        job_dow = display_dow(day_of_week),
        job_dom = display_dom(day_of_month),
        job_timeout = timeout,
        cron_spec = " ".join([str(c) for c in cron_bits])
    )

@app.route("/schedule/edit/<job_id>", methods=["GET","POST"])
@login_required
def edit_scheduled_job(job_id):
    # Check that the user logged in is also authorized to do this
    if not current_user.is_authorized():
        return login_manager.unauthorized()

    if request.method != 'GET' and request.method != 'POST':
        return "Unsupported method: {0}".format(request.method), 405

    job = get_job(job_id=job_id)
    if job is None:
        return "No such job {0}".format(job_id), 404
    elif job['owner'] != current_user.email:
        return "Can't edit job {0}".format(job_id), 401

    if request.method == 'GET':
        # Show job details.
        return render_template("schedule.html", values=job_to_form(job))
    # else: request.method == 'POST'
    # update this job's details
    if request.form['job-id'] != job_id:
        return "Mismatched job id", 400

    errors = {}
    for f in ['job-name', 'commandline', 'output-dir',
              'schedule-frequency', 'schedule-time-of-day', 'timeout']:
        val = request.form[f]
        if val is None or val.strip() == '':
            errors[f] = "This field is required"

    time_of_day = -1
    try:
        time_of_day = get_required_int(request, 'schedule-time-of-day',
                "Time of Day", max_value=23)
    except ValueError, e:
        errors['schedule-time-of-day'] = e.message

    frequency = request.form['schedule-frequency'].strip()
    # m h  dom mon dow   command
    cron_bits = [0, time_of_day]
    day_of_week = None
    day_of_month = None
    if frequency == 'weekly':
        # day of week is required
        try:
            day_of_week = get_required_int(request, 'schedule-day-of-week',
                    "Day of Week", max_value=6)
            cron_bits.extend(['*', '*', day_of_week])
        except ValueError, e:
            errors['schedule-day-of-week'] = e.message
    elif frequency == 'monthly':
        # day of month is required
        try:
            day_of_month = get_required_int(request, 'schedule-day-of-month',
                    "Day of Month", max_value=31)
            cron_bits.extend([day_of_month, '*', '*'])
        except ValueError, e:
            errors['schedule-day-of-month'] = e.message
    elif frequency != 'daily':
        # incoming value is bogus.
        errors['schedule-frequency'] = "Pick one of the values in the list"
    else:
        cron_bits.extend(['*', '*', '*'])

    try:
        timeout = get_required_int(request, 'timeout',
                "Job Timeout", max_value=24*60)
    except ValueError, e:
        errors['timeout'] = e.message

    # You may update code-tarball *or* code-uri, but not both. Otherwise, what
    # should we do with the other one?
    if request.files['code-tarball']:
        filename = request.files['code-tarball'].filename
        if not (filename.endswith(".tar.gz") or filename.endswith(".tgz")):
            errors['code-tarball'] = "Code file must be in .tar.gz or .tgz format"
        if request.form['code-uri'] and request.form['code-uri'] != job['code_uri']:
            errors['code-uri'] = "Cannot change code-uri and upload a new Code Tarball at the same time"
    elif request.form['code-uri']:
        if request.form['code-uri'] != job['code_uri']:
            # Check if the Code URI exists within the expected bucket.
            if request.form['code-uri'].startswith("s3://" + app.config['CODE_BUCKET'] + "/jobs/"):
                code_key_path = request.form['code-uri'][len(app.config['CODE_BUCKET']) + 6:]
                try:
                    code_key = code_bucket.get_key(code_key_path)
                    if code_key is None or not code_key.exists():
                        errors['code-uri'] = "Specified Code URI does not exist"
                except Exception, e:
                    errors['code-uri'] = e.message
            else:
                errors['code-uri'] = "Code URI must begin with 's3://{0}/jobs/'".format(app.config['CODE_BUCKET'])
    else:
        # Also, they can't both be missing, otherwise we have no job code.
        errors['code-tarball'] = "Code Tarball or S3 Code URI is required (.tar.gz or .tgz)"
        errors['code-uri'] = errors['code-tarball']

    if request.form['job-name'] != job['name']:
        errors['job-name'] = "Don't change the job name, that is confusing. " \
            "Should be '{}'. To change a job name, delete and " \
            "recreate it.".format(job['name'])

    # If there were any errors, stop and re-display the form.
    if errors:
        return render_template("schedule.html", values=request.form, errors=errors)

    if request.files['code-tarball']:
        err = upload_code(request.form["job-name"], request.files["code-tarball"])
        if err is not None:
            errors["code-tarball"] = err
            return render_template("schedule.html", values=request.form, errors=errors)
        code_s3path = "s3://{0}/jobs/{1}/{2}".format(app.config['CODE_BUCKET'],
            request.form["job-name"], request.files["code-tarball"].filename)
    else:
        code_s3path = request.form['code-uri']

    data_s3path = "s3://{0}/{1}/data/".format(app.config['PUBLIC_DATA_BUCKET'],
        request.form["job-name"])

    job = {
        "id": job_id,
        "owner": current_user.email,
        "name": request.form['job-name'],
        "timeout_minutes": timeout,
        "code_uri": code_s3path,
        "commandline": request.form['commandline'],
        "data_bucket": app.config['PUBLIC_DATA_BUCKET'],
        "output_dir": request.form['output-dir'],
        "schedule_minute": cron_bits[CRON_IDX_MIN],
        "schedule_hour": cron_bits[CRON_IDX_HOUR],
        "schedule_day_of_month": cron_bits[CRON_IDX_DOM],
        "schedule_month": cron_bits[CRON_IDX_MON],
        "schedule_day_of_week": cron_bits[CRON_IDX_DOW]
    }

    result = update_job(job)

    if result.rowcount > 0:
        print "Updated job id", job_id
        jobs = []
        for j in get_jobs():
            jobs.append(j)
        update_configs(jobs)
        update_crontab(jobs)

    # Last bit is the command to execute.
    # This is just a placeholder - the real command is added when we update
    # the crontab.
    cron_bits.append("jobs/run.sh '{0}'".format(job_id))

    return render_template('schedule_create.html',
        result = result,
        code_s3path = code_s3path,
        data_s3path = data_s3path,
        commandline = request.form['commandline'],
        output_dir = request.form['output-dir'],
        job_frequency = frequency,
        job_time = hour_to_time(time_of_day),
        job_dow = display_dow(day_of_week),
        job_dom = display_dom(day_of_month),
        job_timeout = timeout,
        cron_spec = " ".join([str(c) for c in cron_bits])
    )

@app.route("/schedule/delete/<job_id>", methods=["POST","GET","DELETE"])
@login_required
def delete_scheduled_job(job_id):
    # Check that the user logged in is also authorized to do this
    if not current_user.is_authorized():
        return login_manager.unauthorized()

    job = get_job(job_id=job_id)
    if job is None:
        return "No such job {0}".format(job_id), 404
    elif job['owner'] == current_user.email:
        # OK, this job is yours. let's delete it.
        result = delete_job(job_id, current_user.email)
        if result.rowcount == 1:
            # We don't have to update the configs, though maybe we should
            # delete this job's config to clean up.
            update_crontab()
        return render_template('schedule_delete.html', result=result, job=job)
    return "Can't delete job {0}".format(job_id), 401

@app.route("/schedule/logs/<job_id>", methods=["GET"])
@login_required
def view_job_logs(job_id):
    # Check that the user logged in is also authorized to do this
    if not current_user.is_authorized():
        return login_manager.unauthorized()

    job = get_job(job_id=job_id)
    if job is None:
        return "No such job {0}".format(job_id), 404
    elif job['owner'] == current_user.email:
        # OK, this job is yours. Time to dig up the logs.
        logs = get_job_logs(job)

        # TODO: Add a "<delete>" link
        #       Add a "<delete all logs>" link
        return render_template('schedule_files.html', name="log", files=logs, job=job)
    return "Can't view logs for job {0}".format(job_id), 401

@app.route("/schedule/data/<job_id>", methods=["GET"])
@login_required
def view_job_data(job_id):
    # Check that the user logged in is also authorized to do this
    if not current_user.is_authorized():
        return login_manager.unauthorized()

    job = get_job(job_id=job_id)
    if job is None:
        return "No such job {0}".format(job_id), 404
    elif job['owner'] == current_user.email:
        # OK, this job is yours. Time to dig up the logs.
        files = get_job_files(job, "data")
        return render_template('schedule_files.html', name="data", files=files, job=job)
    return "Can't view data for job {0}".format(job_id), 401

@app.route("/worker", methods=["GET"])
@login_required
def get_worker_params(errors=None, values=None):
    # Check that the user logged in is also authorized to do this
    if not current_user.is_authorized():
        return login_manager.unauthorized()
    return render_template('worker.html', errors=errors, values=values,
        token=str(uuid4()))

@app.route("/worker/new", methods=["POST"])
@login_required
def spawn_worker_instance():
    # Check that the user logged in is also authorized to do this
    if not current_user.is_authorized():
        return login_manager.unauthorized()

    errors = {}

    # Check required fields
    for f in ['name', 'token']:
        val = request.form[f]
        if val is None or val.strip() == '':
            errors[f] = "This field is required"

    # Check required file
    if not request.files['public-ssh-key']:
        errors['code-tarball'] = "Public key file is required"

    # Bug 961200: Check that a proper OpenSSH public key was uploaded.
    # It should start with "ssh-rsa AAAAB3"
    pubkey = request.files['public-ssh-key'].read()
    if not pubkey.startswith("ssh-rsa AAAAB3"):
        errors['public-ssh-key'] = "Supplied file does not appear to be a valid OpenSSH public key."

    if errors:
        return get_worker_params(errors, request.form)

    # Upload s3 key to bucket
    sshkey = bucket.new_key("keys/%s.pub" % request.form['token'])
    sshkey.set_contents_from_string(pubkey)

    ephemeral = app.config.get("EPHEMERAL_MAP", None)
    # Create
    boot_script = render_template('boot-script.sh',
        aws_region          = app.config['AWS_REGION'],
        temporary_bucket    = app.config['TEMPORARY_BUCKET'],
        ssh_key             = sshkey.key,
        ephemeral_map       = ephemeral
    )

    mapping = None
    if ephemeral:
        mapping = BlockDeviceMapping()
        for device, eph_name in ephemeral.iteritems():
            mapping[device] = BlockDeviceType(ephemeral_name=eph_name)

    # Create EC2 instance
    reservation = ec2.run_instances(
        key_name                                = 'mreid',
        image_id                                = 'ami-ace67f9c',
        security_groups                         = app.config['SECURITY_GROUPS'],
        user_data                               = boot_script,
        block_device_map                        = mapping,
        instance_type                           = app.config['INSTANCE_TYPE'],
        instance_initiated_shutdown_behavior    = 'terminate',
        client_token                            = request.form['token'],
        instance_profile_name                   = app.config['INSTANCE_PROFILE']
    )
    instance = reservation.instances[0]

    # Associate a few tags
    ec2.create_tags([instance.id], {
        "Owner":            current_user.email,
        "Name":             request.form['name'],
        "Application":      app.config['INSTANCE_APP_TAG']
    })

    # Send an email to the user who launched it
    params = {
        'monitoring_url':   abs_url_for('monitor', instance_id = instance.id)
    }
    ses.send_email(
        source          = app.config['EMAIL_SOURCE'],
        subject         = ("telemetry-analysis worker instance: %s (%s) launched"
                           % (request.form['name'], instance.id)),
        format          = 'html',
        body            = render_template('instance-launched-email.html', **params),
        to_addresses    = [current_user.email]
    )
    return redirect(url_for('monitor', instance_id = instance.id))

@app.route("/worker/monitor/<instance_id>", methods=["GET"])
@login_required
def monitor(instance_id):
    # Check that the user logged in is also authorized to do this
    if not current_user.is_authorized():
        return  login_manager.unauthorized()

    try:
        # Fetch the actual instance
        reservations = ec2.get_all_reservations(instance_ids = [instance_id])
        instance = reservations[0].instances[0]
    except IndexError:
        return "No such instance: {}".format(instance_id), 404

    # Check that it is the owner who is logged in
    if instance.tags['Owner'] != current_user.email:
        return "No such instance: {}".format(instance_id), 404

    # Alright then, let's report status
    return render_template(
        'monitor.html',
        instance_id  = instance_id,
        instance_state  = instance.state,
        public_dns      = instance.public_dns_name,
        terminate_url   = abs_url_for('kill', instance_id = instance.id)
    )

@app.route("/worker/kill/<instance_id>", methods=["GET"])
@login_required
def kill(instance_id):
    # Check that the user logged in is also authorized to do this
    if not current_user.is_authorized():
        return  login_manager.unauthorized()

    try:
        # Fetch the actual instance
        reservations = ec2.get_all_reservations(instance_ids = [instance_id])
        instance = reservations[0].instances[0]
    except IndexError:
        return "No such instance: {}".format(instance_id), 404

    # Check that it is the owner who is logged in
    if instance.tags['Owner'] != current_user.email:
        return "No such instance: {}".format(instance_id), 404

    # Terminate and update instance
    instance.terminate()
    instance.update()

    # Alright then, let's report status
    return render_template(
        'kill.html',
        instance_id  = instance_id,
        instance_state  = instance.state,
        public_dns      = instance.public_dns_name,
        monitoring_url  = abs_url_for('monitor', instance_id = instance.id)
    )

@app.route("/status", methods=["GET"])
def status():
    return "OK"

login_manager.init_app(app)
browser_id.init_app(app)

if __name__ == '__main__':
    parser = ArgumentParser(description='Launch Telemetry Analysis Service')
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", default=80, type=int)
    parser.add_argument("--db-url", default='sqlite:///telemetry_analysis.db')
    args = parser.parse_args()

    app.config.update(dict(
        DB_URL = args.db_url,
        DEBUG = True
    ))

    app.run(host = args.host, port = args.port, debug=app.config['DEBUG'])
