#!/usr/bin/env python

from argparse import ArgumentParser
from flask import Flask, render_template, g, request, redirect, url_for
from flask.ext.login import LoginManager, login_required, current_user
from flask.ext.browserid import BrowserID
from user import User, AnonymousUser
from boto.ec2 import connect_to_region as ec2_connect
from boto.ses import connect_to_region as ses_connect
from boto.s3 import connect_to_region as s3_connect
from urlparse import urljoin
from uuid import uuid4
from sqlalchemy import create_engine, MetaData
from sqlalchemy.sql import select, func

# Create flask app
app = Flask(__name__)
app.config.from_object('config')

# Connect to AWS
ec2 = ec2_connect(app.config['AWS_REGION'])
ses = ses_connect('us-east-1') # only supported region!
s3  = s3_connect(app.config['AWS_REGION'])
bucket = s3.get_bucket(app.config['TEMPORARY_BUCKET'], validate = False)

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

def connect_db():
    db = {}
    db['engine'] = create_engine(app.config['DB_URL'])
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
        Column("schedule_day_of_week",  String(20),  nullable=False),
        Column("schedule_command",      String(300), nullable=False)
    )
    # Create the table
    db['metadata'].create_all(tables=[scheduled_jobs])

def get_db():
    """Opens a new database connection if there is none yet for the
    current application context.
    """
    if not hasattr(g, 'database'):
        g.database = connect_db()
    return g.database

def save_job(job):
    db = get_db()
    table = db['metadata'].tables['scheduled_jobs']
    insert = table.insert().values(job)
    print "Insert:", str(insert)
    return db['conn'].execute(insert)

def get_jobs(owner):
    db = get_db()
    table = db['metadata'].tables['scheduled_jobs']
    query = select([table]).where(table.c.owner == owner).order_by(table.c.id)
    result = db['conn'].execute(query)
    try:
        for row in result:
            yield row
    except:
        result.close()
        raise
    result.close()

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

def update_crontab():
    # TODO: implement me
    pass

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

    dayname = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"][dow]
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

    # Last bit is the command to execute.
    # TODO: this is a placeholder.
    cron_bits.append("/path/to/run/script.sh")

    # Check if job_name is already in use
    if job_exists(name=request.form['job-name']):
        errors['job-name'] = "The name '{}' is already in use. Choose another name.".format(request.form['job-name'])

    # If there were any errors, stop and re-display the form.
    # It's only polite to render the form with the previously-supplied
    # values filled in. Unfortunately doing so for files doesn't seem to be
    # worth the effort.
    if errors:
        return schedule_job(errors, request.form)

    # Now do it!
    code_s3path = "s3://telemetry-analysis-code/{0}/{1}".format(request.form["job-name"], request.files["code-tarball"].filename)
    data_s3path = "s3://telemetry-public-analysis/{0}/data/".format(request.form["job-name"])

    jerb = {
        "owner": current_user.email,
        "name": request.form['job-name'],
        "timeout_minutes": timeout,
        "code_uri": code_s3path,
        "commandline": request.form['commandline'],
        "data_bucket": "telemetry-public-analysis", #TODO: get this from config
        "output_dir": request.form['output-dir'],
        "schedule_minute": cron_bits[CRON_IDX_MIN],
        "schedule_hour": cron_bits[CRON_IDX_HOUR],
        "schedule_day_of_month": cron_bits[CRON_IDX_DOM],
        "schedule_month": cron_bits[CRON_IDX_MON],
        "schedule_day_of_week": cron_bits[CRON_IDX_DOW],
        "schedule_command": cron_bits[CRON_IDX_CMD]
    }

    result = save_job(jerb)

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

    # Create
    boot_script = render_template('boot-script.sh',
        aws_region          = app.config['AWS_REGION'],
        temporary_bucket    = app.config['TEMPORARY_BUCKET'],
        ssh_key             = sshkey.key
    )

    # Create EC2 instance
    reservation = ec2.run_instances(
        image_id                                = 'ami-ace67f9c',
        security_groups                         = app.config['SECURITY_GROUPS'],
        user_data                               = boot_script,
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
        return "No such instance"

    # Check that it is the owner who is logged in
    if instance.tags['Owner'] != current_user.email:
        return  login_manager.unauthorized()

    # Alright then, let's report status
    return render_template(
        'monitor.html',
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
        return "No such instance"

    # Check that it is the owner who is logged in
    if instance.tags['Owner'] != current_user.email:
        return login_manager.unauthorized()

    # Terminate and update instance
    instance.terminate()
    instance.update()

    # Alright then, let's report status
    return render_template(
        'kill.html',
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
