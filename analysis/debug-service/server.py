#!/usr/bin/env python

from argparse import ArgumentParser
from flask import Flask, render_template, request, redirect, url_for
from flask.ext.login import LoginManager, login_required, current_user
from flask.ext.browserid import BrowserID
from user import User, AnonymousUser
from boto.ec2 import connect_to_region as ec2_connect
from boto.ses import connect_to_region as ses_connect
from boto.s3 import connect_to_region as s3_connect
from urlparse import urljoin
from uuid import uuid4

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
    return render_template('unauthorized.html')

# Routes
@app.route('/', methods=["GET"])
def index():
    return render_template('index.html', token = str(uuid4()))

@app.route("/schedule", methods=["GET"])
@login_required
def schedule_job():
    # Check that the user logged in is also authorized to do this
    if not current_user.is_authorized():
        return login_manager.unauthorized()
    return render_template('schedule.html', token = str(uuid4()))

@app.route("/schedule-job", methods=["POST"])
@login_required
def confirm_job():
    # Check that the user logged in is also authorized to do this
    if not current_user.is_authorized():
        return login_manager.unauthorized()
    return render_template('schedule_confirm.html', token = str(uuid4()))

@app.route("/debug", methods=["GET"])
@login_required
def get_debug_params():
    # Check that the user logged in is also authorized to do this
    if not current_user.is_authorized():
        return login_manager.unauthorized()
    return render_template('debug.html', token = str(uuid4()))

@app.route("/spawn-debug-instance", methods=["POST"])
@login_required
def spawn_debug_instance():
    # Check that the user logged in is also authorized to do this
    if not current_user.is_authorized():
        return login_manager.unauthorized()

    # Upload s3 key to bucket
    sshkey = bucket.new_key("keys/%s.pub" % request.form['token'])
    sshkey.set_contents_from_file(request.files['public-ssh-key'])

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
        subject         = ("telemetry-analysis debug instance: %s (%s) launched"
                           % (request.form['name'], instance.id)),
        format          = 'html',
        body            = render_template('instance-launched-email.html', **params),
        to_addresses    = [current_user.email]
    )
    return redirect(url_for('monitor', instance_id = instance.id))

@app.route("/monitor/<instance_id>", methods=["GET"])
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

@app.route("/kill/<instance_id>", methods=["GET"])
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
    args = parser.parse_args()

    app.run(host = args.host, port = args.port)
