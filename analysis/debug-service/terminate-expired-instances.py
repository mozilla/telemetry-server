#!/usr/bin/env python

from boto.ec import connect_to_region as ec2_connect
from dateutil.parser import parse as parse_date
from datetime.datetime import utcnow
import config

ec2 = ec2_connect(region = config.AWS_REGION)
ses = ses_connect(region = config.AWS_REGION)

def main():
    reservations = ec2.get_all_reservations(
        filters = {'tag:Application':  config.INSTANCE_APP_TAG}
    )
    for reservation in reservations:
        for instance in reservation:
            time = utcnow() - parse_date(instance.launch_time, ignoretz = True)
            if time.days >= 1:
                name = instance.tags.get('name', instance.id)
                ses.send(
                    source          = config.EMAIL_SOURCE,
                    subject         = "telemetry-analysis debug instance %s terminated!" % name,
                    body            = "We've terminated your instance as it has been running for over 24 hours!",
                    to_addresses    = [instance.tags['Owner']]
                )
                instance.terminate()

if __name__ == '__main__':
    main()