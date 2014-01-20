#!/usr/bin/env python
# encoding: utf-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from string import Template
import sys
import time
from aws_launcher import SimpleLauncher
import traceback

class WorkerLauncher(SimpleLauncher):
    def get_user_data(self):
        template_params = {
            "BASE": self.config.get("base_dir", "/mnt/telemetry"),
            "JOB_NAME": self.config["job_name"],
            "CODE_URI": self.config["job_code_uri"],
            "MAIN": self.config.get("job_commandline", "./run.sh"),
            "DATA_BUCKET": self.config.get("job_data_bucket", "telemetry-public-analysis"),
            "OUTPUT_DIR": self.config.get("job_output_dir", "output"),
            "REGION": self.config.get("region", "us-west-2")
        }

        raid_config = ""
        if "ephemeral_map" in self.config:
            raid_devices = self.config["ephemeral_map"].keys()
            raid_devices.sort()
            dev_list = " ".join(raid_devices)
            # by default one of the ephemeral devices gets mounted on /mnt
            raid_config = """
# RAID0 Configuration:
umount /mnt
yes | mdadm --create /dev/md0 --level=0 -c64 --raid-devices={0} {1}
echo 'DEVICE {1}' >> /etc/mdadm/mdadm.conf
mdadm --detail --scan >> /etc/mdadm/mdadm.conf
# The "-T largefile" is to speed up the inode table creation. We
# will mostly be reading and writing files >1MB.
mkfs.ext3 -T largefile /dev/md0
mount /dev/md0 /mnt
""".format(len(raid_devices), dev_list)
        template_params["RAID_CONFIGURATION"] = raid_config

        template_str = """#!/bin/bash
LOG=$BASE/$JOB_NAME.$(date +%Y%m%d%H%M%S).log
S3_BASE=s3://$DATA_BUCKET/$JOB_NAME
$RAID_CONFIGURATION
pip install --upgrade awscli
mkdir -p $BASE
chown ubuntu:ubuntu $BASE
sudo -u ubuntu bash <<EOF
cd $BASE
mkdir -p $OUTPUT_DIR
aws --region $REGION s3 cp $CODE_URI code.tar.gz
tar xzvf code.tar.gz
$MAIN &> $LOG
echo "'$MAIN' exited with code $?" >> $LOG
cd $OUTPUT_DIR
for f in \$(find . -type f); do
  # Remove the leading "./"
  f=\$(sed -e "s/^\.\///" <<< \$f)
  UPLOAD_CMD="aws --region $REGION s3 cp ./\$f $S3_BASE/data/\$f"
  if [[ "\$f" == *.gz ]]; then
    echo "adding 'Content-Type: gzip' for \$f" >> $LOG
    UPLOAD_CMD="\$UPLOAD_CMD --content-encoding 'gzip'"
  else
    echo "Not adding 'Content-Type' header for \$f" >> $LOG
  fi
  echo "Running: \$UPLOAD_CMD" >> $LOG
  \$UPLOAD_CMD &>> $LOG
done
cd -
gzip $LOG
aws --region $REGION s3 cp ${LOG}.gz $S3_BASE/logs/$(basename $LOG).gz --content-type "text/plain" --content-encoding "gzip"
EOF
#halt
"""
        template = Template(template_str)
        return template.safe_substitute(template_params)
    def run(self, instance):
        # TODO: periodically poll for the instance's state
        # if it doesn't die after some timeout, kill it.
        timeout = self.config.get("job_timeout_minutes", 60)
        for i in range(1, timeout + 1):
            time.sleep(60)
            instance.update()
            if instance.state == 'running':
                print "Instance", instance.id, "still running after", i, "minutes:", instance.public_dns_name
            else:
                break

        print "After", i, "minutes, instance", instance.id, "was", instance.state
        if instance.state == 'running':
            print "Time to kill it."
            self.terminate(self.conn, instance)

def main():
    try:
        launcher = WorkerLauncher()
        launcher.go()
        return 0
    except Exception, e:
        print "Error:", e
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
