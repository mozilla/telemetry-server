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
            "REGION": self.config.get("region", "us-west-2"),
            "NOTIFY": self.config.get("job_owner", "telemetry-alerts@mozilla.com")
        }

        raid_config = ""
        if "ephemeral_map" in self.config:
            raid_devices = self.config["ephemeral_map"].keys()
            raid_devices.sort()
            dev_list = " ".join(raid_devices)
            # by default one of the ephemeral devices gets mounted on /mnt
            raid_config = """
# RAID0 Configuration:
# TODO: install xfsprogs in the AMI instead.
export DEBIAN_FRONTEND=noninteractive; apt-get --yes install mdadm xfsprogs
umount /mnt
yes | mdadm --create /dev/md0 --level=0 -c64 --raid-devices={0} {1}
echo 'DEVICE {1}' >> /etc/mdadm/mdadm.conf
mdadm --detail --scan >> /etc/mdadm/mdadm.conf
mkfs.xfs /dev/md0
mount /dev/md0 /mnt
""".format(len(raid_devices), dev_list)
        template_params["RAID_CONFIGURATION"] = raid_config

        template_str = """#!/bin/bash
apt-get update
export DEBIAN_FRONTEND=noninteractive; apt-get --yes install python-pip
S3_BASE="s3://$DATA_BUCKET/$JOB_NAME"
$RAID_CONFIGURATION
pip install --upgrade awscli
mkdir -p $BASE
LOG="$BASE/$JOB_NAME.$(date +%Y%m%d%H%M%S).log"
if [ ! -d "$(dirname "$LOG")" ]; then
  mkdir -p "$(dirname "$LOG")"
fi
chown -R ubuntu:ubuntu $BASE
sudo -Hu ubuntu bash <<EOF
if [ -d "~/telemetry-server" ]; then
  cd ~/telemetry-server
  git pull
fi
mkdir -p ~/.aws
echo "[default]" > ~/.aws/config
echo "region = $REGION" >> ~/.aws/config
cd $BASE
mkdir -p $OUTPUT_DIR
aws s3 cp "$CODE_URI" code.tar.gz
tar xzvf code.tar.gz
# Temporarily disable "exit on error" so we can capture error output:
set +e
echo "Beginning job $JOB_NAME ..." >> "$LOG"
$MAIN &>> "$LOG"
JOB_EXIT_CODE=\$?
echo "Finished job $JOB_NAME" >> "$LOG"
set -e
echo "'$MAIN' exited with code \$JOB_EXIT_CODE" >> "$LOG"
cd $OUTPUT_DIR
for f in \$(find . -type f); do
  # Remove the leading "./"
  f=\$(sed -e "s/^\.\///" <<< \$f)
  UPLOAD_CMD="aws s3 cp ./\$f '$S3_BASE/data/\$f'"
  if [[ "\$f" == *.gz ]]; then
    echo "adding 'Content-Type: gzip' for \$f" >> "$LOG"
    UPLOAD_CMD="\$UPLOAD_CMD --content-encoding gzip"
  else
    echo "Not adding 'Content-Type' header for \$f" >> "$LOG"
  fi
  echo "Running: \$UPLOAD_CMD" >> "$LOG"
  eval \$UPLOAD_CMD &>> "$LOG"
done
cd -
gzip "$LOG"
S3_LOG="$S3_BASE/logs/$(basename "$LOG").gz"
aws s3 cp "${LOG}.gz" "\$S3_LOG" --content-type "text/plain" --content-encoding gzip
if [ \$JOB_EXIT_CODE -ne 0 ]; then
  aws ses send-email \\
    --region $REGION \\
    --from telemetry-alerts@mozilla.com \\
    --to $NOTIFY \\
    --subject "Job Error: $JOB_NAME Failed" \\
    --text "$JOB_NAME exited with status \$JOB_EXIT_CODE. See full log at \$S3_LOG"
fi
EOF
halt
"""
        template = Template(template_str)
        return template.safe_substitute(template_params)
    def run(self, instance):
        # TODO: periodically poll for the instance's state
        # if it doesn't die after some timeout, kill it.
        self.timed_out = False
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
            self.timed_out = True
            self.terminate(self.conn, instance)

def main():
    try:
        launcher = WorkerLauncher()
        launcher.go()
        if launcher.timed_out:
            # Exit with a special code if the job timed out.
            return 2
        return 0
    except Exception, e:
        print "Error:", e
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
