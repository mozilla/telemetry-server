#!/bin/bash

cd /home/ubuntu

# Install a few dependencies
sudo apt-get -y install xz-utils python-pip git python-dev
sudo pip install --upgrade boto awscli simplejson

# Get users ssh key
python - << END
from boto.s3 import connect_to_region
s3 = connect_to_region('{{ aws_region }}')
b = s3.get_bucket('{{ temporary_bucket }}', validate = False)
k = b.get_key('{{ ssh_key }}')
k.get_contents_to_filename('/home/ubuntu/user_key.pub')
END


{% if ephemeral_map %}
# RAID0 Configuration:
{% set raid_devices = ephemeral_map.keys()|sort %}
{% set device_list = " ".join(raid_devices) %}
export DEBIAN_FRONTEND=noninteractive; apt-get --yes install mdadm
umount /mnt
yes | mdadm --create /dev/md0 --level=0 -c64 --raid-devices={{ raid_devices|length }} {{ device_list }}
echo 'DEVICE {{ device_list }}' >> /etc/mdadm/mdadm.conf
mdadm --detail --scan >> /etc/mdadm/mdadm.conf
mkfs.xfs /dev/md0
mount /dev/md0 /mnt
{% endif %}

# Setup users ssh_key
cat /home/ubuntu/user_key.pub >> /home/ubuntu/.ssh/authorized_keys
chmod 600 /home/ubuntu/.ssh/authorized_keys

# Set the default AWS region
if [ ! -d /home/ubuntu/.aws ]; then
  sudo -u ubuntu mkdir /home/ubuntu/.aws
fi
if [ ! -f /home/ubuntu/.aws/config ]; then
  sudo -u ubuntu echo -e "[default]\nregion = {{ aws_region }}" > /home/ubuntu/.aws/config
fi

# Make telemetry work dir
if [ ! -d /mnt/telemetry ]; then
  mkdir /mnt/telemetry
fi
chown ubuntu:ubuntu /mnt/telemetry

# Setup the motd
sudo cat >/etc/motd <<END
Welcome to a Telemetry Analysis worker node.

* For a quick intro, see:
  http://mreid-moz.github.io/blog/2013/11/06/current-state-of-telemetry-analysis/

* The telemetry-server repository is at ~/telemetry-server

* Don't forget to copy any important code and data off this machine! It will
  only be available for 24 hours, after which its contents will be...
          ********************
          *** GONE FOREVER ***
          ********************

* If you get stuck, drop by #telemetry on irc.mozilla.org
END

# Check out telemetry-server repo:
sudo -u ubuntu git clone https://github.com/mozilla/telemetry-server.git
