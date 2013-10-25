#!/bin/bash

cd /home/ubuntu

# Install a few dependencies
sudo apt-get -y install xz-utils python-pip
sudo pip install --upgrade boto awscli

# Get users ssh key
python - << END
from boto.s3 import connect_to_region
s3 = connect_to_region('{{ aws_region }}')
b = s3.get_bucket('{{ temporary_bucket }}', validate = False)
k = b.get_key('{{ ssh_key }}')
k.get_contents_to_filename('/home/ubuntu/user_key.pub')
END

# Setup users ssh_key
cat /home/ubuntu/user_key.pub >> /home/ubuntu/.ssh/authorized_keys
chmod 600 /home/ubuntu/.ssh/authorized_keys
