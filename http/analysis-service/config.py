
SECRET_KEY         = 'Overwrite with a secret on deployment'

# AWS EC2 configuration
AWS_REGION         = 'us-west-2'
INSTANCE_TYPE      = 'c3.4xlarge'
WORKER_AMI         = 'ami-0057b733' # -> telemetry-worker-hvm-20151019 (Ubuntu 15.04)
WORKER_PRIVATE_PROFILE = 'telemetry-example-profile'
WORKER_PUBLIC_PROFILE  = 'telemetry-example-profile'

# EMR configuration
# Master and slave instance types should be the same as the telemetry
# setup bootstrap action depends on it to autotune the cluster.
MASTER_INSTANCE_TYPE = INSTANCE_TYPE
SLAVE_INSTANCE_TYPE = INSTANCE_TYPE
SPARK_VERSION = '1.3.1.e'
AMI_VERSION = '3.3.2'
SPARK_INSTANCE_PROFILE = 'telemetry-example-profile'
SPARK_EMR_BUCKET = 'example'

# Make sure the ephemeral map matches the instance type above.
EPHEMERAL_MAP      = { "/dev/xvdb": "ephemeral0", "/dev/xvdc": "ephemeral1" }
SECURITY_GROUPS    = []
INSTANCE_PROFILE   = 'telemetry-analysis-profile'
INSTANCE_APP_TAG   = 'telemetry-analysis-worker-instance'
EMAIL_SOURCE       = 'telemetry-alerts@mozilla.com'

# Buckets for storing S3 data
TEMPORARY_BUCKET   = 'bucket-for-ssh-keys'
CODE_BUCKET        = 'telemetry-analysis-code'
PUBLIC_DATA_BUCKET = 'telemetry-public-analysis'
PRIVATE_DATA_BUCKET = 'telemetry-private-analysis'
