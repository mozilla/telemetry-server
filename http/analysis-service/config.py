
SECRET_KEY         = 'Overwrite with a secret on deployment'

# AWS EC2 configuration
AWS_REGION         = 'us-west-2'
INSTANCE_TYPE      = 'c3.4xlarge'

# EMR configuration
# Master and slave instance types should be the same as the telemetry
# setup bootstrap action depends on it to autotune the cluster.
MASTER_INSTANCE_TYPE = INSTANCE_TYPE
SLAVE_INSTANCE_TYPE = INSTANCE_TYPE
SPARK_VERSION = '1.1.1.e'
AMI_VERSION = '3.3.1'

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
