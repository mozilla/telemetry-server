
SECRET_KEY        = 'Overwrite with a secret on deployment'

# AWS EC2 configuration
AWS_REGION        = 'us-west-2'
INSTANCE_TYPE     = 'm2.4xlarge'
SECURITY_GROUPS   = []
INSTANCE_PROFILE  = 'telemetry-analysis-profile'
INSTANCE_APP_TAG  = 'telemetry-analysis-debug-instance'
EMAIL_SOURCE      = 'jonasfj@mozilla.com'

# Bucket for storing public ssh-keys
TEMPORARY_BUCKET  = 'bucket-for-ssh-keys'
