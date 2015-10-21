Deploying telemetry-analysis
============================

## Manual setup tasks: 

1. In the [AWS SES console](https://us-west-2.console.aws.amazon.com/ses/home?region=us-west-2), make sure that the email address "telemetry-alerts@mozilla.com" is verified.

2. Make sure cross IAM S3 permissions are set up if cross-IAM access is required. Edit bucket policies for relevant buckets to look something like this:

```json
{
    "Version": "2008-10-17",
    "Statement": [
        {
            "Sid": "ListAccess",
            "Effect": "Allow",
            "Principal": {
                "AWS": [
                    "arn:aws:iam::XXXXXXXXXXXX:root"
                ]
            },
            "Action": "S3:ListBucket",
            "Resource": "arn:aws:s3:::telemetry-published-v2"
        },
        {
            "Sid": "GetAccess",
            "Effect": "Allow",
            "Principal": {
                "AWS": [
                    "arn:aws:iam::XXXXXXXXXXXX:root"
                ]
            },
            "Action": "S3:GetObject",
            "Resource": "arn:aws:s3:::telemetry-published-v2/*"
        }
    ]
}
```

## Automated deployment tasks:

1. Build an AMI for telemetry workers:

```bash
ansible-playbook -i hosts -v --extra-vars "@envs/dev.yml" playbooks/build_ami.yml
```

2. Set `worker_ami_id` in [`envs/dev.yml`](envs/dev.yml) to the value output by (1). This a git-managed file.

3. Set the RDS password in `envs/dev_secrets.yml`. See [`envs/dev_secrets.example.yml`](envs/dev_secrets.example.yml) for an example. This is an un-managed file. If the telemetry-analysis resources stack has already been created, the value you should set this to is the password portion of the URL.

4. Create the static resources Cloudformation template (only needs to be run once):

```bash
ansible-playbook -i hosts -v --extra-vars "@envs/dev.yml" --extra-vars "@envs/dev_secrets.yml" playbooks/resources.yml
```

## To update / deploy the application servers:

5. Create a new code package to use by updating `sources_version` in [`envs/dev.yml`](envs/dev.yml) and running:

```bash
ansible-playbook -i hosts -v --extra-vars "@envs/dev.yml" playbooks/make_code_package.yml
```

6. Deploy the CloudFormation template by running:

```bash
ansible-playbook -i hosts -v --extra-vars "@envs/dev.yml" playbooks/app.yml
```

7. Deploy user-facing DNS with (only needs to be run once):

```bash
ansible-playbook -i hosts -v --extra-vars "@envs/dev.yml" playbooks/route53.yaml
```
