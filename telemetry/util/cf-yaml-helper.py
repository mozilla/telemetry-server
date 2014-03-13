#!/usr/bin/env python

import json, yaml, sys
from boto.cloudformation.connection import CloudFormationConnection
from boto.exception import BotoServerError
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
from xml.etree import ElementTree


def load_template(input_file):
  # Read YAML, dump as JSON
  with open(input_file, 'r') as input:
    try:
      return json.dumps(yaml.load(input), indent = 2), True
    except yaml.scanner.ScannerError as e:
      print >> sys.stderr, "YAML Parsing Failed:"
      print >> sys.stderr, e
      return None, False
    except yaml.parser.ParserError as e:
      print >> sys.stderr, "YAML Parsing Failed:"
      print >> sys.stderr, e
      return None, False

def validate_template(template, aws_key = None, aws_secret_key = None):
    # Connect to CloudFormation, if keys are None, they're loaded from
    # environment variables or boto configuration if present.
    conn = CloudFormationConnection(
      aws_access_key_id = aws_key,
      aws_secret_access_key = aws_secret_key
    )
    retval = True
    try:
      conn.validate_template(template_body = template)
    except BotoServerError as e:
      print >> sys.stderr, "Template Validation Failed:"
      for Error in ElementTree.fromstring(e.args[2]):
        if not Error.tag.endswith("Error"):
          continue
        code = "Unknown"
        msg = "Unknown"
        for element in Error:
          if element.tag.endswith("Code"):
            code = element.text
          elif element.tag.endswith("Message"):
            msg = element.text
        print >> sys.stderr, "Source:   %s" % code
        print >> sys.stderr, "Message:  %s" % msg
      retval = False
    conn.close()
    return retval

def main():
  parser = ArgumentParser(
    description     = "Convert YAML to validated CloudFormation template JSON",
    epilog          = "AWS credentials can also be provided by environment "
                      "variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY or "
                      "by boto configuration file.",
    formatter_class = ArgumentDefaultsHelpFormatter
  )
  parser.add_argument(
    "yaml_template",
    help    = "CloudFormation template as YAML"
  )
  parser.add_argument(
    "-o", "--output-file",
    help    = "File to write validated JSON template",
    default = "-"
  )
  parser.add_argument(
    "-k", "--aws-key",
    help    = "AWS Key"
  )
  parser.add_argument(
    "-s", "--aws-secret-key",
    help    = "AWS Secret Key"
  )
  parser.add_argument(
    "--skip-validation",
    action  = "store_true",
    help    = "Skip template validation"
  )
  args = parser.parse_args()

  # Load template
  template, loaded = load_template(args.yaml_template)
  if not loaded:
    return 1

  # Validate template
  if not args.skip_validation:
    if not validate_template(template, args.aws_key, args.aws_secret_key):
      return 2

  # Output validate template
  if args.output_file is "-":
    print template
  else:
    with open(args.output_file, 'w') as output:
      output.write(template)

  return 0

if __name__ == "__main__":
    sys.exit(main())