#!/bin/bash

# Write the application's AWS credentials file. Both boto2 and boto3 will look to this file when making calls.
# Note that this env var should be set by cfn-init, as defined in the app's CFN template.
if [[ -n "$APPUSER_ACCESS_KEY" ]]; then
  mkdir -p /opt/universal_integrations/.aws
cat << EOF > /opt/universal_integrations/.aws/credentials
[default]
aws_access_key_id = $(echo "$APPUSER_ACCESS_KEY" | cut -d: -f1)
aws_secret_access_key = $(echo "$APPUSER_ACCESS_KEY" | cut -d: -f2)
EOF
fi

