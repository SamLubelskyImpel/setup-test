#!/bin/bash

echo "cding to dealerpeak directory"
cd ../..
cd crm/crm-integrations/dealerpeak


AWS_REGION=$(get_octopusvariable "AWS_REGION")
echo "AWS_REGION: $AWS_REGION"

echo "building and deploying"
sam build --parallel
sam deploy --config-env "test" --no-confirm-changeset --no-fail-on-empty-changeset --no-progressbar --s3-bucket=spincar-deploy-${AWS_REGION}