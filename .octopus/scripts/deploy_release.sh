#!/bin/bash

echo "cding to dealerpeak directory"
cd ../..
cd crm/crm-integrations/dealerpeak

ROLE_ARN="arn:aws:iam::143813444726:role/dev"
AWS_ACCESS_KEY_ID= "#{test_deploy.AccessKey}"
AWS_SECRET_ACCESS_KEY= "#{test_deploy.SecretKey}"
AWS_REGION= $(get_octopusvariable "AWS_REGION")
echo "AWS_REGION: $AWS_REGION"
echo "AWS_ACCESS_KEY_ID: $AWS_ACCESS_KEY_ID"
echo "AWS_SECRET_ACCESS_KEY: $AWS_SECRET_ACCESS_KEY"
echo "ROLE_ARN: $ROLE_ARN"

aws configure set profile.default.output json
aws configure set profile.default.region $AWS_REGION
aws configure set profile.default.aws_access_key_id $AWS_ACCESS_KEY_ID
aws configure set profile.default.aws_secret_access_key $AWS_SECRET_ACCESS_KEY

aws configure set profile.unified-test.role_arn $ROLE_ARN
aws configure set profile.unified-test.source_profile default
aws configure set profile.unified-test.region $AWS_REGION
aws configure set profile.unified-test.output json

echo "building and deploying"
sam build --parallel
sam deploy --config-env "test" --no-confirm-changeset --no-fail-on-empty-changeset --no-progressbar --s3-bucket=spincar-deploy-${AWS_REGION}