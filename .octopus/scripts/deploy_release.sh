#!/bin/bash

echo "cding to dealerpeak directory"
cd ../..
cd crm/crm-integrations/dealerpeak


AWS_REGION=$(get_octopusvariable "AWS_REGION")
echo "AWS_REGION: $AWS_REGION"

echo "installing sam"
curl -LJO https://github.com/aws/aws-sam-cli/releases/latest/download/aws-sam-cli-linux-x86_64.zip
unzip aws-sam-cli-linux-x86_64.zip -d sam-installation
sudo ./sam-installation/install
sam --version

echo "building and deploying"
sam build --parallel
sam deploy --config-env "test" --no-confirm-changeset --no-fail-on-empty-changeset --no-progressbar --s3-bucket=spincar-deploy-${AWS_REGION}