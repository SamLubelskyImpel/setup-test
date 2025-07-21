#!/bin/bash

pwd
ls
cd ../..
pwd
ls
cd crm/crm-integrations/dealerpeak
pwd

AWS_REGION=$(get_octopusvariable "AWS_REGION")
echo "AWS_REGION: $AWS_REGION"

curl -LJO https://github.com/aws/aws-sam-cli/releases/latest/download/aws-sam-cli-linux-x86_64.zip
unzip aws-sam-cli-linux-x86_64.zip -d sam-installation
chmod +x sam-installation/install
./sam-installation/install
sam --version

sam build --parallel
sam deploy --config-env "test" --no-confirm-changeset --no-fail-on-empty-changeset --no-progressbar --s3-bucket=spincar-deploy-${AWS_REGION}