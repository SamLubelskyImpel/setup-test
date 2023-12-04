#!/bin/bash
set -e
cd "$(dirname "$0")" || return

function help() {
  echo "
    Deploy the crm api.
    Usage:
     ./deploy.sh <parameters>
    Options:
     -e       REQUIRED: environment to deploy to, e.g. dev, test, prod
     -r       an an Impel region, e.g. us, eu
     -h       display this help
    "
  exit 2
}

while getopts e:hp:r: option
do
  case "${option}"
    in
    h)help;;
    e)config_env=${OPTARG};;
    r)input_region=${OPTARG};;
    *)help;;
  esac
done

if [ $OPTIND -eq 1 ]; then help; fi
: ${config_env?"Must give an environment, -e"}

if [[ $input_region == "" || $input_region == "us" ]]; then
    region="us-east-1"
else
    echo "Invalid region $input_region"
    exit 2
fi

user=$(aws iam get-user --output json | jq -r .User.UserName)
commit_id=$(git log -1 --format=%H)

python3 ./swagger/oas_interpolator.py
sam build --parallel

if [[ $config_env == "prod" ]]; then
  sam deploy --config-env "prod" \
    --tags "Commit=\"$commit_id\" Environment=\"prod\" UserLastModified=\"$user\"" \
    --region "$region" \
    --s3-bucket "spincar-deploy-$region" \
    --parameter-overrides "Environment=\"prod\""
elif [[ $config_env == "stage" ]]; then
  sam deploy --config-env "stage" \
    --tags "Commit=\"$commit_id\" Environment=\"stage\" UserLastModified=\"$user\"" \
    --region "$region" \
    --s3-bucket "spincar-deploy-$region" \
    --parameter-overrides "Environment=\"stage\""
elif [[ $config_env == "test" ]]; then
  sam deploy --config-env "test" \
    --tags "Commit=\"$commit_id\" Environment=\"test\" UserLastModified=\"$user\"" \
    --region "$region" \
    --s3-bucket "spincar-deploy-$region" \
    --parameter-overrides "Environment=\"test\" DomainSuffix=\"-test\""
else
  env="$user-$(git rev-parse --abbrev-ref HEAD)"
  sam deploy \
    --tags "Commit=\"$commit_id\" Environment=\"$env\" UserLastModified=\"$user\"" \
    --stack-name "crm-api-$env" \
    --region "$region" \
    --s3-bucket "spincar-deploy-$region" \
    --parameter-overrides "Environment=\"$env\" DomainSuffix=\"-$env\""
fi
