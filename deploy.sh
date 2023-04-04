#!/bin/bash
set -e
cd "$(dirname "$0")" || return

function help() {
  echo "
    Deploy the dms data service.
    Usage:
     ./deploy.sh <parameters>
    Options:
     -e       REQUIRED: environment to deploy to, e.g. test, stage, prod
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

python3 ./dms_data_service/oas_interpolator.py

user=$(aws iam get-user --output json | jq -r .User.UserName)
commit_id=$(git log -1 --format=%H)

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
else
  env="$user-$(git rev-parse --abbrev-ref HEAD)"
  sam deploy \
    --tags "Commit=\"$commit_id\" Environment=\"$env\" UserLastModified=\"$user\"" \
    --stack-name "universal-integrations-$env" \
    --region "$region" \
    --s3-bucket "spincar-deploy-$region" \
    --parameter-overrides "Environment=\"$env\""
fi
