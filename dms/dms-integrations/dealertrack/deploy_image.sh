#!/bin/bash
set -e
cd "$(dirname "$0")" || return

function help() {
  echo "
    Deploy the dealertrack integration ECS image.
    Usage:
     ./deploy.sh <parameters>
    Options:
     -e       REQUIRED: environment to deploy to, e.g. dev, test, prod
     -p       REQUIRED: the profile to be used for deployment
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
    p)profile=${OPTARG};;
    r)input_region=${OPTARG};;
    *)help;;
  esac
done

if [ $OPTIND -eq 1 ]; then help; fi
: ${config_env?"Must give an environment, -e"}
: ${profile?"Must give an aws profile, -p"}

if [[ $input_region == "" || $input_region == "us" ]]; then
    region="us-east-1"
else
    echo "Invalid region $input_region"
    exit 2
fi

user=$(aws iam get-user --output json | jq -r .User.UserName | sed 's/\./-/g')
aws_account_id="143813444726"
env="$user-$(git rev-parse --abbrev-ref HEAD)"

if [[ $config_env == "prod" ]]; then
  aws_account_id="196800776222"
  env=prod
elif [[ $config_env == "test" ]]; then
  env=test
fi

ecr_repository="dealertrack-$env-ecr"
image_name="dealertrack-$env-historical-pull-image"

docker build -t $image_name . --platform=linux/amd64
AWS_PROFILE=$profile aws ecr get-login-password --region $region | docker login --username AWS --password-stdin $aws_account_id.dkr.ecr.$region.amazonaws.com
docker tag $image_name $aws_account_id.dkr.ecr.$region.amazonaws.com/$ecr_repository:latest
docker push $aws_account_id.dkr.ecr.$region.amazonaws.com/$ecr_repository:latest
