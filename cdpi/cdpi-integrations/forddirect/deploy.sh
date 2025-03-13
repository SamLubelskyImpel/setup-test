#!/bin/bash
set -e
cd "$(dirname "$0")" || return

function help() {
  echo "
    Deploy the Ford Direct CDPI resources.
    Usage:
     ./deploy.sh <parameters>
    Options:
     -e       REQUIRED: environment to deploy to, e.g. dev, test, prod
     -r       an Impel region, e.g. us, eu
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

user=$(aws iam get-user --output json | jq -r .User.UserName | sed 's/\./-/g')
commit_id=$(git log -1 --format=%H)

sam build --parallel

if [[ $config_env == "prod" ]]; then
  stack_name="forddirect-cdpi-prod"
  profile="unified-prod"
  sam deploy --config-env "prod" \
    --tags "Commit=\"$commit_id\" Environment=\"prod\" UserLastModified=\"$user\"" \
    --region "$region" \
    --s3-bucket "spincar-deploy-$region" \
    --parameter-overrides "Environment=\"prod\""
elif [[ $config_env == "test" ]]; then
  stack_name="forddirect-cdpi-test"
  profile="unified-test"
  sam deploy --config-env "test" \
    --tags "Commit=\"$commit_id\" Environment=\"test\" UserLastModified=\"$user\"" \
    --region "$region" \
    --s3-bucket "spincar-deploy-$region" \
    --parameter-overrides "Environment=\"test\""
else
  env="$user-$(git rev-parse --abbrev-ref HEAD)"
  stack_name="forddirect-cdpi-$env"
  profile="unified-test"
  sam deploy \
    --tags "Commit=\"$commit_id\" Environment=\"$env\" UserLastModified=\"$user\"" \
    --stack-name "$stack_name" \
    --region "$region" \
    --s3-bucket "spincar-deploy-$region" \
    --parameter-overrides "Environment=\"$env\" DomainSuffix=\"-$env\""
fi

# ✅ **Retrieve SQS URL after deployment**
echo "Fetching SQS URL from CloudFormation stack: $stack_name..."
sqs_url=$(aws cloudformation describe-stacks \
  --stack-name "$stack_name" \
  --region "$region" \
  --profile "$profile" \
  --query "Stacks[0].Outputs[?OutputKey=='SendEventToFordDirectQueue'].OutputValue | [0]" \
  --output text)

if [[ -z "$sqs_url" || "$sqs_url" == "None" ]]; then
  echo "Error: SQS URL not found in CloudFormation outputs!"
  echo sqs_url
  exit 1
fi

echo "SQS URL retrieved: $sqs_url"

# ✅ **Check if the SQS URL already exists in SSM**
ssm_param="/sqs/$config_env/queue_url"

existing_sqs_url=$(aws ssm get-parameter \
  --name "$ssm_param" \
  --region "$region" \
  --profile "$profile" \
  --query "Parameter.Value" \
  --output text 2>/dev/null)

if [[ $? -eq 0 && "$existing_sqs_url" == "$sqs_url" ]]; then
  echo "The SQS URL already exists in SSM and matches the fetched URL. Exiting."
  exit 1
else
  echo "SQS URL is either not in SSM or does not match. Proceeding with update."
fi

# ✅ **Store SQS URL in SSM Parameter Store**
echo "Storing SQS URL in SSM Parameter Store at $ssm_param..."
aws ssm put-parameter \
  --name "$ssm_param" \
  --value "$sqs_url" \
  --type "String" \
  --overwrite \
  --region "$region" \
  --profile "$profile"

echo "✅ SQS URL successfully stored in SSM: $ssm_param"
