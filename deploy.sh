#!/bin/bash

echo "Deploying Universal Integration..."
PYTHON_VERSION=`python3 -V 2>&1 | grep -Po '(?<=Python )(.+)'`
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)
echo "$SCRIPT_DIR"

CFN_INIT_ARTIFACT=null

optstring=":h:p:e:"
usage() { echo "$0 usage:" && grep " .)\ #" "$0"; exit 0; }
[ $# -eq 0 ] && usage
while getopts ${optstring} arg; do
  case $arg in
    p) # Specify AWS credential profile e.g test or prod
      PROFILE=${OPTARG}
      ;;
    e) # Environment can either be test or prod
      ENVIRONMENT=${OPTARG}
      ;;
    h | *) # Display help.
      usage
      exit 0
      ;;
  esac
done

function parse_s3_path()
{
    s3_url=$1
    env_account=$(echo $s3_url | awk -F/ '{print $4}')
    artifact_url=$(echo $s3_url | awk -F/ '{print $5}')
    development_bucket=$(echo $s3_url | awk -F/ '{print $3}')
    cfninit_artifact_s3_key="$env_account/$artifact_url"
    echo "$development_bucket" "$cfninit_artifact_s3_key"
}
APP_NAME="universal-integrations-$ENVIRONMENT"
CONFIG="default"
CommitHash=`git log -1 --format=%h`
USER=$(aws iam get-user --output json | jq -r .User.UserName)
DEPLOYMENT_START_TIME=$(date '+%Y%m%dT%H%M')
DEPLOYMENT_ID="$CommitHash-$DEPLOYMENT_START_TIME"
AmiId="ami-007855ac798b5175e" # Ubuntu Server 22.04 LTS (HVM), SSD Volume Type
ROLE_ARN="arn:aws:iam::196800776222:role/cfn-deployer-universal-integration"
REGION="us-east-1"

if [[ "$ENVIRONMENT" == "test" ]]; then
  CONFIG=$ENVIRONMENT
  ROLE_ARN="arn:aws:iam::143813444726:role/cfn-deployer-universal-integration"
fi

S3CFNInitArticfact=`$SCRIPT_DIR/deploy/build_cfninit_artifact.sh -a "$APP_NAME" -e "$ENVIRONMENT" -p "$PROFILE" -r "$REGION";`
read DEPLOYMENT_BUCKET CFN_INIT_ARTIFACT < <(parse_s3_path "$S3CFNInitArticfact")

<<<<<<< HEAD
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
=======
if [ -z "$S3CFNInitArticfact" ]
then
  exit 1
>>>>>>> master
fi

echo "DEPLOYMENT_BUCKET: {$DEPLOYMENT_BUCKET}"
echo "cfninit_artifact_s3_key: {$CFN_INIT_ARTIFACT}"

S3CodeArtifact=`$SCRIPT_DIR/deploy/build_code_artifact.sh -a $APP_NAME -e $ENVIRONMENT -p $PROFILE -r $REGION`
echo "S3CodeArtifact => $S3CodeArtifact"
read ARTIFACT_DEPLOYMENT_BUCKET CODE_ARTIFACT_KEY < <(parse_s3_path "$S3CodeArtifact")

if [ -z "$CODE_ARTIFACT_KEY" ]
then
  exit 1
fi

echo "ARTIFACT_DEPLOYMENT_BUCKET: {$ARTIFACT_DEPLOYMENT_BUCKET}"
echo "CODE_ARTIFACT_KEY: {$CODE_ARTIFACT_KEY}"

# deploy with sam
# sam build --profile "$PROFILE" --region "$REGION" --template "./template.yaml"

sam deploy --profile "$PROFILE" --template "./template.yaml" --stack-name "$APP_NAME" --capabilities CAPABILITY_IAM \
  --role-arn "$ROLE_ARN" \
  --parameter-overrides ParameterKey=AmiId,ParameterValue="$AmiId" \
  ParameterKey=AppName,ParameterValue="$APP_NAME" \
  ParameterKey=CommitHash,ParameterValue="$CommitHash" \
  ParameterKey=DeploymentBucket,ParameterValue="$DEPLOYMENT_BUCKET" \
  ParameterKey=Environment,ParameterValue="$ENVIRONMENT" \
  ParameterKey=CodeArtifactKey,ParameterValue="$CODE_ARTIFACT_KEY" \
  ParameterKey=CfnInitArtifactKey,ParameterValue="$CFN_INIT_ARTIFACT" \
  --tags "DeploymentId=\"$DEPLOYMENT_ID\" UserLastModified=\"$USER\" Environment=\"$ENVIRONMENT\""
