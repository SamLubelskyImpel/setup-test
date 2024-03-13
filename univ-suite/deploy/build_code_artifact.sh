#!/bin/bash

set -e
cd "$(dirname "$0")" || return


function help() {
  echo "
    Build a universal integrations app code artifact and upload it to S3.

    Usage:
     build_code_artifact.sh <parameters>

     * All parameters are required

    Options:
     -a       the name of the app
     -e       the environment this package is being used for
     -p       an awscli profile
     -r       an AWS region, e.g. us-east-1, eu-west-1

     -h       display this help
    "
  exit 2
}


while getopts a:e:hp:r: option
do
  case "${option}"
    in
    a)app=${OPTARG};;
    e)env=${OPTARG};;
    h)help;;
    p)profile=${OPTARG};;
    r)region=${OPTARG};;
    *)help;;
  esac
done

if [ $OPTIND -eq 1 ]; then help; fi
: ${app?"Must give an app name, -a"}
: ${env?"Must give an environment, -e"}
: ${profile?"Must give a profile, -p"}
: ${region?"Must give a region, -r"}

aws_account=$(aws --profile $profile sts get-caller-identity --query "Account" --output text)
bucket="spincar-deploy-$region"
commit=$(git rev-parse HEAD)
deployment_id="$commit-$(date -u +%Y%m%d%H%M%S)"

# Create the temp files/dirs that will be used to stage all the files.
tmptar=$(mktemp --suffix=.tar)
tmpcodedir=$(mktemp -d)
mkdir $tmpcodedir/app
tmpconfig="$tmpcodedir/app/uiapp.env"  # This is the file that will store the apps runtime environment variables.
trap 'rm --preserve-root --one-file-system -rf "$tmptar"' EXIT  # Clean up on exit, as safely as possible.

cd $tmpcodedir
ln -s "$HOME/universal_integrations/univ-suite/dms_upload_api" ./app
ln -s ./app/dms_upload_api/appspec.yml .  # appspec.yaml MUST be at the root of the artifact.
cp -r "$HOME/universal_integrations/univ-suite/deploy/dms_upload_api/bootstrap/codedeploy" .  # Copy the general codedeploy scripts.
\cp -r "$HOME/universal_integrations/univ-suite/dms_upload_api/bootstrap/codedeploy" .  # Now copy the apps codedeploy scripts. Many shells force interactive cp; using \cp circumvents this.

files=(
	.gitignore
)
pushd ~/universal_integrations > /dev/null
cp --parents --recursive ${files[@]} "$tmpcodedir"/app
popd > /dev/null

flavor="test" && [[ "$env" == "prod" ]] && flavor="prod"
sts_region="us" && [[ "$region" != "us-east-1" ]] && sts_region="eu"
cat > $tmpconfig<< EOF
AWS_METADATA_SERVICE_NUM_ATTEMPTS=5
DEPLOYMENT_ID=$deployment_id
STS_APPLICATION_NAME=$app
STS_COMMIT=$commit
STS_ENVIRONMENT=$env
STS_FLAVOR=$flavor
STS_REGION=$sts_region
STS_TEST=$env
TF_CPP_MIN_LOG_LEVEL=3
VIRTUAL_ENV="/opt/impel/.env"
EOF


# Finally, tar the package.
# tar -chf $tmptar --exclude-backups --exclude-vcs-ignores --exclude=.gitignore --exclude='__pycache__' --exclude='bootstrap' --exclude='venv' --exclude='env' --exclude=template.yaml .

tar -chf $tmptar --exclude-backups --exclude-vcs-ignores -X "$HOME/universal_integrations/univ-suite/deploy/exclude_file.txt" .


s3_uri="s3://$bucket/$aws_account/$env-$app-$deployment_id-code.tar.gz"
gzip --stdout $tmptar | aws --region $region --profile $profile s3 cp - "$s3_uri"  --acl bucket-owner-full-control
echo $s3_uri
