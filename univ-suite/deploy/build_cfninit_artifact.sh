#!/bin/bash

set -e
cd "$(dirname "$0")" || return


function help() {
  echo "
    Build a CloudFormation Init (cfn-init) artifact and upload it to S3.

    Usage:
     build_cfninit_artifact.sh <parameters>

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


while getopts a:b:e:hp:r: option
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
deployment_id="$(git rev-parse HEAD)-$(date -u +%Y%m%d%H%M%S)"

tmptar=$(mktemp --suffix=.tar)
trap 'rm --preserve-root --one-file-system -rf "$tmptar"' EXIT  # Clean up on exit, as safely as possible.

# First tar the general bootstrap directory, then update the tar with the app-specific one, if it exists.
tar -cf $tmptar --exclude-backups --exclude-vcs-ignores --exclude='*.pyc' --exclude='__pycache__' -C dms_upload_api/bootstrap/cfn_init .
if [ -d "../$app/bootstrap/universal_integration_app/cfn_init" ]; then
  tar -rf $tmptar --exclude-backups --exclude-vcs-ignores --exclude='*.pyc' --exclude='__pycache__' -C ../$app/deploy/dms_upload_api/bootstrap/cfn_init .
fi

s3_uri="s3://$bucket/$aws_account/$env-$app-$deployment_id-cfninit.tar.gz"
gzip --stdout $tmptar | aws --region $region --profile $profile s3 cp - "$s3_uri" --acl bucket-owner-full-control
echo $s3_uri
