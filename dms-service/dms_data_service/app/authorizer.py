"""Authorize requests to the dms data service api gateway."""
import json
import logging
import os

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOGLEVEL", "INFO").upper())

IS_PROD = int(os.environ.get("IS_PROD", 0)) == 1


def _lambda_handler(event, context):
    """Take in the API_KEY/client_id pair sent to our API Gateway and verifies against secrets manager."""
    logger.info(event)

    method_arn = event["methodArn"]
    client_id = event["headers"]["client_id"]
    api_key = event["headers"]["x_api_key"]

    SM_CLIENT = boto3.client("secretsmanager")

    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {"Action": "execute-api:Invoke", "Effect": "Deny", "Resource": method_arn}
        ],
    }

    try:
        secret = SM_CLIENT.get_secret_value(
            SecretId=f"{'prod' if IS_PROD else 'test'}/DmsDataService"
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            return {"policyDocument": policy, "principalId": client_id}
        else:
            raise
    try:
        secret = json.loads(secret["SecretString"])[str(client_id)]
        secret_data = json.loads(secret)
    except KeyError as e:
        logger.exception("Invalid client_id")
        raise Exception("Unauthorized")

    authorized = api_key == secret_data["api_key"]
    if authorized:
        policy["Statement"][0]["Effect"] = "Allow"
        return {"policyDocument": policy, "principalId": client_id}

    raise Exception("Unauthorized")


def lambda_handler(event, context):
    """Run the authorization lambda."""
    try:
        return _lambda_handler(event, context)
    except Exception:
        logger.exception(event)
        raise
