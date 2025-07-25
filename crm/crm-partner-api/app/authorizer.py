"""Authorize requests to the crm partner api gateway."""
import json
import logging
import os
from typing import Any, Dict

import boto3
from base64 import b64decode
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOGLEVEL", "INFO").upper())

is_prod = os.environ.get("ENVIRONMENT", "test") == "prod"


def decode_basic_auth(basic_auth_str: str):
    """Decode a given basic auth."""
    try:
        input_auth_encoded = basic_auth_str.split("Basic ")[1]
        input_auth_str = b64decode(input_auth_encoded).decode("utf-8")
        client_id, input_auth = input_auth_str.split(":")
        return client_id, input_auth
    except (ValueError, IndexError):
        logger.exception(f"Malformed input basic auth {basic_auth_str}")
        return None, None


def _lambda_handler(event: Any, context: Any) -> Any:
    """Take in the authorization headers sent to the API Gateway and verifies against secrets manager."""
    logger.info(event)

    method_arn = event["methodArn"]

    policy: Dict[str, Any] = {
        "Version": "2012-10-17",
        "Statement": [
            {"Action": "execute-api:Invoke", "Effect": "Deny", "Resource": method_arn}
        ],
    }

    basic_header = event.get('headers', {}).get('Authorization', None)
    activix_header = event.get('headers', {}).get('X-Activix-Signature', None)
    authorized = False

    if basic_header:
        partner_id, api_key = decode_basic_auth(basic_header)

        SM_CLIENT = boto3.client("secretsmanager")

        try:
            secret = SM_CLIENT.get_secret_value(
                SecretId=f"{'prod' if is_prod else 'test'}/crm-partner-api"
            )
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                return {"policyDocument": policy, "principalId": partner_id}
            else:
                raise
        try:
            secret = json.loads(secret["SecretString"])[str(partner_id)]
            secret_data = json.loads(secret)
        except KeyError:
            logger.exception("Invalid partner_id")
            raise Exception("Unauthorized")

        authorized = api_key == secret_data["api_key"]

    # The Activix signature is created with the SHA256 algorithm, so authentication must occur within the Lambda function to access the request body.
    if activix_header:
        partner_id = 'activix'

    if authorized or activix_header:
        policy["Statement"][0]["Effect"] = "Allow"
        return {"policyDocument": policy, "principalId": partner_id}

    raise Exception("Unauthorized")


def lambda_handler(event: Any, context: Any) -> Any:
    """Run the authorization lambda."""
    try:
        return _lambda_handler(event, context)
    except Exception:
        logger.exception(event)
        raise
