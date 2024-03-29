"""Authorize requests to the adf assembler gateway."""
import json
import logging
import os
from typing import Any, Dict

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOGLEVEL", "INFO").upper())

is_prod = os.environ.get("ENVIRONMENT", "test") == "prod"


def match_path(request_path: str, allowed_path: str) -> bool:
    """Match the request path against the allowed path."""
    request_segments = request_path.strip('/').split('/')
    allowed_segments = allowed_path.strip('/').split('/')

    if len(request_segments) != len(allowed_segments):
        return False

    for request_segment, allowed_segment in zip(request_segments, allowed_segments):
        # If the allowed segment is a placeholder (e.g., "{lead_id}"), we skip the comparison
        # Otherwise, we compare the segments directly
        if allowed_segment.startswith('{') and allowed_segment.endswith('}'):
            continue
        if request_segment != allowed_segment:
            return False

    return True


def _lambda_handler(event: Any, context: Any) -> Any:
    """Take in the API_KEY/action_id pair sent to the API Gateway and verifies against secrets manager."""
    logger.info(event)

    method_arn = event["methodArn"]

    headers = {k.lower(): v for k, v in event['headers'].items()}
    action_id = headers.get('action_id')
    api_key = headers.get('x_api_key')    

    SM_CLIENT = boto3.client("secretsmanager")

    policy: Dict[str, Any] = {
        "Version": "2012-10-17",
        "Statement": [
            {"Action": "execute-api:Invoke", "Effect": "Deny", "Resource": method_arn}
        ],
    }

    try:
        secret = SM_CLIENT.get_secret_value(
            SecretId=f"{'prod' if is_prod else 'test'}/adf-assembler"
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            return {"policyDocument": policy, "principalId": action_id}
        else:
            raise
    try:
        secret = json.loads(secret["SecretString"])[str(action_id)]
        secret_data = json.loads(secret)
    except KeyError:
        logger.exception("Invalid action_id")
        raise Exception("Unauthorized")

    authorized = api_key == secret_data["api_key"]

    if authorized:
        policy["Statement"][0]["Effect"] = "Allow"
        return {"policyDocument": policy, "principalId": action_id}
    else:
        logger.info(f"Access denied for action_id {action_id}")
        raise Exception("Unauthorized")


def lambda_handler(event: Any, context: Any) -> Any:
    """Run the authorization lambda."""
    try:
        return _lambda_handler(event, context)
    except Exception:
        logger.exception(event)
        raise
