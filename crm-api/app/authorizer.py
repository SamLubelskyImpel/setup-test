"""Authorize requests to the crm api gateway."""
import json
import logging
import os
from typing import Any, Dict

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOGLEVEL", "INFO").upper())

is_prod = os.environ.get("ENVIRONMENT", "test") == "prod"


def is_endpoint_allowed(method_arn: str, allowed_endpoints: Any) -> bool:
    """Check if the requested endpoint and method are in the set of allowed endpoints for the partner."""
    if allowed_endpoints == ["*"]:
        return True

    arn_parts = method_arn.split(':')[-1].split('/')
    http_method = arn_parts[2]
    request_path = '/' + '/'.join(arn_parts[3:])

    for allowed_endpoint in allowed_endpoints:
        allowed_method, allowed_path = allowed_endpoint.split('/', 1)
        if http_method != allowed_method:
            continue

        if match_path(request_path, allowed_path):
            return True
    return False


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
    """Take in the API_KEY/partner_id pair sent to the API Gateway and verifies against secrets manager."""
    logger.info(event)

    method_arn = event["methodArn"]

    headers = {k.lower(): v for k, v in event['headers'].items()}
    partner_id = headers.get('partner_id')
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
            SecretId=f"{'prod' if is_prod else 'test'}/crm-api"
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
    allowed_endpoints = secret_data.get("endpoints", [])
    endpoint_allowed = is_endpoint_allowed(method_arn, allowed_endpoints)

    if authorized and endpoint_allowed:
        policy["Statement"][0]["Effect"] = "Allow"
        return {
            "policyDocument": policy, 
            "principalId": partner_id,
            "context": {
                "integration_partner": secret_data.get("integration_partner", None)
            }
        }
    else:
        logger.info(f"Access denied for partner_id {partner_id} to endpoint {method_arn}")
        raise Exception("Unauthorized")


def lambda_handler(event: Any, context: Any) -> Any:
    """Run the authorization lambda."""
    try:
        return _lambda_handler(event, context)
    except Exception:
        logger.exception(event)
        raise
