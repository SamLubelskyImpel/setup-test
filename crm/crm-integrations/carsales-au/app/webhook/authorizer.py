"""Authorize requests to the Carsales AU Webhook."""
import json
import logging
import os
from typing import Any, Dict
import boto3
from botocore.exceptions import ClientError


logger = logging.getLogger()
logger.setLevel(os.environ.get("LOGLEVEL", "INFO").upper())

is_prod = os.environ.get("ENVIRONMENT", "test") == "prod"


def lambda_handler(event: Any, context: Any) -> Any:
    """Take in the API_KEY/client_id pair sent to the API Gateway and verifies against secrets manager."""
    logger.info(event)

    try:
        method_arn = event["methodArn"]

        headers = {k.lower(): v for k, v in event["headers"].items()}
        client_id = headers.get("client_id")
        api_key = headers.get("api_key")

        SM_CLIENT = boto3.client("secretsmanager")

        policy: Dict[str, Any] = {
            "Version": "2012-10-17",
            "Statement": [
                {"Action": "execute-api:Invoke", "Effect": "Deny", "Resource": method_arn}
            ],
        }

        try:
            secret = SM_CLIENT.get_secret_value(
                SecretId=f"{'prod' if is_prod else 'test'}/carsales-au-webhook"
            )
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                return {"policyDocument": policy, "principalId": client_id}
            raise

        try:
            secret = json.loads(secret["SecretString"])[str(client_id)]
            secret_data = json.loads(secret)
        except KeyError:
            logger.exception("Invalid client_id")
            raise Exception("Unauthorized")

        authorized = api_key == secret_data["api_key"]

        if not authorized:
            logger.info(f"Access denied for client_id {client_id} to endpoint {method_arn}")
            raise Exception("Unauthorized")

        policy["Statement"][0]["Effect"] = "Allow"
        return {
            "policyDocument": policy,
            "principalId": client_id
        }

    except Exception:
        logger.exception(event)
        raise
