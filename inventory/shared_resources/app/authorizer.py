"""Authorize requests to the InventoryInternalApi."""
import json
import logging
import os

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOGLEVEL", "INFO").upper())

is_prod = os.environ.get("ENVIRONMENT", "test") == "prod"


def _lambda_handler(event, context):
    """Take in the API_KEY/client_id pair sent to our API Gateway and verify against Secrets Manager."""
    logger.info("Received event: %s", event)
    method_arn = event["methodArn"]

    headers = {k.lower(): v for k, v in event['headers'].items()}
    client_id = headers.get('client_id')
    api_key = headers.get('x_api_key')
    
    sm_client = boto3.client("secretsmanager")

    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {"Action": "execute-api:Invoke", "Effect": "Deny", "Resource": method_arn}
        ],
    }

    try:
        secret = sm_client.get_secret_value(
            SecretId=f"{'prod' if is_prod else 'test'}/InventoryInternalApi"
        )
        secret = json.loads(secret["SecretString"]).get(str(client_id), {})
        if not secret:
            logger.error("Invalid client_id: %s, secret is not present", client_id)       
            raise Exception("Unauthorized")
        else:
            secret_data = json.loads(secret)
    except ClientError as e:
        logger.error("Error retrieving secret: %s", e)
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            return {"policyDocument": policy, "principalId": client_id}
        else:
            raise
    except KeyError:
        logger.error("Invalid client_id: %s", client_id)
        raise Exception("Unauthorized")
    except Exception as e:
        logger.exception("Unexpected error occurred")
        raise

    authorized = api_key == secret_data.get("api_key")
    logger.info("Authorization status: %s", "Authorized" if authorized else "Unauthorized")
    
    if authorized:
        policy["Statement"][0]["Effect"] = "Allow"
        return {"policyDocument": policy, "principalId": client_id}
    
    logger.warning("Unauthorized access attempt by client_id: %s", client_id)
    raise Exception("Unauthorized")

def lambda_handler(event, context):
    """Run the authorization Lambda."""
    try:
        return _lambda_handler(event, context)
    except Exception as e:
        logger.exception("Exception in lambda_handler: %s", e)
        raise
