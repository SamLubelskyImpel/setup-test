"""Secrets manager functionality."""
from os import environ
import boto3
from json import loads
from botocore.exceptions import ClientError
from base64 import b64decode
from api.cloudwatch import get_logger
_logger = get_logger()


def get_secret(secret_id: str, region_name: str):
    """Get a secret from secrets manager."""
    try:
        sm_client = boto3.session.Session(region_name=region_name).client(
            "secretsmanager"
        )
        secret = sm_client.get_secret_value(SecretId=secret_id)
    except ClientError:
        _logger.exception(f"Error getting secret {secret_id}")
        raise
    return secret


def decode_basic_auth(basic_auth_str: str):
    """ Decode a given basic auth. """
    try:
        input_auth_encoded = basic_auth_str.split("Basic ")[1]
        input_auth_str = b64decode(input_auth_encoded).decode("utf-8")
        client_id, input_auth = input_auth_str.split(":")
        return client_id, input_auth
    except (ValueError, IndexError):
        _logger.exception(f"Malformed input basic auth {basic_auth_str}")
        return None, None


def check_basic_auth(client_id: str, input_auth: str):
    """Check if an api key is valid."""
    ENV = environ.get("ENV", "test")
    REGION_NAME = environ.get("REGION_NAME", "us-east-1")

    SECRET_ID = f"{ENV}/DMSUploadAPI"
    secret = get_secret(SECRET_ID, REGION_NAME)

    try:
        secret_value = loads(secret["SecretString"])[client_id]
    except KeyError:
        _logger.exception(f"Unknown client id {client_id}")
        return False

    try:
        expected_auth = loads(secret_value)["api_key"]
    except KeyError:
        _logger.exception(
            f"Malformed secret {secret_value} for client {client_id}"
        )
        raise

    if input_auth != expected_auth:
        _logger.exception(f"Wrong auth value input for client {client_id}")
        return False

    return True
