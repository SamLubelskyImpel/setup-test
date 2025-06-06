import boto3
import logging
from json import loads
from os import environ

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
sm_client = boto3.client('secretsmanager')


def get_secret(secret_name: str, secret_value: str) -> dict:
    """Get the secret value from Secrets Manager."""
    try:
        secret = sm_client.get_secret_value(SecretId=secret_name)
        secret = loads(secret["SecretString"])[secret_value]
        return loads(secret)
    except Exception as e:
        logger.error(f"Error getting secret: {e}")
        raise
