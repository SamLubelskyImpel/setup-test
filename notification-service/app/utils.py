import boto3
import logging
from json import loads
from os import environ

s3_client = boto3.client('s3')
ENVIRONMENT = environ.get("ENVIRONMENT")
INTEGRATIONS_BUCKET = environ.get("INTEGRATIONS_BUCKET")

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


def is_writeback_disabled(partner_name: str) -> bool:
    """Check if writeback is disabled for the partner."""
    try:
        s3_key = f"configurations/{'prod' if ENVIRONMENT == 'prod' else 'test'}_GENERAL.json"
        config = loads(
            s3_client.get_object(
                Bucket=INTEGRATIONS_BUCKET,
                Key=s3_key
            )["Body"].read().decode("utf-8")
        )
        logger.info(f"Config: {config}")
        disabled_partners = config["writeback_disabled_partners"]
        if partner_name in disabled_partners:
            return True

    except Exception as e:
        logger.error(f"Error checking writeback status: {str(e)}")
        raise

    return False
