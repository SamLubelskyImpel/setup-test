
import logging
import requests
from os import environ
from json import loads
from boto3 import client

ENVIRONMENT = environ.get("ENVIRONMENT", "test")
CRM_API_SECRET_KEY = environ.get("CRM_API_SECRET_KEY")
CRM_API_DOMAIN = environ.get("CRM_API_DOMAIN")

secret_client = client("secretsmanager")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def get_secrets(secret_name, secret_data_key):
    """Retrieve API secret from AWS Secrets Manager."""
    secret = secret_client.get_secret_value(
        SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/{secret_name}"
    )
    secret_data = loads(secret["SecretString"])
    return loads(secret_data[secret_data_key])


def call_crm_api(url):
    """Call CRM API."""
    try:
        logger.info(f"Calling CRM API with URL: {url}")

        secret = get_secrets("crm-api", CRM_API_SECRET_KEY)
        api_key = secret["api_key"]

        response = requests.get(
            url=url,
            headers={
                "x_api_key": api_key,
                "partner_id": CRM_API_SECRET_KEY,
            },
        )

        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"Error occurred calling CRM API: {e}")
        raise e
