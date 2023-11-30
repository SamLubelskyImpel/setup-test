import boto3
from os import environ
from json import loads


ENVIRONMENT = environ.get("ENVIRONMENT")
SECRET_KEY = environ.get("SECRET_KEY")
CRM_API_URL = environ.get("CRM_API_URL")

secret_client = boto3.client("secretsmanager")


def get_dealerpeak_credentials():
    """Get DealerPeak API secrets."""
    secret = secret_client.get_secret_value(
        SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/crm-integration-partner"
    )
    secret = loads(secret["SecretString"])[str(SECRET_KEY)]
    secret_data = loads(secret)

    return secret_data["API_URL"], secret_data["API_USERNAME"], secret_data["API_PASSWORD"]

def get_crm_api_credentials():
    """Get CRM API secrets."""
    partner_id = "impel"
    secret = secret_client.get_secret_value(
        SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/crm-api"
    )
    secret = loads(secret["SecretString"])[partner_id]
    secret_data = loads(secret)

    return CRM_API_URL, partner_id, secret_data["api_key"]