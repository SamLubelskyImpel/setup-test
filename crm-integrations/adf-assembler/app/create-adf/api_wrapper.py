import boto3
import logging
from os import environ
import requests
from json import loads

ENVIRONMENT = environ.get("ENVIRONMENT")
SECRET_KEY = environ.get("SECRET_KEY")
CRM_API_DOMAIN = environ.get("CRM_API_DOMAIN")
CRM_API_SECRET_KEY = environ.get("UPLOAD_SECRET_KEY")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
secret_client = boto3.client("secretsmanager")


class CRMApiError(Exception):
    pass


class ApiWrapper:
    """API Wrapper."""

    def __init__(self) -> None:
        self.partner_id = CRM_API_SECRET_KEY
        self.api_key = self.get_secrets()

    def get_secrets(self):
        secret = secret_client.get_secret_value(
            SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/crm-api"
        )
        secret = loads(secret["SecretString"])[CRM_API_SECRET_KEY]
        secret_data = loads(secret)

        return secret_data["api_key"]

    def get_lead(self, lead_id):
        try:
            vehicle = requests.get(
                url=f"https://{CRM_API_DOMAIN}/leads/{lead_id}",
                headers={
                    "x_api_key": self.api_key,
                    "partner_id": self.partner_id,
                },
            )
            vehicle.raise_for_status()
            
            consumer = requests.get(
                url=f"https://{CRM_API_DOMAIN}/consumers/{vehicle.json().get('consumer_id')}",
                headers={
                    "x_api_key": self.api_key,
                    "partner_id": self.partner_id,
                },
            )
            consumer.raise_for_status()

            dealer = requests.get(
                url=f"https://{CRM_API_DOMAIN}/dealers/{consumer.json().get('dealer_id')}",
                headers={
                    "x_api_key": self.api_key,
                    "partner_id": self.partner_id,
                },
            )
            dealer.raise_for_status()

            return vehicle.json() | consumer.json() | dealer.json()
        except Exception as e:
            logger.error(f"Error occured calling CRM API: {e}")
            raise CRMApiError(f"Error occured calling CRM API: {e}")
