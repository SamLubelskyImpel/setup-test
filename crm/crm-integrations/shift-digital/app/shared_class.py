import logging
import requests
from os import environ
from json import loads
from boto3 import client

ENVIRONMENT = environ.get("ENVIRONMENT", "test")
CRM_API_SECRET_KEY = environ.get("UPLOAD_SECRET_KEY")


logging.basicConfig(level=logging.INFO)
secret_client = client("secretsmanager")

class CRMApiError(Exception):
    pass

class BaseClass:
    def __init__(self) -> None:
        self.partner_id = CRM_API_SECRET_KEY
        self.api_key = self._get_secrets("crm-api", self.partner_id)["api_key"]

    def _get_secrets(self, secret_name, secret_data_key):
        """Retrieve API secret from AWS Secrets Manager."""
        secret = secret_client.get_secret_value(
            SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/{secret_name}"
        )
        secret_data = loads(secret["SecretString"])
        return loads(secret_data[secret_data_key])

    def call_crm_api(self, url):
        """Call CRM API."""
        try:
            logging.info(f"Make crm-api call on this URL: {url}")
            response = requests.get(
                url=url,
                headers={
                    "x_api_key": self.api_key,
                    "partner_id": self.partner_id,
                },
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logging.error(f"Error occurred calling CRM API: {e}")
            raise CRMApiError(f"Error occurred calling CRM API: {e}")
