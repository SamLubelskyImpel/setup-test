"""
These classes are designed to manage calls to the Eskimo/CRM API for activities.
This wrapper classes defined this file should NOT be modified or used by any other resources aside from the SendActivity lambda.
A decision was made to isolate source code for each lambda in order to limit the impact of errors caused by changes to other resources.
"""

import requests
from os import environ
from json import loads
from boto3 import client
import logging


ENVIRONMENT = environ.get("ENVIRONMENT")
SECRET_KEY = environ.get("SECRET_KEY")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
secret_client = client("secretsmanager")
ESKIMO_API_URL = "https://eskimo-software.com/api/salesai"


class EskimoApiWrapper:
    """Eskimo API Wrapper."""

    def __init__(self, **kwargs):
        self.__api_url, self.__api_token, self.__account_id = self.get_secrets()
        self.__activity = kwargs.get("activity")

    def get_secrets(self):
        secret = secret_client.get_secret_value(
            SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/crm-integrations-partner"
        )
        secret = loads(secret["SecretString"])[str(SECRET_KEY)]
        secret_data = loads(secret)

        return (
            secret_data["API_URL"],
            secret_data["API_PASSWORD"],
            secret_data["ACCOUNT_ID"],
        )

    def __call_api(self, payload=None, method="POST"):
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.__api_token}",
        }
        response = requests.request(
            method=method,
            url=self.__api_url,
            json=payload,
            headers=headers,
        )
        logger.info(f"Response from CRM: {response.status_code} {response.text}")
        return response

    def __insert_note(self):
        """Insert note on CRM."""
        payload = {
            "accountId": self.__account_id,
            "customerId": self.__activity["consumer_id"],
            "note": self.__activity["notes"]
        }
        logger.info(f"Payload to CRM: {payload}")
        response = self.__call_api(payload)
        response.raise_for_status()

        return response.text

    def create_activity(self):
        """Create activity on CRM."""
        if self.__activity["activity_type"] == "note":
            return self.__insert_note()
        else:
            logger.error(
                f"Eskimo CRM doesn't support activity type: {self.__activity['activity_type']}"
            )
            return None
