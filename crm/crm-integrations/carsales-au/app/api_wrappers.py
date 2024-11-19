"""
These classes are designed to manage calls to the Carsales/CRM API for activities.
This wrapper classes defined this file should NOT be modified or used by any other resources aside from the SendActivity lambda.
A decision was made to isolate source code for each lambda in order to limit the impact of errors caused by changes to other resources.
"""

import requests
from os import environ
from json import loads
from boto3 import client
import logging
import base64

ENVIRONMENT = environ.get("ENVIRONMENT")
SECRET_KEY = environ.get("SECRET_KEY")
CRM_API_DOMAIN = environ.get("CRM_API_DOMAIN")
CRM_API_SECRET_KEY = environ.get("UPLOAD_SECRET_KEY")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
secret_client = client("secretsmanager")


class CrmApiWrapper:
    """CRM API Wrapper."""

    def __init__(self) -> None:
        self.partner_id = CRM_API_SECRET_KEY
        self.api_key = self.get_secrets()

    def get_secrets(self):
        secret = secret_client.get_secret_value(
            SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/crm-api"
        )
        secret = loads(secret["SecretString"])[CRM_API_SECRET_KEY]
        secret_data = loads(secret)
        logger.info(f"secret_data is: {secret_data}")
        return secret_data["api_key"]

    def __run_get(self, endpoint: str):
        response = requests.get(
            url=f"https://{CRM_API_DOMAIN}/{endpoint}",
            headers={
                "x_api_key": self.api_key,
                "partner_id": self.partner_id,
            },
        )
        response.raise_for_status()
        return response.json()

    def get_salesperson(self, lead_id: int):
        salespersons = self.__run_get(f"leads/{lead_id}/salespersons")
        if not salespersons:
            return None

        return salespersons[0]

class CarsalesApiWrapper:
    """Carsales API Wrapper."""

    def __init__(self, **kwargs):
        self.__api_url, self.__api_username, self.__api_password = self.get_secrets()
        self.__activity = kwargs.get("activity")

    def get_secrets(self):
        secret = secret_client.get_secret_value(
            SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/crm-integrations-partner"
        )
        secret = loads(secret["SecretString"])[str(SECRET_KEY)]
        secret_data = loads(secret)
        logger.info(f"secret_data: {secret_data}")
        return (
            secret_data["API_URL"],
            secret_data["API_USERNAME"],
            secret_data["API_PASSWORD"]
        )

    def __call_api(self, url, payload=None, method="POST"):
        auth_string = f"{self.__api_username}:{self.__api_password}"
        encoded_auth = base64.b64encode(auth_string.encode('utf-8')).decode('utf-8')

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Basic {encoded_auth}",
        }

        try:
            response = requests.request(
                method=method,
                url=url,
                json=payload,
                headers=headers,
            )
            logger.info(f"Response from CRM: {response.status_code}")
            return response
        except Exception as e:
            logger.error(f"Error calling API: {e}")
            raise

    def __create_outbound_call(self):
        """Create outbound call on CRM."""
        crm_lead_id = self.__activity.get('crm_lead_id')
        url = "{}/v2/lead/{}/status/contact".format(self.__api_url, crm_lead_id)

        try:
            response = self.__call_api(url)
            response.raise_for_status()
            response_json = response.json()
            logger.info(f"Response from CRM: {response_json}")
        except Exception as e:
            logger.error(f"Failed to create outbound call for lead {crm_lead_id}: {e}")
            return

    def create_activity(self):
        """Create activity on CRM."""
        if self.__activity["activity_type"] == "outbound_call":
            return self.__create_outbound_call()
        else:
            logger.error(
                f"Carsales CRM doesn't support activity type: {self.__activity['activity_type']}"
            )
            return None

    def get_salespersons(self):
        url = f"{self.__api_url}/employee"
        response = self.__call_api(url=url, method="GET")
        response.raise_for_status()
        response_json = response.json()
        logger.info(f"Response from CRM: {response_json}")

        return response_json
