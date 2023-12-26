"""
These classes are designed to manage calls to the Dealerpeak/CRM API for activities.
This wrapper classes defined this file should NOT be modified or used by any other resources aside from the SendActivity lambda.
A decision was made to isolate source code for each lambda in order to limit the impact of errors caused by changes to other resources.
"""

import boto3
import logging
from os import environ
import requests
from json import loads
from requests.auth import HTTPBasicAuth

ENVIRONMENT = environ.get("ENVIRONMENT")
SECRET_KEY = environ.get("SECRET_KEY")
CRM_API_DOMAIN = environ.get("CRM_API_DOMAIN")
CRM_API_SECRET_KEY = environ.get("UPLOAD_SECRET_KEY")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
secret_client = boto3.client("secretsmanager")


class CRMApiError(Exception):
    pass


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

        return secret_data["api_key"]

    def __run_get(self, endpoint: str):
        res = requests.get(
            url=f"https://{CRM_API_DOMAIN}/{endpoint}",
            headers={
                "x_api_key": self.api_key,
                "partner_id": self.partner_id,
            }
        )
        res.raise_for_status()
        return res.json()

    # def get_salesperson(self, lead_id: int):
    #     return self.__run_get(f"leads/{lead_id}/salespersons")[0]

    def update_activity(self, activity_id, crm_activity_id):
        try:
            res = requests.put(
                url=f"https://{CRM_API_DOMAIN}/activities/{activity_id}",
                json={
                    "crm_activity_id": crm_activity_id
                },
                headers={
                    "x_api_key": self.api_key,
                    "partner_id": self.partner_id,
                }
            )
            res.raise_for_status()
            return res.json()
        except Exception as e:
            logger.error(f"Error occured calling CRM API: {e}")
            raise CRMApiError(f"Error occured calling CRM API: {e}")


class ReyreyApiWrapper:
    """ReyRey API Wrapper."""
    def __init__(self, **kwargs):
        self.__url, self.__username, self.__password = self.get_secrets()
        self.__activity = kwargs.get("activity")
        self.__store_number, self.__area_number, self.__dealer_number = self.__activity["crm_dealer_id"].split("_")

    def get_secrets(self):
        secret = secret_client.get_secret_value(
            SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/crm-integrations-partner"
        )
        secret = loads(secret["SecretString"])[str(SECRET_KEY)]
        secret_data = loads(secret)

        return secret_data["API_URL"], secret_data["API_USERNAME"], secret_data["API_PASSWORD"]

    def __insert_note(self):
        # payload = {
        #     "addedBy_UserID": self.__salesperson["crm_salesperson_id"],
        #     "leadID": self.__activity["crm_lead_id"],
        #     "note": self.__activity["notes"],
        # }
        # logging.info(f"Payload to CRM: {payload}")

        # res = requests.post(
        #     url=f"{self.__url}/dealergroup/{self.__dealer_group_id}/customer/{self.__activity['crm_consumer_id']}/notes",
        #     json=payload,
        #     auth=HTTPBasicAuth(self.__username, self.__password)
        # )

        # res.raise_for_status()
        # return res.json()["noteID"]
        pass

    def create_activity(self):
        # if self.__activity["activity_type"] == "note":
        #     return self.__insert_note()
        # elif self.__activity["activity_type"] == "appointment":
        #     return self.__create_appointment()
        # elif self.__activity["activity_type"] == "outbound_call":
        #     return self.__create_outbound_call()
        # else:
        #     logger.error(f"ReyRey CRM doesn't support activity type: {self.__activity['activity_type']}")
        #     return None
        pass