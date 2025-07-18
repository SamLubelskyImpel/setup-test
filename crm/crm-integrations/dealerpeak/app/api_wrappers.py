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

syntax error
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

    def get_salesperson(self, lead_id: int):
        response = requests.get(
            url=f"https://{CRM_API_DOMAIN}/leads/{lead_id}/salespersons",
            headers={
                "x_api_key": self.api_key,
                "partner_id": self.partner_id,
            }
        )
        response.raise_for_status()
        logger.info(f"CRM API responded with: {response.status_code}")
        if response.status_code != 200:
            raise Exception(f"Error getting salespersons for lead {lead_id}: {response.text}")

        salespersons = response.json()
        if not salespersons:
            raise Exception(f"No salespersons found for lead {lead_id}")
        return salespersons[0]

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


class DealerpeakApiWrapper:
    """Dealerpeak API Wrapper."""
    def __init__(self, **kwargs):
        self.__url, self.__username, self.__password = self.get_secrets()
        self.__activity = kwargs.get("activity")
        self.__salesperson = kwargs.get("salesperson")
        self.__dealer_group_id = self.__activity["crm_dealer_id"].split("__")[0]

    def get_secrets(self):
        secret = secret_client.get_secret_value(
            SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/crm-integrations-partner"
        )
        secret = loads(secret["SecretString"])[str(SECRET_KEY)]
        secret_data = loads(secret)

        return secret_data["API_URL"], secret_data["API_USERNAME"], secret_data["API_PASSWORD"]

    def __insert_note(self):
        payload = {
            "addedBy_UserID": self.__salesperson["crm_salesperson_id"],
            "leadID": self.__activity["crm_lead_id"],
            "note": self.__activity["notes"],
        }
        logging.info(f"Payload to CRM: {payload}")

        res = requests.post(
            url=f"{self.__url}/dealergroup/{self.__dealer_group_id}/customer/{self.__activity['crm_consumer_id']}/notes",
            json=payload,
            auth=HTTPBasicAuth(self.__username, self.__password)
        )

        res.raise_for_status()
        return res.json()["noteID"]

    def __get_task_type(self):
        return {
            "appointment": 2,
            "phone_call_task": 3,
            "outbound_call": 3,
        }[self.__activity["activity_type"]]

    def __insert_task(self):
        payload = {
            "agent": {
                "userID": self.__salesperson["crm_salesperson_id"]
            },
            "customer": {
                "userID": self.__activity["crm_consumer_id"]
            },
            "description": self.__activity["notes"],
            "taskType": {
                "typeID": self.__get_task_type()
            },
            "taskResult": {}
        }

        if self.__activity["activity_type"] in ("appointment", "phone_call_task"):
            payload["dueDate"] = self.__activity["activity_due_ts"]
        elif self.__activity["activity_type"] == "outbound_call":
            payload["dueDate"] = self.__activity["activity_requested_ts"]
            payload["completedDate"] = self.__activity["activity_requested_ts"]
            payload["taskResult"]["resultID"] = 5

        logging.info(f"Payload to CRM: {payload}")

        res = requests.post(
            url=f"{self.__url}/dealergroup/{self.__dealer_group_id}/tasks",
            json=payload,
            auth=HTTPBasicAuth(self.__username, self.__password)
        )

        res.raise_for_status()
        return res.json()["task"]["taskID"]

    def create_activity(self):
        if self.__activity["activity_type"] == "note":
            return self.__insert_note()
        return self.__insert_task()
