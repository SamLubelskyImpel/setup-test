"""
These classes are designed to manage calls to the Momentum/CRM API for activities.
This wrapper classes defined this file should NOT be modified or used by any other resources aside from the SendActivity lambda.
A decision was made to isolate source code for each lambda in order to limit the impact of errors caused by changes to other resources.
"""

import requests
from os import environ
from json import loads
from boto3 import client
from datetime import datetime, timedelta
import logging

ENVIRONMENT = environ.get("ENVIRONMENT")
SECRET_KEY = environ.get("SECRET_KEY")
CRM_API_DOMAIN = environ.get("CRM_API_DOMAIN")
CRM_API_SECRET_KEY = environ.get("UPLOAD_SECRET_KEY")
ACTIVIX_API_DOMAIN = environ.get("ACTIVIX_API_DOMAIN")

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

    def update_activity(self, activity_id, crm_activity_id):
        try:
            response = requests.put(
                url=f"https://{CRM_API_DOMAIN}/activities/{activity_id}",
                json={"crm_activity_id": crm_activity_id},
                headers={
                    "x_api_key": self.api_key,
                    "partner_id": self.partner_id,
                },
            )
            response.raise_for_status()
            logger.info(f"CRM API PUT Activities responded with: {response.status_code}")
            return response.json()
        except Exception as e:
            logger.error(f"Error occured calling CRM API: {e}")


class ActivixApiWrapper:
    """Activix API Wrapper."""

    def __init__(self, **kwargs):
        self.__activity = kwargs.get("activity")
        self.__salesperson = kwargs.get("salesperson")
        self.__api_key = self.get_secrets()
        self.__user_id = self.__activity.get("dealer_integration_partner_metadata", {}).get("userId", "")

    def get_secrets(self):
        secret = secret_client.get_secret_value(
            SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/activix"
        )
        crm_dealer_id = self.__activity["crm_dealer_id"]
        secret = loads(secret["SecretString"])[crm_dealer_id]
        secret_data = loads(secret)

        return secret_data["api_key"]

    def __call_api(self, url, payload=None, method="POST"):
        headers = {
            "Content-Type": "application/json",
            "Authorization": self.__api_key,
        }
        response = requests.request(
            method=method,
            url=url,
            json=payload,
            headers=headers,
        )
        logger.info(f"Response from CRM: {response.status_code}")
        return response

    def __create_appointment(self):
        """Create appointment on CRM."""
        url = "{}/events".format(ACTIVIX_API_DOMAIN)

        dt = datetime.strptime(self.__activity["activity_due_ts"], "%Y-%m-%dT%H:%M:%SZ")
        start_at = dt.strftime("%Y-%m-%dT%H:%M:%S+00:00")
        end_at = (dt + timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S+00:00")
        owner_id = int(self.__salesperson["crm_salesperson_id"]) if self.__salesperson else self.__user_id

        payload = {
            "owner": {
                "id": owner_id
            },
            "start_at": start_at,
            "end_at": end_at,
            "lead_id": int(self.__activity["crm_lead_id"]),
            "title": "Appointment",
            "description": self.__activity["notes"],
            "type": "appointment"
        }

        logger.info(f"Payload to CRM: {payload}")
        response = self.__call_api(url, payload)
        response.raise_for_status()
        response_json = response.json()
        logger.info(f"Response from CRM: {response_json}")

        return str(response_json.get("data", {}).get("id", ""))

    def __insert_note(self):
        """Insert note on CRM."""
        lead_id = int(self.__activity["crm_lead_id"])

        # First API call to the leads endpoint
        url = "{}/leads/{}/notes".format(ACTIVIX_API_DOMAIN, lead_id)

        payload = {
            "content": self.__activity["notes"]
        }

        logger.info(f"Payload to CRM: {payload}")
        response = self.__call_api(url, payload)
        response.raise_for_status()
        response_json = response.json()
        logger.info(f"Response from CRM: {response_json}")

        note_id = str(response_json.get("data", "").get("id", ""))

        # Second API call to communications endpoint
        url_communications = "{}/communications".format(ACTIVIX_API_DOMAIN)
        payload_communications = {
            "lead_id": lead_id,
            "description": self.__activity["notes"]
        }
        logger.info(f"Payload to CRM (communications): {payload_communications}")
        response_communications = self.__call_api(url_communications, payload_communications)
        response_communications.raise_for_status()
        response_json_communications = response_communications.json()
        logger.info(f"Response from CRM (communications): {response_json_communications}")

        return note_id

    def __create_phone_call_task(self):
        """Create phone call task on CRM."""
        url = "{}/tasks".format(ACTIVIX_API_DOMAIN)

        dt = datetime.strptime(self.__activity["activity_due_ts"], "%Y-%m-%dT%H:%M:%SZ")
        date = dt.strftime("%Y-%m-%dT%H:%M:%S+00:00")
        owner_id = int(self.__salesperson["crm_salesperson_id"]) if self.__salesperson else self.__user_id

        payload = {
            "owner": {
                "id": owner_id
            },
            "date": date,
            "lead_id": int(self.__activity["crm_lead_id"]),
            "title": "Phone call task",
            "description": self.__activity["notes"],
            "type": "call"
        }

        logger.info(f"Payload to CRM: {payload}")
        response = self.__call_api(url, payload)
        response.raise_for_status()
        response_json = response.json()
        logger.info(f"Response from CRM: {response_json}")

        return str(response_json.get("data", {}).get("id", ""))

    def create_activity(self):
        """Create activity on CRM."""
        if self.__activity["activity_type"] == "note":
            return self.__insert_note()
        elif self.__activity["activity_type"] == "appointment":
            return self.__create_appointment()
        elif self.__activity["activity_type"] == "outbound_call":
            return
        elif self.__activity["activity_type"] == "phone_call_task":
            return self.__create_phone_call_task()
        else:
            logger.error(
                f"Activix CRM doesn't support activity type: {self.__activity['activity_type']}"
            )
            return None
