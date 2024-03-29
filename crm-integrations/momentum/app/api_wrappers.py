"""
These classes are designed to manage calls to the Momentum/CRM API for activities.
This wrapper classes defined this file should NOT be modified or used by any other resources aside from the SendActivity lambda.
A decision was made to isolate source code for each lambda in order to limit the impact of errors caused by changes to other resources.
"""

import requests
from os import environ
from json import loads
from boto3 import client
from uuid import uuid4
from datetime import datetime
from typing import Tuple
import logging
import pytz

ENVIRONMENT = environ.get("ENVIRONMENT")
SECRET_KEY = environ.get("SECRET_KEY")
CRM_API_DOMAIN = environ.get("CRM_API_DOMAIN")
CRM_API_SECRET_KEY = environ.get("UPLOAD_SECRET_KEY")
OUTBOUND_CALL_DEFAULT_MESSAGE = "Sales AI email/text sent. Clock stopped."

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

    def get_appointments(self, lead_id):
        response_json = self.__run_get(f"leads/{lead_id}/activities")
        return [activity for activity in response_json if activity["activity_type"] == "appointment"]


class MomentumApiWrapper:
    """Momentum API Wrapper."""

    def __init__(self, **kwargs):
        self.__login_url, self.__master_key = self.get_secrets()
        self.__activity = kwargs.get("activity")
        self.__api_token, self.__api_url = self.get_token()
        self.__salesperson = kwargs.get("salesperson")
        self.__dealer_timezone = self.__activity["dealer_timezone"]

    def get_token(self):
        response = requests.get(
            url="{}/{}".format(self.__login_url, self.__activity["crm_dealer_id"]),
            headers={
                "Content-Type": "application/json",
                "MOM-ApplicationType": "V",
                "MOM-Api-Key": self.__master_key,
            },
        )
        response.raise_for_status()
        response_json = response.json()
        logger.info(f"Response from CRM - Get Token: {response_json}")

        api_token = response_json["apiToken"]
        api_url = response_json["endPoint"]
        if ENVIRONMENT != "prod":
            api_url += "/v1"

        return api_token, api_url

    def get_secrets(self):
        secret = secret_client.get_secret_value(
            SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/crm-integrations-partner"
        )
        secret = loads(secret["SecretString"])[str(SECRET_KEY)]
        secret_data = loads(secret)

        return (
            secret_data["API_URL"],
            secret_data["API_MASTER_KEY"]
        )

    def __call_api(self, url, payload=None, method="POST"):
        headers = {
            "Content-Type": "application/json",
            "MOM-ApplicationType": "V",
            "MOM-Api-Token": self.__api_token,
        }
        response = requests.request(
            method=method,
            url=url,
            json=payload,
            headers=headers,
        )
        logger.info(f"Response from CRM: {response.status_code}")
        return response

    def convert_utc_to_timezone(self, input_ts: str) -> Tuple[str, str]:
        """Convert UTC timestamp to dealer's local time."""
        utc_datetime = datetime.strptime(input_ts, '%Y-%m-%dT%H:%M:%SZ')
        utc_datetime = pytz.utc.localize(utc_datetime)

        if not self.__dealer_timezone:
            logger.warning("Dealer timezone not found for crm_dealer_id: {}".format(self.__activity["crm_dealer_id"]))
            new_ts = utc_datetime.strftime('%Y-%m-%d %H:%M:%S')
        else:
            # Get the dealer timezone object, convert UTC datetime to dealer timezone
            dealer_tz = pytz.timezone(self.__dealer_timezone)
            dealer_datetime = utc_datetime.astimezone(dealer_tz)
            new_ts = dealer_datetime.strftime('%Y-%m-%d %H:%M:%S')

        timestamp_str = new_ts.split(" ")
        return timestamp_str[0], timestamp_str[1]

    def __reschedule_appointment(self, url, appt_date, appt_time, payload):
        """Reschedule appointment on CRM."""
        crm_api = CrmApiWrapper()
        existing_appointments = crm_api.get_appointments(self.__activity["lead_id"])
        logger.info(f"Existing appointments: {existing_appointments}")

        appointment = next((appt for appt in existing_appointments if appt["crm_activity_id"]), None)
        if not appointment:
            logger.error(f"No scheduled appointments found for lead_id: {self.__activity['lead_id']}.")
            raise Exception(f"Conflict creating appointment for lead_id: {self.__activity['lead_id']}. No existing scheduled appointments found.")

        cancel_url = "{}/lead/{}/appointment/sales/{}".format(
            self.__api_url, self.__activity["crm_lead_id"], appointment["crm_activity_id"]
        )
        logger.info(f"Cancelling appointment. URL: {cancel_url}")
        response = self.__call_api(cancel_url, method="DELETE")
        response.raise_for_status()

        logger.info("Retry creating appointment.")
        response = self.__call_api(url, payload)
        response.raise_for_status()
        return response.json()

    def __create_appointment(self):
        """Create appointment on CRM."""
        url = "{}/lead/{}/appointment/sales".format(self.__api_url, self.__activity["crm_lead_id"])
        appt_date, appt_time = self.convert_utc_to_timezone(self.__activity["activity_due_ts"])
        request_id = str(uuid4())

        payload = {
            "apptDate": appt_date,
            "apptTime": appt_time,
            "salesmanApiID": self.__salesperson["crm_salesperson_id"],
            "note": self.__activity["notes"],
            "externalCreatedByName": "ImpelCRM",
            "externalID": request_id,
        }

        logger.info(f"Payload to CRM: {payload}")
        response = self.__call_api(url, payload)
        if response.status_code == 409:
            logger.warning(f"Appointment already exists for lead_id: {self.__activity['crm_lead_id']}. Rescheduling...")
            response_json = self.__reschedule_appointment(url, appt_date, appt_time, payload)
        else:
            response.raise_for_status()
            response_json = response.json()

        logger.info(f"Response from CRM: {response_json}")

        return str(response_json.get("appointmentApiID", ""))

    def __insert_note(self):
        """Insert note on CRM."""
        url = "{}/lead/{}/contact/remark".format(self.__api_url, self.__activity["crm_lead_id"])

        payload = {
            "remark": self.__activity["notes"]
        }
        logger.info(f"Payload to CRM: {payload}")
        response = self.__call_api(url, payload)
        response.raise_for_status()
        response_json = response.json()
        logger.info(f"Response from CRM: {response_json}")

        return str(response_json.get("leadRemarkID", ""))

    def __create_outbound_call(self):
        """Create outbound call on CRM."""
        url = "{}/lead/{}/contact/remark/clockstop".format(self.__api_url, self.__activity["crm_lead_id"])

        payload = {
            "remark": self.__activity["notes"] if self.__activity["notes"] else OUTBOUND_CALL_DEFAULT_MESSAGE
        }
        logger.info(f"Payload to CRM: {payload}")
        response = self.__call_api(url, payload)
        response.raise_for_status()
        response_json = response.json()
        logger.info(f"Response from CRM: {response_json}")

        return str(response_json.get("leadRemarkID", ""))

    def create_activity(self):
        """Create activity on CRM."""
        if self.__activity["activity_type"] == "note":
            return self.__insert_note()
        elif self.__activity["activity_type"] == "appointment":
            return self.__create_appointment()
        elif self.__activity["activity_type"] == "outbound_call":
            return self.__create_outbound_call()
        else:
            logger.error(
                f"Momentum CRM doesn't support activity type: {self.__activity['activity_type']}"
            )
            return None
