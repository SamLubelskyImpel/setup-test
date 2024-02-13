"""
These classes are designed to manage calls to the Momentum/CRM API for activities.
This wrapper classes defined this file should NOT be modified or used by any other resources aside from the SendActivity lambda.
A decision was made to isolate source code for each lambda in order to limit the impact of errors caused by changes to other resources.
"""

import requests
from os import environ
from json import loads
from boto3 import client
from datetime import datetime
from logging import getLogger, info
from requests.auth import HTTPBasicAuth


ENVIRONMENT = environ.get("ENVIRONMENT")
SECRET_KEY = environ.get("SECRET_KEY")
CRM_API_DOMAIN = environ.get("CRM_API_DOMAIN")
CRM_API_SECRET_KEY = environ.get("UPLOAD_SECRET_KEY")

logger = getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
secret_client = client("secretsmanager")


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
            },
        )
        res.raise_for_status()
        return res.json()

    def get_salesperson(self, lead_id: int):
        return self.__run_get(f"leads/{lead_id}/salespersons")[0]

    def update_activity(self, activity_id, crm_activity_id):
        try:
            res = requests.put(
                url=f"https://{CRM_API_DOMAIN}/activities/{activity_id}",
                json={"crm_activity_id": crm_activity_id},
                headers={
                    "x_api_key": self.api_key,
                    "partner_id": self.partner_id,
                },
            )
            res.raise_for_status()
            return res.json()
        except Exception as e:
            logger.error(f"Error occured calling CRM API: {e}")
            raise CRMApiError(f"Error occured calling CRM API: {e}")


class MomentumApiWrapper:
    """Momentum API Wrapper."""

    def __init__(self, **kwargs):
        self.__url, self.__username, self.__password = self.get_secrets()
        self.__api_token, self.__api_end_point = self.get_token()
        self.__activity = kwargs.get("activity")

    def get_token(self):
        res = requests.get(
            url=self.__url,
            headers={
                "Content-Type": "application/json",
                "MOM-ApplicationType": "V",
                "MOM-Api-Key": self.__password,
            },
        )
        res.raise_for_status()
        json_data = res.json()
        api_token = json_data["apiToken"]
        end_point = json_data["endPoint"]

        return api_token, end_point

    def get_secrets(self):
        secret = secret_client.get_secret_value(
            SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/crm-integrations-partner"
        )
        secret = loads(secret["SecretString"])[str(SECRET_KEY)]
        secret_data = loads(secret)

        return (
            secret_data["API_URL"],
            secret_data["API_USERNAME"],
            secret_data["API_PASSWORD"],
        )

    def __call_api(self, payload, endpoint):
        headers = {
            "Content-Type": "application/json",
            "MOM-ApplicationType": "V",
            "MOM-Api-Key": self.__api_token,
        }
        res = requests.post(
            url=f"{self.__api_end_point}/{self.__activity['crm_lead_id']}/{endpoint}",
            json=payload,
            headers=headers,
        )

        res.raise_for_status()
        return res.json()["taskID"]

    def format_dealertime(self, utc_time_str):
        """Format Local Dealer Time."""
        # utc_time_str == Local Dealer Time
        utc_time = datetime.strptime(utc_time_str, "%Y-%m-%dT%H:%M:%SZ")
        apptDate = utc_time.strftime("%Y-%m-%d")
        apptTime = utc_time.strftime("%H:%M")
        return apptDate, apptTime

    def __create_appointment(self):
        apptDate, apptTime = self.format_dealertime(self.__activity["activity_due_ts"])
        payload = {
            "apptDate": apptDate,
            "apptTime": apptTime,
            "_______managerApiID": "[your-manager-api-id]",
            "salesmanApiID": "ce15f797-4690-4df3-b045-ebdcae71d605",
            "note": self.__activity["notes"],
            "externalCreatedByName": "ImpelCRM",
        }

        return self.__call_api(payload, "appointment/sales")

    def __insert_note(self):
        payload = {"remark": self.__activity["notes"]}
        info(f"Payload to CRM: {payload}")

        return self.__call_api(payload, "contact/remark")

    def __create_outbound_call(self):
        payload = {"remark": self.__activity["notes"]}
        info(f"Payload to CRM: {payload}")

        return self.__call_api(payload, "contact/remark/clockstop")

    def create_activity(self):
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
