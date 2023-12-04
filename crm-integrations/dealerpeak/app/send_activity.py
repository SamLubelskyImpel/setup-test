"""Send activity to DealerPeak."""

import boto3
import logging
from os import environ
from json import loads
import requests
from requests.auth import HTTPBasicAuth

ENVIRONMENT = environ.get("ENVIRONMENT")
SECRET_KEY = environ.get("SECRET_KEY")
CRM_API_URL = environ.get("CRM_API_URL")
CRM_API_SECRET_KEY = environ.get("UPLOAD_SECRET_KEY")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
secret_client = boto3.client("secretsmanager")


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
            url=f"{CRM_API_URL}/{endpoint}",
            headers={
                "x_api_key": self.api_key,
                "partner_id": self.partner_id,
            }
        )
        res.raise_for_status()
        return res.json()

    def get_lead(self, lead_id: int):
        return self.__run_get(f"leads/{lead_id}")

    def get_salesperson(self, lead_id: int):
        return self.__run_get(f"leads/{lead_id}/salespersons")[0]

    def get_consumer(self, consumer_id: int):
        return self.__run_get(f"consumers/{consumer_id}")

    def update_activity(self, activity_id, crm_activity_id):
        res = requests.put(
            url=f"{CRM_API_URL}/activities/{activity_id}",
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


class DealerpeakApiWrapper:
    """Dealerpeak API Wrapper."""
    def __init__(self, **kwargs):
        self.__url, self.__username, self.__password = self.get_secrets()
        self.__activity = kwargs.get("activity")
        self.__salesperson = kwargs.get("salesperson")
        self.__consumer = kwargs.get("consumer")
        self.__lead = kwargs.get("lead")
        self.__dealer_group_id = self.activity["crm_dealer_id"].split("__")[0]

    def get_secrets(self):
        secret = secret_client.get_secret_value(
            SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/crm-integration-partner"
        )
        secret = loads(secret["SecretString"])[str(SECRET_KEY)]
        secret_data = loads(secret)

        return secret_data["API_URL"], secret_data["API_USERNAME"], secret_data["API_PASSWORD"]

    def __insert_note(self):
        payload = {
            "addedBy_UserID": self.__salesperson["crm_salesperson_id"],
            "leadID": self.__lead["crm_lead_id"],
            "note": self.__activity["notes"],
        }
        logging.info(f"Payload to CRM: {payload}")

        res = requests.post(
            url=f"{self.__url}/dealergroup/{self.__dealer_group_id}/customer/{self.__consumer['crm_consumer_id']}/notes",
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
                "userID": self.__consumer["crm_consumer_id"]
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

        logging.info(payload)

        res = requests.post(
            url=f"{self.url}/dealergroup/{self.__dealer_group_id}/tasks",
            json=payload,
            auth=HTTPBasicAuth(self.__username, self.__password)
        )

        res.raise_for_status()
        return res.json()["task"]["taskID"]

    def create_activity(self):
        if self.__activity["activity_type"] == "note":
            return self.__insert_note()
        return self.__insert_task()


def lambda_handler(event: dict, context):
    """Create activity in DealerPeak."""
    logger.info(f"Event: {event}")
    crm_api = CrmApiWrapper()
    batch_failures = []

    for event in event.get('Records', []):
        try:
            activity = loads(event['body'])
            lead = crm_api.get_lead(activity["lead_id"])
            salesperson = crm_api.get_salesperson(activity["lead_id"])
            consumer = crm_api.get_consumer(lead["consumer_id"])

            dealer_peak_api = DealerpeakApiWrapper(
                activity=activity,
                salesperson=salesperson,
                consumer=consumer,
                lead=lead
            )

            dealerpeak_task_id = dealer_peak_api.create_activity()
            crm_api.update_activity(activity["activity_id"], dealerpeak_task_id)
        except Exception:
            logger.exception(f"Failed to post activity {activity['activity_id']} to Dealerpeak")
            batch_failures.append(event['messageId'])

    return {
        "batchItemFailures": batch_failures
    }
