"""
These classes are designed to manage calls to the TEKION/CRM API for activities.
This wrapper classes defined this file should NOT be modified or used by any other resources aside from the SendActivity lambda.
A decision was made to isolate source code for each lambda in order to limit the impact of errors caused by changes to other resources.
"""
import pytz
import logging
import requests
from typing import Tuple
from boto3 import client
from json import dumps, loads
from datetime import datetime
from .schemas import SendActivityEvent
from .utils import get_token_from_s3, get_credentials_from_secrets
from .envs import ENV, CRM_API_SECRET_KEY, CRM_API_DOMAIN, LOG_LEVEL

logger = logging.getLogger()
logger.setLevel(LOG_LEVEL.upper())
secret_client = client("secretsmanager")


class InvalidLeadException(Exception):
    """Exception raised for invalid lead data."""
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class InvalidNoteException(Exception):
    """Exception raised for invalid notes."""
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class CrmApiWrapper:
    """CRM API Wrapper."""

    def __init__(self) -> None:
        self.partner_id = CRM_API_SECRET_KEY
        self.api_key = self.__get_secrets()

    def __get_secrets(self):
        secret = secret_client.get_secret_value(
            SecretId=f"{'prod' if ENV == 'prod' else 'test'}/crm-api"
        )
        secret = loads(secret["SecretString"])[self.partner_id]
        secret_data = loads(secret)

        return secret_data["api_key"]

    def __call_api(self, url, payload=None, method="POST"):
        headers = {
            "x_api_key": self.api_key,
            "partner_id": self.partner_id,
        }
        response = requests.request(
            method=method,
            url=url,
            json=payload,
            headers=headers,
        )
        response.raise_for_status()
        return response.json(), response.status_code

    def update_activity(self, activity_id, crm_activity_id):
        try:
            response, status_code = self.__call_api(
                url=f"https://{CRM_API_DOMAIN}/activities/{activity_id}",
                payload={"crm_activity_id": crm_activity_id},
                method="PUT"
            )
            logger.info(f"CRM API PUT Activities responded with: {status_code}")
            return response
        except Exception as e:
            logger.error(f"Error occured calling CRM API: {e}")


class TekionApiWrapper:
    """Tekion API Wrapper."""
    def __init__(self, activity: SendActivityEvent):
        self.__credentials, self.__token = self.__get_token_and_credentials()
        self.__crm_dealer_id = activity.crm_dealer_id
        self.__lead_id = activity.crm_lead_id
        self.__activity_type = activity.activity_type
        self.__activity = activity

    def __get_token_and_credentials(self):
        """Retrieve credentials and token from S3."""
        json_from_s3 = get_token_from_s3()
        credentials = get_credentials_from_secrets()
        token = f"{json_from_s3.token_type} {json_from_s3.token}"
        return credentials, token

    def __call_api(self, url, payload=None, method="POST"):
        headers = {
            "accept": "application/json",
            "Content-Type": "application/json",
            "app_id": self.__credentials.app_id,
            "dealer_id": self.__crm_dealer_id,
            "Authorization": self.__token,
        }
        logger.info(headers)
        response = requests.request(
            method=method,
            url=url,
            json=payload,
            headers=headers,
        )
        logger.info(f"Response from CRM: {response.status_code}")
        response.raise_for_status()
        return response.json()

    def convert_utc_to_timezone(self, input_ts: str) -> Tuple[str, str]:
        """Convert UTC timestamp to dealer's local time."""
        utc_datetime = datetime.strptime(input_ts, '%Y-%m-%dT%H:%M:%SZ')
        utc_datetime = pytz.utc.localize(utc_datetime)

        if not self.__activity.dealer_timezone:
            logger.warning("Dealer timezone not found for crm_dealer_id: {}".format(self.__activity.crm_dealer_id))
            new_ts = utc_datetime.strftime('%m-%d-%Y %H:%M %p')
        else:
            # Get the dealer timezone object, convert UTC datetime to dealer timezone
            dealer_tz = pytz.timezone(self.__activity.dealer_timezone)
            dealer_datetime = utc_datetime.astimezone(dealer_tz)
            new_ts = dealer_datetime.strftime('%m-%d-%Y %H:%M %p')

        timestamp_str = new_ts.split(" ")
        return timestamp_str[0], timestamp_str[1]

    def get_lead_information(self) -> dict:
        url = f"{self.__credentials.url}/openapi/v3.1.0/crm-leads?id={self.__lead_id}"
        lead_data = self.__call_api(url, method="GET")['data'][0]
        if not lead_data.get('externalId'):
            logger.warning("Activity type Note can't be created if lead was invalid")
            raise InvalidLeadException(f"This is lead: \n{dumps(lead_data, indent=2)}")
        return lead_data

    def __create_outbound_call(self):
        lead_data = self.get_lead_information()
        _, appt_time = self.convert_utc_to_timezone(self.__activity.activity_requested_ts)
        lead_data['notes'] = [{
            "description": f"Sales AI sent customer message at {appt_time}",
            "name": "Sales AI",
            "title": "First Contact"
        }]
        json_response = self.__call_api(
            url=f"{self.__credentials.url}/openapi/v3.1.0/crm-leads?id={self.__lead_id}",
            payload=lead_data,
            method="PUT"
        )['data']['notes'][0]
        logger.info(f"Outbound call was successfully created !\n{json_response}")
        return json_response['id']

    def __create_phone_call_task(self):
        lead_data = self.get_lead_information()
        date, time = self.convert_utc_to_timezone(self.__activity.activity_requested_ts)
        lead_data['notes'] = [{
            "description": f"Customer wants you to call them\nDate: {date}\nTime: {time}",
            "name": "Sales AI",
            "title": "Phone call request"
        }]
        json_response = self.__call_api(
            url=f"{self.__credentials.url}/openapi/v3.1.0/crm-leads?id={self.__lead_id}",
            payload=lead_data,
            method="PUT"
        )['data']['notes'][0]
        logger.info(f"Phone call was successfully created !\n{json_response}")
        return json_response['id']

    def __create_appointment(self):
        lead_data = self.get_lead_information()
        appt_date, appt_time = self.convert_utc_to_timezone(self.__activity.activity_due_ts)
        lead_data['notes'] = [{
            "description": f"{self.__activity.notes}\nDate: {appt_date}\nTime: {appt_time}",
            "name": "Sales AI",
            "title": "Appointment"
        }]
        json_response = self.__call_api(
            url=f"{self.__credentials.url}/openapi/v3.1.0/crm-leads?id={self.__lead_id}",
            payload=lead_data,
            method="PUT"
        )['data']['notes'][0]
        logger.info(f"Appointment was successfully created !\n{json_response}")
        return json_response['id']

    def __insert_note(self):
        lead_data = self.get_lead_information()
        if not self.__activity.notes:
            logger.warning("Activity type Note can't be created if note is empty")
            raise InvalidNoteException("Note can't be empty or invalid")
        lead_data['notes'] = [{
            "description": self.__activity.notes,
            "name": "Sales AI",
            "title": "Note"
        }]
        json_response = self.__call_api(
            url=f"{self.__credentials.url}/openapi/v3.1.0/crm-leads?id={self.__lead_id}",
            payload=lead_data,
            method="PUT"
        )['data']['notes'][0]
        logger.info(f"Note was successfully created !\n{json_response}")
        return json_response['id']
        
    def create_activity(self):
        """Create activity on CRM."""
        if self.__activity_type == "note":
            return self.__insert_note()
        elif self.__activity_type == "appointment":
            return self.__create_appointment()
        elif self.__activity_type == "outbound_call":
            return self.__create_outbound_call()
        elif self.__activity_type == "phone_call_task":
            return self.__create_phone_call_task()
        else:
            logger.error(
                f"Tekion CRM doesn't support activity type: {self.__activity_type}"
            )
            return None
