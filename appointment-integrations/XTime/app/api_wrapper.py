"""
These classes are designed to manage calls to the XTime for appointments.
This wrapper classes defined this file should NOT be modified or used by any other resources aside from the appointment lambda.
A decision was made to isolate source code for each lambda in order to limit the impact of errors caused by changes to other resources.
"""

import pytz
import requests
from os import environ
from json import loads
from boto3 import client
from datetime import datetime
from models import GetAppointments, CreateAppointment, AppointmentSlots

import logging

ENVIRONMENT = environ.get("ENVIRONMENT")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
secret_client = client("secretsmanager")

class XTimeApiWrapper:
    """XTime API Wrapper."""

    def __init__(self):
        self.__x_api_key, self.__api_url, self.__developer_key = self.__get_secrets()


    def __get_secrets(self):
        secret = secret_client.get_secret_value(
            SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/appt-integration-partners"
        )
        secret = loads(secret["SecretString"])["XTime"]
        secret_data = loads(secret)

        return (
            secret_data["API_URL"],
            secret_data["X_API_KEY"],
            secret_data["DEVELOPER_KEY"]
        )


    def __call_api(self, url, payload=None, method="POST", params=None):
        headers = {
            "Content-Type": "application/json",
            "x-api-key": self.__x_api_key,
            "developerKey": self.__developer_key,
        }
        response = requests.request(
            method=method,
            url=url,
            json=payload,
            headers=headers,
            params=params
        )
        logger.info(f"Response from CRM: {response.status_code}")
        return response


    def __convert_utc_to_timezone(self, input_ts: str, dealer_timezone: str) -> str:
        """Convert UTC timestamp to dealer's local time."""
        utc_datetime = datetime.strptime(input_ts, '%Y-%m-%dT%H:%M:%SZ')
        utc_datetime = pytz.utc.localize(utc_datetime)

        if not dealer_timezone:
            logger.warning("Dealer timezone not found")
            return utc_datetime.strftime('%Y-%m-%dT%H:%M:%S')

        # Get the dealer timezone object, convert UTC datetime to dealer timezone
        dealer_tz = pytz.timezone(dealer_timezone)
        dealer_datetime = utc_datetime.astimezone(dealer_tz)

        return dealer_datetime.strftime('%Y-%m-%dT%H:%M:%S')
    

    def create_appointments(self, create_appt_data: CreateAppointment):
        """Create appointments on XTime."""
        url = "{}/appointments-bookings".format(self.__api_url)
        
        params = {
            "dealerCode": create_appt_data.integration_dealer_id,
            "vin": create_appt_data.vin,
            "year": create_appt_data.year,
            "make": create_appt_data.make,
            "model": create_appt_data.model
        }

        payload = {
            "appointmentId":create_appt_data.request_id,
            "appointmentDateTimeLocal": create_appt_data.timeslot,
            "firstName": create_appt_data.first_name,
            "lastName": create_appt_data.last_name,
            "emailAddress": create_appt_data.email_address,
            "phoneNumber": create_appt_data.phone_number,
            "comment": create_appt_data.comment,
            "dmsNotes": "", #TODO find out if we can send this data.
            "advisorId": create_appt_data.integration_dealer_id, #Can be nothing
            "transportType": "",
            "label": {},
            "reportingLabel": {},
            "services": [
                {
                    "serviceName": "",
                    "opcode": create_appt_data.op_code,
                    "price": "",
                    "comment": ""
                }
            ],
            "servicePackage": {},
            "tellusMore": {},
            "valet": {}
        }

        response = self.__call_api(url, payload=payload, params=params)

        response.raise_for_status()
        logger.info(f"Response from XTime: {response.json()}")
        return response.json()


    def retrieve_appointments(self, appointments: GetAppointments):
        """Retrieve appointments on XTime."""
        url = "{}/appointments".format(self.__api_url)

        params = {
            "dealerCode": appointments.integration_dealer_id,
            "vin": appointments.vin
        }

        response = self.__call_api(url, method="GET", params=params)

        response.raise_for_status()
        response_json = response.json()

        logger.info(f"Response from CRM: {response_json}")
        return response_json["appointments"]


    def retrieve_appt_time_slots(self, appointment_slots: AppointmentSlots):
        """Retrieve appointment time slots on XTime."""
        url = "{}/appointments-availabilities".format(self.__api_url)

        start_time = self.__convert_utc_to_timezone(appointment_slots.start_time, appointment_slots.dealer_timezone)
        end_time = self.__convert_utc_to_timezone(appointment_slots.end_time, appointment_slots.dealer_timezone)

        params = {
            "dealerCode": appointment_slots.integration_dealer_id,
            "vin": appointment_slots.vin,
            "year": appointment_slots.year,
            "make": appointment_slots.make,
            "model": appointment_slots.model,
            "opcode": appointment_slots.op_code,
            "start": start_time,
            "end": end_time
        }

        response = self.__call_api(url, method="GET", params=params)

        response.raise_for_status()
        logger.info(f"Response from XTime: {response.json()}")
        return response.json()
