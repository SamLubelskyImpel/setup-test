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
        self.__authorization_data, self.__x_api_key, self.__api_url, self.__dealer_code = self.__get_secrets()

        self.__authorization_token = self.__get_token()


    def __get_secrets(self):
        secret = secret_client.get_secret_value(
            SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/appt-integration-partners"
        )
        secret = loads(secret["SecretString"])["XTime"]
        secret_data = loads(secret)
        logger.info(secret_data)
        return (
            secret_data["authorization_token_config"],
            secret_data["x_api_key"],
            secret_data["api_url"],
            secret_data["dealer_code"]
        )

    def __get_token(self):
        url, username, password = self.__authorization_data.values()
        session = requests.Session()
        session.auth = (username, password)

        payload='grant_type=client_credentials&scope=cai.scheduling.appointments.read%20cai.scheduling.appointments.write'

        response = session.post(
            url=url, 
            data=payload,
        )

        response.raise_for_status()
        response_json = response.json()
        logger.info(f"Status code from XTime Auth: {response.status_code}")

        return f"{response_json['token_type']} {response_json['access_token']}"


    def __call_api(self, url, payload=None, method="POST", params=None):
        headers = {
            "Content-Type": "application/json",
            "x-api-key": self.__x_api_key,
            "Authorization": self.__authorization_token,
        }
        try:
            response = requests.request(method=method, url=url, json=payload, headers=headers, params=params)
            logger.info(f"Response from XTime: {response.json()}")
            
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"API call failed: {e}")
            raise


    def __convert_utc_to_timezone(self, input_ts: str, dealer_timezone: str) -> str:
        """Convert UTC timestamp to dealer's local time."""
        utc_datetime = datetime.strptime(input_ts, '%Y-%m-%dT%H:%M:%S')
        utc_datetime = pytz.utc.localize(utc_datetime)

        if not dealer_timezone:
            logger.warning("Dealer timezone not found")
            return utc_datetime.strftime('%Y-%m-%d')

        # Get the dealer timezone object, convert UTC datetime to dealer timezone
        dealer_tz = pytz.timezone(dealer_timezone)
        dealer_datetime = utc_datetime.astimezone(dealer_tz)

        return dealer_datetime.strftime('%Y-%m-%d')
    

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
            "dmsNotes": "",
            "advisorId": create_appt_data.integration_dealer_id,
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

        response_json = self.__call_api(url, payload=payload, params=params)

        logger.info(f"Response from XTime: {response_json}")
        return response_json


    def retrieve_appointments(self, appointments: GetAppointments):
        """Retrieve appointments on XTime."""
        url = "{}/service/appointments".format(self.__api_url)

        params = {
            "dealerCode": self.__dealer_code,
            "vin": appointments.vin
        }

        return self.__call_api(url, method="GET", params=params)


    def retrieve_appt_time_slots(self, appointment_slots: AppointmentSlots):
        """Retrieve appointment time slots on XTime."""
        url = f"{self.__api_url}/service/appointments-availabilities"

        params = {
            "dealerCode": appointment_slots.integration_dealer_id,
            "start": self.__convert_utc_to_timezone(appointment_slots.start_time, appointment_slots.dealer_timezone),
            "end": self.__convert_utc_to_timezone(appointment_slots.end_time, appointment_slots.dealer_timezone),
            "opcode": appointment_slots.op_code,
            "vin": appointment_slots.vin
        }

        response_json = self.__call_api(url, method="GET", params=params)

        logger.info(f"Response from XTime: {response_json}")
        return response_json