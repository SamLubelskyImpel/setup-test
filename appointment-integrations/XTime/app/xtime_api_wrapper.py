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
        # secret = secret_client.get_secret_value(
        #     SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/appt-integration-partners"
        # )
        # secret = loads(secret["SecretString"])["XTime"]
        # secret_data = loads(secret)
        test_dict = {
            "authorization_token_config": {
                "url":  "https://auth.coxautoinc.com/token",
                "username": "6d95cc71-8de0-4574-805a-c5f3594fdf4d",
                "password": "vttqvYiMk783ZB3mEE8S1Ffuxd229sCF3sfGhr4xWYvI9paCohIxr4ErD"
            },
            "x_api_key": "1Dn57QXZxo1Jt8f97lrJ337dLhwTr1Uf6tHytBQb",
            "api_url": "https://sandbox.api.coxautoinc.com/xtime", 
            "dealer_code": "xts9010"
        }
        return (
            test_dict["authorization_token_config"],
            test_dict["x_api_key"],
            test_dict["api_url"],
            test_dict["dealer_code"]
        )
        # return (
        #     secret_data["API_URL"],
        #     secret_data["X_API_KEY"],
        #     secret_data["DEVELOPER_KEY"]
        # )


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
            logger.info(f"Response from CRM: {response}")
            
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"API call failed: {e}")
            raise


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
        url = "{}/appointments".format(self.__api_url)

        params = {
            "dealerCode": self.__dealer_code,
            "vin": appointments.vin
        }

        response_json = self.__call_api(url, method="GET", params=params)

        logger.info(f"Response from CRM: {response_json}")
        return response_json["appointments"]


    def retrieve_appt_time_slots(self, appointment_slots: AppointmentSlots):
        """Retrieve appointment time slots on XTime."""
        url = f"{self.__api_url}/service/appointments-availabilities"

        params = {
            "dealerCode": appointment_slots.integration_dealer_id,
        }

        parameters = ["year", "make", "model", "op_code", "start_time", "end_time"]

        for param in parameters:
            value = getattr(appointment_slots, param, None)
            if value:
                params[param] = value

        response_json = self.__call_api(url, method="GET", params=params)

        logger.info(f"Response from XTime: {response_json}")
        return response_json