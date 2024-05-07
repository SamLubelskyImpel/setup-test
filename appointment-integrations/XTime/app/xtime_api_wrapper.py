"""
These classes are designed to manage calls to the XTime for appointments.
This wrapper classes defined this file should NOT be modified or used by any other resources aside from the appointment lambda.
A decision was made to isolate source code for each lambda in order to limit the impact of errors caused by changes to other resources.
"""

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
            logger.info(f"Headers for XTime: {headers}")
            response = requests.request(method=method, url=url, json=payload, headers=headers, params=params)
            logger.info(f"Response from XTime: {response.json()}")
            
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"API call failed: {e}")
            raise
 

    def _add_optional_params(self, params, data_instance):
            """Add optional parameters to the request."""
            for key in ["vin", "year", "make", "model"]:
                value = getattr(data_instance, key, None)
                if value is not None:
                    params[key] = value


    def create_appointments(self, create_appt_data: CreateAppointment):
        """Create appointments on XTime."""
        url = "{}/service/appointments-bookings".format(self.__api_url)
        
        params = {
            "dealerCode": create_appt_data.integration_dealer_id
        }

        self._add_optional_params(params=params, data_instance=create_appt_data)

        payload = {
            "appointmentDateTimeLocal": datetime.strptime(create_appt_data.timeslot, '%Y-%m-%dT%H:%M:%S').strftime('%Y-%m-%dT%H:%M%z'),
            "firstName": create_appt_data.first_name,
            "lastName": create_appt_data.last_name,
            "emailAddress": create_appt_data.email_address,
            "phoneNumber": create_appt_data.phone_number.replace("-", ""),
            "services": [
                {
                    "opcode": create_appt_data.op_code
                }
            ]
        }
        logger.info(f"This is payload for creating appointment: \n {payload}")

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
            "start": datetime.strptime(appointment_slots.start_time, '%Y-%m-%dT%H:%M:%S').strftime('%Y-%m-%d'),
            "end": datetime.strptime(appointment_slots.end_time, '%Y-%m-%dT%H:%M:%S').strftime('%Y-%m-%d'),
            "opcode": appointment_slots.op_code
        }
        self._add_optional_params(params=params, data_instance=appointment_slots)

        response_json = self.__call_api(url, method="GET", params=params)

        logger.info(f"Response from XTime: {response_json}")
        return response_json