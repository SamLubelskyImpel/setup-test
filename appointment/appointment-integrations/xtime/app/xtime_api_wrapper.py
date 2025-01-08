"""Wrapper class designed to manage calls to the XTime for appointments."""

import requests
from os import environ
from json import loads
from boto3 import client
from datetime import datetime
from models import GetAppointments, CreateAppointment, AppointmentSlots
from dateutil import parser
import pytz
import logging
import json
from xtime_token_management.secrets import get_token, update_token
from xtime_token_management.token_status import is_token_expired
import requests
from datetime import datetime

ENVIRONMENT = environ.get("ENVIRONMENT")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
secret_client = client("secretsmanager")


class XTimeApiWrapper:
    """XTime API Wrapper."""

    def __init__(self):
        self.__authorization_data, self.__x_api_key, self.__api_url = self.__get_secrets()

        self.__authorization_token = self.__get_token()

    def __get_secrets(self):
        secret = secret_client.get_secret_value(
            SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/appt-integration-partners"
        )
        secret = loads(secret["SecretString"])["XTime"]
        secret_data = loads(secret)
        return (
            secret_data["authorization_token_config"],
            secret_data["x_api_key"],
            secret_data["api_url"]
        )

    def __get_token(self):
        """Generate or retrieve the authorization token for XTime."""
        token_from_secrets = get_token()
        secret_string = json.loads(token_from_secrets['SecretString'])

        if not is_token_expired(token_from_secrets):
            logger.info("Reusing existing token")
            return f"{secret_string['token_type']} {secret_string['access_token']}"

        logger.info("Token expired. Generating new token")
        url, username, password = self.__authorization_data.values()
        session = requests.Session()
        session.auth = (username, password)

        payload = 'grant_type=client_credentials&scope=cai.scheduling.appointments.read%20cai.scheduling.appointments.write'

        response = session.post(url=url, data=payload)
        response.raise_for_status()
        new_token_data = response.json()

        new_secret_string = json.dumps({
            'access_token': new_token_data['access_token'],
            'token_type': new_token_data['token_type'],
            'expires_in': new_token_data['expires_in']
        })
        update_token(new_secret_string)

        logger.info(f"New token generated and stored. Status code: {response.status_code}")
        return f"{new_token_data['token_type']} {new_token_data['access_token']}"

    def __call_api(self, url, payload=None, method="POST", params=None):
        """Call the XTime API."""
        headers = {
            "Content-Type": "application/json",
            "x-api-key": self.__x_api_key,
            "Authorization": self.__authorization_token,
        }
        try:
            response = requests.request(method=method, url=url, json=payload, headers=headers, params=params, timeout=15)
            logger.info(f"Status code from XTime: {response.status_code}")
            logger.info(f"Response text from XTime: {response.text}")

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

    def __localize_time(self, time_string: str, dealer_timezone: str) -> str:
        """Localize the timestamp to include the dealer's timezone."""
        parsed_ts = parser.parse(time_string)
        dealer_tz = pytz.timezone(dealer_timezone)
        localized_ts = dealer_tz.localize(parsed_ts)
        return localized_ts.strftime('%Y-%m-%dT%H:%M%z')

    def create_appointments(self, create_appt_data: CreateAppointment):
        """Create appointments on XTime."""
        url = "{}/service/appointments-bookings".format(self.__api_url)

        params = {
            "dealerCode": create_appt_data.integration_dealer_id
        }
        self._add_optional_params(params=params, data_instance=create_appt_data)
        logger.info(f"Params for XTime: {params}")

        email_address = create_appt_data.email_address
        phone_number = create_appt_data.phone_number

        if not email_address:
            email_address = "not-available@noemail.com"  # Default email address
        elif not phone_number:
            phone_number = "5550000000"  # Default phone number
        elif not email_address and not phone_number:
            raise ValueError("Email address or phone number is required.")

        if create_appt_data.source_product == "SERVICE_AI":
            label_content = "Impel Service AI"
        elif create_appt_data.source_product == "CHAT_AI":
            label_content = "Impel Chat AI"
        elif create_appt_data.source_product == "VOICE_AI":
            label_content = "Impel Voice AI"
        else:
            logger.error(f"Unknown source product: {create_appt_data.source_product}")
            label_content = "Impel Service Scheduling"

        payload = {
            "appointmentDateTimeLocal": self.__localize_time(create_appt_data.timeslot, create_appt_data.dealer_timezone),
            "firstName": create_appt_data.first_name,
            "lastName": create_appt_data.last_name,
            "emailAddress": email_address,
            "phoneNumber": phone_number.replace("-", ""),
            "comment": create_appt_data.comment,
            "services": [
                {
                    "opcode": create_appt_data.op_code
                }
            ],
            "label": {
                "value": "Impel Service Scheduling",
                "context": label_content
            },
            "reportingLabel": {
                "value": "Impel Service Scheduling",
                "context": label_content
            }
        }
        payload = {key: value for key, value in payload.items() if value}
        logger.info(f"Payload for XTime: \n {payload}")

        response_json = self.__call_api(url, payload=payload, params=params)

        logger.info(f"XTime Create Appt Response: {response_json}")
        return response_json

    def retrieve_appointments(self, appointments: GetAppointments):
        """Retrieve appointments on XTime."""
        url = "{}/service/appointments".format(self.__api_url)

        params = {
            "dealerCode": appointments.integration_dealer_id,
            "vin": appointments.vin
        }

        response_json = self.__call_api(url, method="GET", params=params)
        logger.info(f"XTime Retrieve Appt Response: {response_json}")

        return response_json

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
        logger.info(f"Params for XTime: {params}")

        response_json = self.__call_api(url, method="GET", params=params)

        logger.info(f"XTime Timeslots Response: {response_json}")
        return response_json

    def get_dealer_codes(self, integration_dealer_id: str):
        """Retrieve standard dealer op codes from XTime."""
        url = f"{self.__api_url}/service/services"
        params = {
            "dealerCode": integration_dealer_id,
            "year": 2024,
            "make": "OTHER",
            "model": "OTHER"
        }

        response_json = self.__call_api(url, method="GET", params=params)

        logger.info(f"XTime Dealer Op Codes Response: {response_json}")
        return response_json
