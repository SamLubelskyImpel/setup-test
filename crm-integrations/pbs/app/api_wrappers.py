"""
These classes are designed to manage calls to the PBS/CRM API for activities.
This wrapper classes defined this file should NOT be modified or used by any other resources aside from the SendActivity lambda.
A decision was made to isolate source code for each lambda in order to limit the impact of errors caused by changes to other resources.
"""

import boto3
import requests
from json import loads
from requests.auth import HTTPBasicAuth
from botocore.exceptions import ClientError
from os import environ
import logging
logger = logging.getLogger(__name__)


is_prod = environ.get("Environment", "test") == "prod"
SECRET_NAME = "prod/crm-integrations-partner" if is_prod else "test/crm-integrations-partner"
REGION_NAME = "us-east-1"
ENVIRONMENT = environ.get("ENVIRONMENT")
SECRET_KEY = environ.get("SECRET_KEY")
CRM_API_DOMAIN = environ.get("CRM_API_DOMAIN")
CRM_API_SECRET_KEY = environ.get("UPLOAD_SECRET_KEY")
secret_client = boto3.client("secretsmanager")


class CRMApiError(Exception):
    pass


class CRMAPIWrapper:
    """CRM API Wrapper."""

    def __init__(self):
        self.partner_id = CRM_API_SECRET_KEY

    def __run_get(self, endpoint: str):
        api_key = self.__get_api_secrets()
        response = requests.get(
            url=f"https://{CRM_API_DOMAIN}/{endpoint}",
            headers={
                "x_api_key": api_key,
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

    def get_consumer(self, consumer_id: int):
        consumer = self.__run_get(f"consumers/{consumer_id}")
        logger.info(consumer)
        if not consumer:
            return None
        return consumer

    def __get_api_secrets(self):
        """Retrieve the CRM API key from a different secret."""
        secret = secret_client.get_secret_value(
            SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/crm-api"
        )
        secret = loads(secret["SecretString"])[self.partner_id]
        secret_data = loads(secret)

        return secret_data["api_key"]

    def update_activity(self, activity_id, crm_activity_id):
        api_key = self.__get_api_secrets()
        try:
            response = requests.put(
                url=f"https://{CRM_API_DOMAIN}/activities/{activity_id}",
                json={"crm_activity_id": crm_activity_id},
                headers={
                    "x_api_key": api_key,
                    "partner_id": self.partner_id,
                },
            )
            response.raise_for_status()
            logger.info(f"CRM API PUT Activities responded with: {response.status_code}")
            return response.json()
        except Exception as e:
            logger.error(f"Error occurred calling CRM API: {e}")
            raise CRMApiError(f"Error occured calling CRM API: {e}")


class PbsApiWrapper:
    """PBS API Wrapper."""

    def __init__(self, **kwargs):
        self.secret_name = SECRET_NAME
        self.region_name = REGION_NAME
        self.secret_client = boto3.client('secretsmanager', region_name='us-east-1')
        self.__activity = kwargs.get("activity")
        self.credentials = self.get_secret()
        self.__consumer = kwargs.get("consumer")
        self.base_url = self.credentials["API_URL"]
        self.auth = HTTPBasicAuth(self.credentials["API_USERNAME"], self.credentials["API_PASSWORD"])
        self.__salesperson = kwargs.get("salesperson")

    def get_secret(self):
        """Retrieve the API credentials from AWS Secrets Manager."""
        session = boto3.session.Session()
        client = session.client(service_name='secretsmanager', region_name=self.region_name)

        try:
            get_secret_value_response = client.get_secret_value(SecretId=self.secret_name)
            logger.info("Successfully retrieved secrets from Secrets Manager.")
        except ClientError as e:
            logger.error(f"Error retrieving secret: {e}")
            raise e
        else:
            return loads(loads(get_secret_value_response['SecretString'])['PBS'])

    def __call_api(self, url, payload=None, method="POST"):
        headers = {
            "Content-Type": "application/json",
        }
        response = requests.request(
            method=method,
            url=url,
            json=payload,
            headers=headers,
            auth=self.auth
        )
        logger.info(f"Response from CRM: {response.status_code} - {response.text}")
        response.raise_for_status()
        return response

    def call_employee_get(self, crm_dealer_id):
        """Call the EmployeeGet endpoint with the given employee_id."""
        endpoint = f"{self.base_url}/json/reply/EmployeeGet"
        params = {
            "SerialNumber": crm_dealer_id,
            "IncludeInactive": False
        }

        try:
            response = requests.get(endpoint, params=params, auth=self.auth, timeout=3)
            response.raise_for_status()
            logger.info(f"Successfully fetched employee data for DealerId: {crm_dealer_id}")
            return response.json()
        except requests.exceptions.HTTPError as err:
            logger.error(f"HTTP error occurred: {err}")
            raise
        except Exception as err:
            logger.error(f"Other error occurred: {err}")
            raise

    def call_deal_get(self, start_time, end_time, crm_dealer_id):
        endpoint = f"{self.base_url}/json/reply/DealGet"
        params = {
            "SerialNumber": crm_dealer_id,
            "ContractSince": start_time,
            "ContractUntil": end_time
        }
        try:
            response = requests.get(endpoint, params=params, auth=self.auth, timeout=3)
            response.raise_for_status()
            logger.info(f"Successfully fetched new Deal data created from {start_time} to {end_time}")
            return response.json()
        except requests.exceptions.HTTPError as err:
            logger.error(f"HTTP error occurred: {err}")
            raise
        except Exception as err:
            logger.error(f"Other error occurred: {err}")
            raise

    def call_contact_get(self, contact_id, crm_dealer_id):
        endpoint = f"{self.base_url}/json/reply/ContactGet"
        params = {
            "SerialNumber": crm_dealer_id,
            "ContactIdList": contact_id
        }
        try:
            response = requests.get(endpoint, params=params, auth=self.auth, timeout=3)
            response.raise_for_status()
            logger.info(f"Successfully fetched Contact with Id {contact_id}")
            return response.json()
        except requests.exceptions.HTTPError as err:
            logger.error(f"HTTP error occurred while calling Contact GET: {err}")
            raise
        except Exception as err:
            logger.error(f"Other error occurred while calling Contact GET: {err}")
            raise

    def call_vehicle_get(self, vehicle_id, crm_dealer_id):
        endpoint = f"{self.base_url}/json/reply/VehicleGet"
        params = {
            "SerialNumber": crm_dealer_id,
            "VehicleIdList": vehicle_id
        }
        try:
            response = requests.get(endpoint, params=params, auth=self.auth, timeout=3)
            response.raise_for_status()
            logger.info(f"Successfully fetched Vehicle with Id {vehicle_id}")
            return response.json()
        except requests.exceptions.HTTPError as err:
            logger.error(f"HTTP error occurred while calling Vehicle GET: {err}")
            raise
        except Exception as err:
            logger.error(f"Other error occurred while calling Vehicle GET: {err}")
            raise

    def __create_payload(self, event_type: str) -> dict:
        """
        Create a common payload structure for different activities.

        Args:
            event_type (str): The type of event (e.g., 'Appointment', 'Note', 'Phone Call Request', 'First Contact').

        Returns:
            dict: A dictionary representing the common payload structure for the given activity.

        This function extracts common fields from the `__activity` attribute and creates a standardized
        payload structure. It conditionally includes 'DueDate' or 'EventDate' based on the activity type.
        """
        serial_number = self.__activity.get("crm_dealer_id")
        contact_ref = self.__activity.get("crm_consumer_id")
        details = self.__activity.get("notes", f"Impel Sales AI {event_type}.")
        employee_ref = self.__salesperson.get("crm_salesperson_id")

        # Determine the summary based on the event type
        if event_type == "appointment":
            summary = "Appointment"
        elif event_type == "note":
            summary = "Note"
        elif event_type == "outbound_call":
            summary = "Outbound call"
        elif event_type == "phone_call_task":
            summary = "Phone call request from Impel Sales AI"
        else:
            logger.error(f"Unsupported event type: {event_type}")
            raise ValueError(f"Unsupported event type: {event_type}")

        payload = {
            "Id": f"{serial_number}/00000000-0000-0000-0000-000000000000",
            "SerialNumber": serial_number,
            "ContactRef": contact_ref,
            "Status": "Open",
            "UserRefs": [{"EmployeeRef": employee_ref}],
            "Summary": summary,
            "Details": details,
        }

        # Conditionally add DueDate or EventDate based on the activity type
        if event_type in ["appointment", "phone_call_task"]:
            payload["DueDate"] = self.__activity.get("activity_due_ts")
        elif event_type == "outbound_call":
            payload["EventDate"] = self.__activity.get("activity_requested_ts")

        return payload

    def __send_payload(self, endpoint: str, payload: dict):
        """
        Send a payload to the PBS CRM API.

        Args:
            endpoint (str): The specific API endpoint to which the payload should be sent.
            payload (dict): The payload data to be sent to the API.

        Returns:
            str: The 'ReferenceId' from the API response if successful.

        This function sends the prepared payload to a specified PBS CRM API endpoint.
        It handles the response, logging relevant information, and raises an exception for non-200 status codes.
        """
        url = f"{self.base_url}/json/reply/{endpoint}"
        logger.info(f"Payload to CRM: {payload}")

        response = self.__call_api(url=url, payload=payload)

        if response.status_code == 200:
            response_json = response.json()
            reference_id = response_json.get("ReferenceId")
            return reference_id
        else:
            logger.error(f"Failed to send payload. Status Code: {response.status_code}, Response: {response.text}")
            response.raise_for_status()

    def __create_appointment(self):
        """
        Create an appointment on PBS CRM.

        Returns:
            str: The 'ReferenceId' from the API response if successful.

        This function builds the payload for an appointment activity, including specific details like
        'AppointmentType', 'TimeAllowed', and 'Recurrence'. It then sends the payload to the appropriate
        API endpoint using the `__send_payload` function.
        """

        payload = {
            "AppointmentInfo": {
                **self.__create_payload("appointment"),
                "AppointmentType": "Appointment",
                "TimeAllowed": "60",
                "Recurrence": "Once",
                "Status": "Open"
            },
            "IsAsynchronous": False
        }

        return self.__send_payload("WorkplanAppointmentChange", payload)

    def __insert_note(self):
        """Create note on PBS CRM."""
        payload = {
            "EventInfo": {
                **self.__create_payload("note"),
                "Action": "Note",
                "Status": "Active"
            },
            "IsAsynchronous": False
        }
        return self.__send_payload("WorkplanEventChange", payload)

    def __phone_call_task(self):
        """Create phone call task on PBS CRM."""
        payload = {
            "ReminderInfo": {
                **self.__create_payload("phone_call_task"),
                "Status": "Active",
            },
            "IsAsynchronous": False
        }
        return self.__send_payload("WorkplanReminderChange", payload)

    def __create_outbound_call(self):
        """Create outbound call on PBS CRM."""
        # Extract consumer details
        first_name = self.__consumer.get("first_name", "")
        last_name = self.__consumer.get("last_name", "")
        middle_name = self.__consumer.get("middle_name", "")
        full_name = f"{first_name} {middle_name} {last_name}".strip()

        dealer_email = self.__activity.get("dealer_integration_partner_metadata", {}).get("email", "")
        dealer_phone = self.__activity.get("dealer_integration_partner_metadata", {}).get("phone", "")

        address_value = ""

        # Determine the contact method and set the action and address accordingly
        contact_method = self.__activity.get("contact_method", "").lower()

        if contact_method == "email":
            action = "Email"
            address_value = self.__consumer.get("email", "")  # Use the consumer's email address
        elif contact_method in ["phone", "text"]:
            action = "Text"
            address_value = self.__consumer.get("phone", "")  # Use the consumer's phone number
        else:
            logger.error(f"Unsupported contact method: {contact_method}")
            raise ValueError(f"Unsupported contact method: {contact_method}")

        if not address_value:
            logger.error(f"Missing contact information: No email or phone number available for consumer {full_name}.")
            return

        payload = {
            "EventInfo": {
                **self.__create_payload("outbound_call"),
                "Action": action,
                "Recipients": [
                    {
                        "RecipientId": self.__activity.get("crm_consumer_id"),
                        "Name": full_name,
                        "Address": address_value,  # Use the email or phone number based on contact method
                        "Type": "From",
                        "Flags": "Sent"
                    },
                    {
                        "Name": "Impel Sales AI",
                        "Address": dealer_email if contact_method == "email" else dealer_phone,
                        "Type": "To",
                        "Flags": "Sent"
                    }
                ]
            },
            "IsAsynchronous": False,
            "IsHistorical": False
        }

        return self.__send_payload("WorkplanEventChange", payload)

    def create_activity(self):
        """Create activity on CRM."""
        if self.__activity["activity_type"] == "note":
            return self.__insert_note()
        elif self.__activity["activity_type"] == "appointment":
            return self.__create_appointment()
        elif self.__activity["activity_type"] == "outbound_call":
            return self.__create_outbound_call()
        elif self.__activity["activity_type"] == "phone_call_task":
            return self.__phone_call_task()
        else:
            logger.error(
                f"PBS CRM doesn't support activity type: {self.__activity['activity_type']}"
            )
            return None
