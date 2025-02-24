import boto3
import requests
import logging
from os import environ
from json import loads, dumps
from botocore.exceptions import ClientError
from shared_class import BaseClass
import uuid

# Logging
logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

# AWS Secrets Manager Config
ENVIRONMENT = environ.get("ENVIRONMENT")
is_prod = ENVIRONMENT == "prod"
SECRET_NAME = "prod/crm-integrations-partner" if is_prod else "test/crm-integrations-partner"
REGION_NAME = "us-east-1"
SECRET_KEY = environ.get("SECRET_KEY")
CRM_API_DOMAIN = environ.get("CRM_API_DOMAIN")


class APIError(Exception):
    """Generic API Error Exception."""
    pass


class CRMAPIWrapper(BaseClass):
    """Handles calls to the CRM API by extending BaseClass."""

    def __init__(self):
        """Initialize the wrapper using BaseClass."""
        super().__init__()  # Inherit BaseClass functionality

    def get_vehile_lead_data(self, lead_id: int):
        """Fetch lead details from CRM API."""
        return self.call_crm_api(f"https://{CRM_API_DOMAIN}/leads/{lead_id}")

    def get_consumer(self, consumer_id: int):
        """Fetch consumer details from CRM API."""
        return self.call_crm_api(f"https://{CRM_API_DOMAIN}/consumers/{consumer_id}")

    def get_vehicles(self, lead_id: int):
        """Fetch associated vehicles from CRM API."""
        return self.call_crm_api(f"leads/{lead_id}/vehicles")

    def update_lead(self, lead_id: int, update_data: dict):
        """Update lead in CRM API."""
        url = f"{self.crm_api_url}/leads/{lead_id}"
        headers = {
            "x_api_key": self.api_key,
            "partner_id": self.partner_id,
        }

        try:
            logger.info(f"Updating lead {lead_id} in CRM API: {dumps(update_data, indent=2)}")
            response = requests.put(url, headers=headers, json=update_data)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"Error updating lead in CRM API: {e}, Response: {response.text}")
            raise APIError(f"Error updating lead in CRM API: {e}")


class ShiftDigitalAPIWrapper:
    """Handles API interactions with Shift Digital."""

    def __init__(self, oem_partner: dict = None):
        """Initialize API credentials from AWS Secrets Manager."""
        oem_partner = oem_partner or {}

        self.oem_name = oem_partner.get("name", "").upper()
        self.oem_dealer_code = oem_partner.get("dealer_code", "")

        self.secret_client = boto3.client("secretsmanager", region_name=REGION_NAME)
        self.credentials = self.get_secret()
        self.api_url = self.credentials["API_URL"]
        self.headers = {
            "Content-Type": "application/json"
        }
        self.crm_api = CRMAPIWrapper()

        self.provider_source_ids = {
            "STELLANTIS": {
                "sales": "5320",
                "contact": "5322"
            }
        }

    def get_secret(self):
        """Retrieve Shift Digital API credentials from AWS Secrets Manager."""
        logger.info(f"Fetching secrets from: {SECRET_NAME}")

        try:
            response = self.secret_client.get_secret_value(SecretId=SECRET_NAME)
            secret_data = loads(response["SecretString"])  # Convert string to dictionary

            # Extract only the SHIFT_DIGITAL key
            shift_digital_secret = loads(secret_data["SHIFT_DIGITAL"])  

            logger.info("Successfully retrieved Shift Digital API secrets.")
            return shift_digital_secret

        except KeyError as e:
            logger.error(f"SHIFT_DIGITAL key not found in secrets: {e}")
            raise e
        except ClientError as e:
            logger.error(f"Error retrieving Shift Digital API secret: {e}")
            raise e


    def format_lead_data(self, lead_id: int, dealer_code: str) -> dict:
        """Retrieve data from CRM API and format it for Shift Digital API."""
        logger.info(f"Formatting lead data for Shift Digital submission: lead_id={lead_id}, dealer_code={dealer_code}")

        # Fetch data from CRM API
        try:
            lead = self.crm_api.get_vehile_lead_data(lead_id) or {}
            vehicle_of_interest = lead["vehicles_of_interest"][0] if lead.get("vehicles_of_interest", []) else {}

            is_vehicle_of_interest = any(vehicle_of_interest.get(field) not in [None, ""] for field in ['vin', 'year', 'make', 'model'])

            customer_data = self.crm_api.get_consumer(lead.get("consumer_id", {}))

        except APIError as e:
            logger.error(f"Failed to fetch data from CRM API: {e}")
            raise APIError(f"Could not retrieve data from CRM API for lead_id: {lead_id}")

        # Determine preferred contact method
        preferred_contact = "email" if customer_data.get("email_optin_flag") else "sms" if customer_data.get("sms_optin_flag") else "phone"

        # Determine provider source ID based on lead type
        sourceId = self.provider_source_ids.get(self.oem_name, {}).get(
            "sales" if is_vehicle_of_interest else "contact", "UNKNOWN"
        )

        # Generate UUID for the Lead ID
        lead_uuid = str(uuid.uuid4())

        # Construct formatted data
        formatted_data = {
            "lead": {
                "id": lead_uuid,
                "sourceId": sourceId,
                "dealerCode": dealer_code,
                "timestamp": lead.get("lead_ts", ""),
                "url": lead.get("lead_url", ""), ####TODO: ASK ABOUT THIS
            },
            "customer": {
                "firstName": customer_data.get("first_name", ""),
                "middleName": customer_data.get("middle_name", ""),
                "lastName": customer_data.get("last_name", ""),
                "address1": customer_data.get("address", ""),
                "city": customer_data.get("city", ""),
                "state": customer_data.get("state", ""),
                "zipCode": customer_data.get("postal_code", ""),
                "mobilePhone": customer_data.get("phone", ""),
                "emailAddress": customer_data.get("email", ""),
                "preferredContactMethod": preferred_contact,
                "comments": lead.get("lead_comment", ""),
            },
        }

        # ✅ Only include `vehicles` field if it's a vehicle of interest
        if is_vehicle_of_interest:
            formatted_data["vehicles"] = [
                {
                    "status": vehicle_of_interest.get("condition", "").lower(),
                    "type": "VehicleOfInterest",
                    "vin": vehicle_of_interest.get("vin", ""),
                    "year": vehicle_of_interest.get("year", ""),
                    "make": vehicle_of_interest.get("make", ""),
                    "model": vehicle_of_interest.get("model", ""),
                    "trim": vehicle_of_interest.get("trim", ""),
                    "exteriorColor": vehicle_of_interest.get("exterior_color", ""),
                    "interiorColor": vehicle_of_interest.get("interior_color", ""),
                }
            ]

        return formatted_data, is_vehicle_of_interest

    def submit_lead(self, lead_id: int, dealer_code: str) -> str:
        """Formats data and submits a lead to Shift Digital API."""
        logger.info(f"Submitting lead {lead_id} to Shift Digital.")
        payload, is_vehicle_of_interest = self.format_lead_data(lead_id, dealer_code)

        try:
            logger.info(f"Payload before sending: {dumps(payload, indent=2)}")
            response = requests.post(f"{self.api_url}/deals", json=payload, headers=self.headers)
            response.raise_for_status()

            lead_response = response.json()
            logger.info(f"Shift Digital Responded with: {lead_response}")

            lead_response_id = lead_response.get("Id")
            if not lead_response_id:
                logger.error(f"Shift Digital API did not return a lead ID. Full Response: {lead_response}")
                raise APIError("Shift Digital API did not return a lead ID.")

            logger.info(f"Lead successfully submitted to Shift Digital. Lead ID: {lead_response_id}")
            return lead_response_id, is_vehicle_of_interest

        except requests.exceptions.RequestException as e:
            logger.error(f"Error submitting lead to Shift Digital API: {e}. Response: {response.text}")
            raise APIError(f"Error submitting lead to Shift Digital API: {e}")

    def check_lead_status(self, lead_id: str) -> dict:
        """Check lead status in Shift Digital API."""
        url = f"{self.api_url}/deals/{lead_id}"

        try:
            logger.info(f"Checking lead status for ID: {lead_id}")
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"Error fetching lead status from Shift Digital: {e}, Response: {response.text}")
            raise APIError(f"Error fetching lead status from Shift Digital API: {e}")

    def extract_crm_lead_id(self, status_response: dict) -> str:
        """Extract the first available CRM lead ID from the Shift Digital API response."""
        leads = status_response.get("leads", [])

        for lead in leads:
            crms = lead.get("crms", [])
            if crms:
                return crms[0].get("id")

        return None


    def process_callback(self, shift_digital_lead_id: str, lead_id: str):
        """Process the callback by checking the lead status and updating CRM API."""
        try:
            # Check Shift Digital lead status
            status_response = self.check_lead_status(shift_digital_lead_id)
            logger.info(f"Lead status response: {status_response}")

            # Ensure the lead is generated before updating our CRM
            if status_response.get("status") == "Lead Generated":
                extracted_crm_lead_id = self.extract_crm_lead_id(status_response)

                if not extracted_crm_lead_id:
                    logger.warning(f"No CRM lead ID found in response for Shift Digital Lead ID {shift_digital_lead_id}")
                    return

                update_payload = {
                    "crm_lead_id": extracted_crm_lead_id
                }
                # Update our CRM API
                logger.info(f"Updating CRM API with Shift Digital Lead ID: {shift_digital_lead_id}")
                self.crm_api.update_lead(lead_id, update_payload)
                logger.info(f"Successfully updated CRM API for lead {extracted_crm_lead_id}.")
            else:
                logger.warning(f"Lead ID {shift_digital_lead_id} is not yet processed by Shift Digital.")

        except APIError as e:
            logger.error(f"Error processing callback: {e}")
            raise
