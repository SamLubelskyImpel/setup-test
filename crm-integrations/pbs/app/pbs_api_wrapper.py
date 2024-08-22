import boto3
import requests
import json
from requests.auth import HTTPBasicAuth
from botocore.exceptions import ClientError
import os
import logging

# Configure logging
logger = logging.getLogger(__name__)

# Configuration
is_prod = os.environ.get("Environment", "test") == "prod"
SECRET_NAME = "prod/crm-integrations-partner" if is_prod else "test/crm-integrations-partner"
REGION_NAME = "us-east-1"

class APIWrapper:
    def __init__(self):
        self.secret_name = SECRET_NAME
        self.region_name = REGION_NAME
        self.credentials = self.get_secret()
        self.base_url = self.credentials["API_URL"]
        self.auth = HTTPBasicAuth(self.credentials["API_USERNAME"], self.credentials["API_PASSWORD"])

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
            # The secret is stored under the 'PBS' key in the retrieved dictionary
            return json.loads(json.loads(get_secret_value_response['SecretString'])['PBS'])

    def call_employee_get(self, employee_id, crm_dealer_id):
        """Call the EmployeeGet endpoint with the given employee_id."""
        endpoint = f"{self.base_url}/json/reply/EmployeeGet"
        params = {
            "SerialNumber": crm_dealer_id,
            "EmployeeId": employee_id,
            "IncludeInactive": False
        }

        try:
            response = requests.post(endpoint, params=params, auth=self.auth, timeout=3)
            response.raise_for_status()
            logger.info(f"Successfully fetched employee data for EmployeeId: {employee_id}")
            return response.json()
        except requests.exceptions.HTTPError as err:
            logger.error(f"HTTP error occurred: {err}")
            raise
        except Exception as err:
            logger.error(f"Other error occurred: {err}")
            raise

    def call_deal_get(self, start_time, crm_dealer_id):
        endpoint = f"{self.base_url}/json/reply/DealGet"
        data = {
            "SerialNumber": crm_dealer_id,
            "ContractSince": start_time
        }

        try:
            response = requests.post(endpoint, data=data, auth=self.auth, timeout=3)
            response.raise_for_status()
            logger.info(f"Successfully fetched new Deal data created since {start_time}")
            return response.json()
        except requests.exceptions.HTTPError as err:
            logger.error(f"HTTP error occurred: {err}")
            raise
        except Exception as err:
            logger.error(f"Other error occurred: {err}")
            raise

    def call_contact_get(self, contactIdList, crm_dealer_id):
        endpoint = f"{self.base_url}/json/reply/ContactGet"

        data = {
            "SerialNumber": crm_dealer_id,
            "ContactIdList": contactIdList
        }

        try:
            response = requests.post(endpoint, data=data, auth=self.auth, timeout=3)
            response.raise_for_status()
            logger.info(f"Successfully fetched Contact with Ids {contactIdList}")
            return response.json()
        except requests.exceptions.HTTPError as err:
            logger.error(f"HTTP error occurred: {err}")
            raise
        except Exception as err:
            logger.error(f"Other error occurred: {err}")
            raise

    def call_vehicle_get(self, vehicleIdList, crm_dealer_id):
        endpoint = f"{self.base_url}/json/reply/VehicleGet"
        
        data = {
            "SerialNumber": crm_dealer_id,
            "VehicleIdList": vehicleIdList
        }

        try:
            response = requests.post(endpoint, data=data, auth=self.auth, timeout=3)
            response.raise_for_status()
            logger.info(f"Successfully fetched Vehicle with Ids {vehicleIdList}")
            return response.json()
        except requests.exceptions.HTTPError as err:
            logger.error(f"HTTP error occurred: {err}")
            raise
        except Exception as err:
            logger.error(f"Other error occurred: {err}")
            raise

