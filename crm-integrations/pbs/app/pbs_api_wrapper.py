import boto3
import requests
import json
from requests.auth import HTTPBasicAuth
from botocore.exceptions import ClientError
import os

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
        except ClientError as e:
            print(f"Error retrieving secret: {e}")
            raise e
        else:
            # The secret is stored under the 'PBS' key in the retrieved dictionary
            return json.loads(json.loads(get_secret_value_response['SecretString'])['PBS'])

    def call_employee_get(self, employee_id):
        """Call the EmployeeGet endpoint with the given employee_id."""
        endpoint = f"{self.base_url}/json/reply/EmployeeGet"
        params = {
            "SerialNumber": self.credentials["SERIAL_NUMBER"]
        }
        payload = {
            "EmployeeId": employee_id,
            "IncludeInactive": False
        }

        try:
            response = requests.post(endpoint, params=params, json=payload, auth=self.auth, timeout=3)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as err:
            print(f"HTTP error occurred: {err}")
            raise
        except Exception as err:
            print(f"Other error occurred: {err}")
            raise
            
