"""DMS API Wrapper."""
from json import JSONDecodeError, loads
from os import environ
import logging
import requests

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

ENVIRONMENT = environ.get("ENVIRONMENT", "stage")

class ApiWrapper:
    def __init__(self):
        self.base_url = "https://dms-service.impel.io" if ENVIRONMENT == "prod" else "https://dms-service.testenv.impel.io"
        self.client_id = "impel_service"
        self.api_key = self.get_api_key()
    
    def get_api_key(self):
        """Get API keys."""
        secret_id = f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/DmsDataService"
        try:
            secret = boto3.client("secretsmanager").get_secret_value(
                SecretId=secret_id
            )
            secret = loads(secret["SecretString"])[self.client_id]
            x_api_key = loads(secret)["api_key"]
            return x_api_key
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                raise RuntimeError(f"Secret '{secret_id}' not found")
            raise

    def get_integration_dealers(self, integration, page=1):
        """Get all dealers for an integration."""
        headers = {
            "accept": "application/json",
            "client_id": self.client_id,
            "x_api_key": self.api_key
        }
        resp = requests.get(
            f"{self.base_url}/dealer/v1?page={page}&impel_integration_partner_id={integration}",
            headers=headers
        )
        resp.raise_for_status()

        try:
            resp_json = resp.json()
            if resp_json["has_next_page"]:
                return resp_json["results"] + self.get_integration_dealers(
                    integration, page + 1
                )
            else:
                return resp_json["results"]
        except JSONDecodeError:
            logger.exception(f"Unable to parse integration_dealers response {resp}")
            raise
