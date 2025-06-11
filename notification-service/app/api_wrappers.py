"""
CRM API Wrapper for handling dealer and activities
"""

import boto3
import logging
from os import environ
import requests
from json import loads
from requests.auth import HTTPBasicAuth
from utils import get_secret

ENVIRONMENT = environ.get("ENVIRONMENT")
CRM_API_DOMAIN = environ.get("CRM_API_DOMAIN")
CRM_API_SECRET_KEY = environ.get("CRM_API_SECRET_KEY")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
secret_client = boto3.client("secretsmanager")


class CRMApiError(Exception):
    pass


class CrmApiWrapper:
    """CRM API Wrapper."""
    def __init__(self) -> None:
        self.partner_id = CRM_API_SECRET_KEY
        secret_json = get_secret(f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/crm-api", CRM_API_SECRET_KEY)
        self.api_key = secret_json['api_key']

    def get_activity(self, activity_id: int):
        response = requests.get(
            url=f"https://{CRM_API_DOMAIN}/activities/{activity_id}",
            headers={
                "x_api_key": self.api_key,
                "partner_id": self.partner_id,
            }
        )
        response.raise_for_status()
        logger.info(f"CRM API -get_activity- responded with: {response.status_code}")

        if response.status_code != 200:
            raise Exception(f"Error getting activity {activity_id}: {response.text}")

        activity = response.json()
        if not activity:
            raise Exception(f"Activity not found for ID: {activity_id}")

        return activity

    def get_consumer(self, consumer_id: int):
        response = requests.get(
            url=f"https://{CRM_API_DOMAIN}/consumers/{consumer_id}",
            headers={
                "x_api_key": self.api_key,
                "partner_id": self.partner_id,
            }
        )
        response.raise_for_status()
        logger.info(f"CRM API -get_consumer- responded with: {response.status_code}")

        if response.status_code != 200:
            raise Exception(f"Error getting consumer {consumer_id}: {response.text}")

        consumer = response.json()
        if not consumer:
            raise Exception(f"Consumer not found for ID: {consumer_id}")

        return consumer
