import logging
import json
import boto3
from datetime import datetime
from dateutil.tz import tzutc

from .envs import CRM_INTEGRATION_SECRETS_ID, LOG_LEVEL

logger = logging.getLogger()
logger.setLevel(LOG_LEVEL)
client = boto3.client("secretsmanager")

def get_token():
    response = client.get_secret_value(SecretId=CRM_INTEGRATION_SECRETS_ID)
    return response

def update_token(new_secret_string):
    response = client.update_secret(
        SecretId=CRM_INTEGRATION_SECRETS_ID,
        SecretString=new_secret_string
    )
    logger.info(f"Token updated successfully. Version ID: {response['VersionId']}")
