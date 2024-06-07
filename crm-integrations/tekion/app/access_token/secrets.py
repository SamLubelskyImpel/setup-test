import json
import logging

import boto3

from .envs import CRM_INTEGRATION_SECRETS_ID, LOGLEVEL, PARTNER_KEY
from .schemas import TekionCredentials

logger = logging.getLogger()
logger.setLevel(LOGLEVEL.upper())


def get_credentials_from_secrets() -> TekionCredentials:
    logger.debug(
        "Fetching credentials from Secrets Manager: %s",
        CRM_INTEGRATION_SECRETS_ID,
    )
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=CRM_INTEGRATION_SECRETS_ID)
    content = json.loads(response["SecretString"])
    partner_content = content[PARTNER_KEY]

    if isinstance(partner_content, str):
        partner_content = json.loads(partner_content)

    return TekionCredentials(**partner_content)
