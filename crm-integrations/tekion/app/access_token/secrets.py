import json

import boto3
from aws_lambda_powertools import Logger

from .envs import CRM_INTEGRATION_SECRETS_ID, SECRET_KEY
from .schemas import TekionCredentials

logger = Logger()


def get_credentials_from_secrets() -> TekionCredentials:
    logger.debug(
        "Fetching credentials from Secrets Manager: %s",
        extra={"secrets_id": CRM_INTEGRATION_SECRETS_ID},
    )
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=CRM_INTEGRATION_SECRETS_ID)
    content = json.loads(response["SecretString"])
    partner_content = content[SECRET_KEY]

    if isinstance(partner_content, str):
        partner_content = json.loads(partner_content)

    return TekionCredentials(**partner_content)
