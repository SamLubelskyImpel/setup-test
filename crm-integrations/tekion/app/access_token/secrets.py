import boto3
from aws_lambda_powertools import Logger
from os import environ

from .schemas import TekionCredentials

ENVIRONMENT = environ.get("ENVIRONMENT", "test")
TEKION_AUTH_SECRETS_ID = environ.get(
    "TEKION_AUTH_SECRETS_ID",
    f"{"prod" if ENVIRONMENT == "prod" else "test"}/crm-tekion"
)

logger = Logger()


def get_credentials_from_secrets() -> TekionCredentials:
    logger.debug(
        "Fetching credentials from Secrets Manager",
        extra={
            "secrets_manager_id": TEKION_AUTH_SECRETS_ID
        }
    )
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=TEKION_AUTH_SECRETS_ID)
    content = response["SecretString"]
    return TekionCredentials.model_validate_json(json_data=content)
