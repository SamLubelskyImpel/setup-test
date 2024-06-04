from os import environ

import boto3

from .schemas import TekionCredentials

ENVIRONMENT = environ.get("ENVIRONMENT", "test")
SECRETS_MANAGER_ID = environ.get(
    "SECRETS_MANAGER_ID", f"{"prod" if ENVIRONMENT == "prod" else "test" }/crm-tekion"
)


def get_credentials_from_secrets() -> TekionCredentials:
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(SecretId=SECRETS_MANAGER_ID)
    content = response["SecretString"]
    return TekionCredentials.model_validate_json(json_data=content)