from os import environ
from aws_lambda_powertools import Logger
import boto3

from .schemas import TekionCredentials

ENVIRONMENT = environ.get("ENVIRONMENT", "test")
SECRETS_MANAGER_ID = environ.get(
    "SECRETS_MANAGER_ID", f"{"prod" if ENVIRONMENT == "prod" else "test" }/crm-tekion"
)

logger = Logger()


def get_credentials_from_secrets() -> TekionCredentials:
    logger.debug(
        "Fetching credentials from Secrets Manager",
        extra={
            "secrets_manager_id": SECRETS_MANAGER_ID
        }
    )
    #client = boto3.client("secretsmanager")
    #response = client.get_secret_value(SecretId=SECRETS_MANAGER_ID)
    #content = response["SecretString"]
    #return TekionCredentials.model_validate_json(json_data=content)
    return TekionCredentials(
        auth_uri="https://api.tekioncloud.xyz",
        access_key="53625187faf146099f7db83da08f298a",
        secret_key="%Jmze%65c!eOX6TM",
        client_id="f4f9028170cf4e93bd43f6aef5042eb6",
    )
