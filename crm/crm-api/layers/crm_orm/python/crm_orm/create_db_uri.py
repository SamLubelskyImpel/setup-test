"""Return appropriate DB URI."""
from json import loads
from typing import Dict

import boto3

SM_CLIENT = boto3.client("secretsmanager")


def __get_db_secrets(secretId: str) -> str:
    """Get DB secrets from SecretsManager."""
    SecretString: Dict[str, str] = loads(SM_CLIENT.get_secret_value(SecretId=secretId)["SecretString"])

    return "postgresql://{}:{}@{}/{}".format(
        SecretString["user"],
        SecretString["password"],
        SecretString["host"],
        SecretString["db_name"],
    )


def create_db_uri(env: str) -> str:
    """Construct and return database URI."""
    sm_env: str = f"{'prod' if env == 'prod' else 'test'}/RDS/CRM"
    uri: str = __get_db_secrets(sm_env)

    return uri
