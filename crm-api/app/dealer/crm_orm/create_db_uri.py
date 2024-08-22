"""Return appropriate DB URI."""

from json import loads
from typing import Dict

import boto3

SM_CLIENT = boto3.client("secretsmanager")


def __get_db_secrets(secretId: str) -> str:
    """Get DB secrets from SecretsManager."""
    SecretString: Dict[str, str] = {
        "jdbc_url": "jdbc:postgresql://db-unified-data-test-1.testenv.impel.io:5432/dms",
        "user": "developer",
        "password": "31eeeb5d8805f3f6e0ee686a47f47bd5",
        "db_name": "dms",
        "host": "db-unified-data-test-1.testenv.impel.io",
        "port": "5432",
    }
    # loads(SM_CLIENT.get_secret_value(SecretId=secretId)["SecretString"])

    return "postgresql://{}:{}@{}/{}".format(
        SecretString["user"],
        SecretString["password"],
        SecretString["host"],
        SecretString["db_name"],
    )


def create_db_uri(env: str) -> str:
    """Construct and return database URI."""
    sm_env: str = f"{'prod' if env == 'prod' else 'test'}/DMSDB"
    uri: str = __get_db_secrets(sm_env)

    return uri
