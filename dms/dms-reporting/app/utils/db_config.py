import boto3
from datetime import datetime, timedelta
import logging
from json import loads
from os import environ
import psycopg2


env = environ["ENVIRONMENT"]

SM_CLIENT = boto3.client("secretsmanager")


logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def __get_db_secrets():
    """Get DB secrets from SecretsManager."""
    secretId = f"{'prod' if env == 'prod' else 'test'}/DMSDB"
    
    SecretString = loads(SM_CLIENT.get_secret_value(SecretId=secretId)["SecretString"])
    DB = {
        'DB_USER': SecretString["user"],
        'DB_PASSWORD': SecretString["password"],
        'DB_HOST': SecretString["host"],
        'DB_NAME': SecretString["db_name"],
        'DB_PORT': 5432
    }
    return DB

def get_connection():
    """Return DMS DB connection."""

    DB = __get_db_secrets()

    # Retrieve database connection details from environment variables
    db_host = DB['DB_HOST']
    db_port = DB['DB_PORT']
    db_name = DB['DB_NAME']
    db_user = DB['DB_USER']
    db_password = DB['DB_PASSWORD']

    # Establish a connection to the PostgreSQL database
    conn = psycopg2.connect(
        host=db_host,
        port=db_port,
        database=db_name,
        user=db_user,
        password=db_password
    )
    return conn