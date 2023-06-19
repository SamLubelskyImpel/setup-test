"""Daily Check Service Repair Order"""
import boto3
import logging
from datetime import date, datetime, timezone
from json import dumps, loads
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

def lambda_handler(event, context):
    """Daily check for new service repair order data."""

    pass



# The first lambda will query the dms rds for all active integrations in dealer_integration_partner to get a list of all active dealer integrations.
# Then query a count for the previous day all entries in the service_repair_order table grouped by dealer_integration_partner_id. 
# If an active dealer doesn’t appear in that list or has a count of 0, add that dealer to a list. 
# Then if that list has data, notify the TopicUnivClientEngineeringAlertTopic once. Then repeat this process for the vehicle_sale table.
