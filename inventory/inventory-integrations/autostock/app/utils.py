import boto3
import json
from datetime import datetime, timezone
from os import environ
from typing import Any
import paramiko
import logging
import requests


secret_client = boto3.client("secretsmanager")

ENVIRONMENT = environ["ENVIRONMENT"]

sm_client = boto3.client("secretsmanager")

def connect_sftp_server(hostname, port, username, password):
    """Connect to SFTP server and return the connection."""
    transport = paramiko.Transport((hostname, port))
    transport.connect(username=username, password=password)
    sftp = paramiko.SFTPClient.from_transport(transport)
    return sftp

def get_secrets(secret_name, secret_data_key, client_id=None):
    """Retrieve API secret from AWS Secrets Manager."""
    secret = secret_client.get_secret_value(
        SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/{secret_name}"
    )
    secret_data = json.loads(secret["SecretString"])

    if client_id:
        secret_data = json.loads(secret_data.get(client_id))

    try:
        value = json.loads(secret_data[secret_data_key])
    except:
        value = secret_data[secret_data_key]

    return value

def call_inventory_internal_api(endpoint: str):
    """Call Inventory Internal API."""

    client_id = 'impel' if ENVIRONMENT == 'prod' else 'test'
    api_key = get_secrets("InventoryInternalApi", "api_key", client_id)
    url = f"{environ.get('INVENTORY_INTERNAL_API_URL')}/{endpoint}"

    try:
        logging.info(f"Calling inventory internal api call on URL: {url}")
        response = requests.get(
            url=url,
            headers={
                "x_api_key": api_key,
                "client_id": client_id,
            },
        )
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logging.error(f"Error occurred calling Inventory Internal API: {e}")
        raise e
