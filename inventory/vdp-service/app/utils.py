"""Utility functions for the VDP Service."""

from datetime import datetime
from json import loads, dumps
from typing import Any
from os import environ
from logging import getLogger

import paramiko
import boto3

logger = getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

SNS_TOPIC_ARN = environ.get("SNS_TOPIC_ARN")
ENVIRONMENT = environ["ENVIRONMENT"]

sm_client = boto3.client("secretsmanager")


def get_sftp_secrets(secret_name: Any, secret_key: Any) -> Any:
    """Get SFTP secret from Secrets Manager."""
    secret = sm_client.get_secret_value(
        SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/{secret_name}"
    )
    secret = loads(secret["SecretString"])[str(secret_key)]
    secret_data = loads(secret)

    return secret_data["hostname"], secret_data["port"], secret_data["username"], secret_data["password"]


def connect_sftp_server(hostname, port, username, password):
    """Connect to SFTP server and return the connection."""
    transport = paramiko.Transport((hostname, port))
    transport.connect(username=username, password=password)
    sftp = paramiko.SFTPClient.from_transport(transport)
    return sftp
