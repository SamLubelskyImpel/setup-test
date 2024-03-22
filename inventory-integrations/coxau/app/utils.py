import boto3
import json
from datetime import datetime, timezone
from os import environ
from typing import Any
import paramiko

ENVIRONMENT = environ["ENVIRONMENT"]

sm_client = boto3.client("secretsmanager")


def get_sftp_secrets(secret_name: Any, secret_key: Any) -> Any:
    """Get SFTP secret from Secrets Manager."""
    secret = sm_client.get_secret_value(
        SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/{secret_name}"
    )
    secret = json.loads(secret["SecretString"])[str(secret_key)]
    secret_data = json.loads(secret)

    return secret_data["hostname"], secret_data["port"], secret_data["username"], secret_data["password"]


def connect_sftp_server(hostname, port, username, password):
    """Connect to SFTP server and return the connection."""
    transport = paramiko.Transport((hostname, port))
    transport.connect(username=username, password=password)
    sftp = paramiko.SFTPClient.from_transport(transport)
    return sftp


def get_file_modification_time(sftp, file_name):
    """Get the modification time of a file on the SFTP server."""
    attrs = sftp.stat(file_name)
    modification_time = datetime.fromtimestamp(attrs.st_mtime, tz=timezone.utc)
    return modification_time
