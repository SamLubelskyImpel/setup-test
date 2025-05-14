import boto3
import json
from os import environ
from typing import Any
import paramiko
import logging

ENVIRONMENT = environ["ENVIRONMENT"]
SNS_TOPIC_ARN = environ.get("SNS_TOPIC_ARN")

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


def send_alert_notification(request_id: str, endpoint: str, e: Exception):
    """Send alert notification to CE team."""
    data = {
        "message": f"Error occurred in {endpoint} for request_id {request_id}: {e}",
    }
    sns_client = boto3.client("sns")

    sns_client.publish(
        TopicArn=SNS_TOPIC_ARN,
        Message=json.dumps({"default": json.dumps(data)}),
        Subject=f"Inventory Integration ICC: {endpoint} Failure Alert",
        MessageStructure="json",
    )

    logging.info(f"Alert sent to CE team for {endpoint} with request_id {request_id}")
