from os import environ
import boto3
from json import loads, dumps
import paramiko
import logging

ENVIRONMENT = environ.get("ENVIRONMENT")
SNS_TOPIC_ARN = environ.get("SNS_TOPIC_ARN")

sm_client = boto3.client("secretsmanager")
logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

def get_sftp_secrets(secret_name, secret_key):
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

def send_alert_notification(request_id: str, endpoint: str, e: Exception) -> None:
    """Send alert notification to CE team."""
    data = {
        "message": f"Error occurred in {endpoint} for request_id {request_id}: {e}",
    }
    sns_client = boto3.client("sns")
    result =sns_client.publish(
        TopicArn=SNS_TOPIC_ARN,
        Message=dumps({"default": dumps(data)}),
        Subject=f"Inventory Service: {endpoint} Failure Alert",
        MessageStructure="json",
    )
    logger.info(f"Alert sent to CE team for {endpoint} with request_id {request_id}")

def send_alert_missing_inventory_files(request_id: str, error_report: list) -> None:
    """Send missing/outdated inventory files alert notification to CE team."""
    data = {
        "message": f"Missing/outdated inventory files for request_id {request_id}: {error_report}",
    }
    try:
        sns_client = boto3.client("sns")
        result = sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=dumps({"default": dumps(data)}),
            Subject=f"Inventory Service: Missing/outdated Inventory Files Alert",
            MessageStructure="json",
        )
        logger.info(f"Alert sent to CE team with request_id {request_id} - {result}")
    
    except Exception as e:
        logger.error(f"Failed to send inventory file syndication alert: {e}")
    
    