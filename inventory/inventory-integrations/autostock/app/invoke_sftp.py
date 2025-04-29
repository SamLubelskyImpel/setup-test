import logging
from os import environ
from datetime import datetime, timezone
import json
import boto3
from typing import Any
from utils import get_secrets, connect_sftp_server, call_inventory_internal_api
from paramiko import SFTPClient


ENVIRONMENT = environ["ENVIRONMENT"]
DOWNLOAD_QUEUE_URL = environ["DOWNLOAD_QUEUE_URL"]
SECRET_KEY = environ["SFTP_SECRET_KEY"]

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
sqs_client = boto3.client("sqs")


def send_to_download_queue(message):
    """Send message to the download queue."""
    try:
        response = sqs_client.send_message(
            QueueUrl=DOWNLOAD_QUEUE_URL,
            MessageBody=json.dumps(message),
        )
        logger.info(f"Message {message} added to the download queue: {response}")
    except Exception as e:
        logger.exception("Error occurred while sending message to the download queue")
        raise e


def get_sftp_files(sftp: SFTPClient, folder_name: str, dealers: list) -> list:
    """Get files for the list of dealers."""
    files = []

    for file_attr in sftp.listdir_attr(folder_name):
        dealer_id = file_attr.filename.split('.')[0]
        if dealer_id in dealers:
            files.append({
                "provider_dealer_id": dealer_id,
                "file": file_attr.filename
            })

    return files


def lambda_handler(event: Any, _: Any) -> Any:
    """Check for feed files on the SFTP server."""
    logger.info(f"Event: {event}")

    try:
        current_time = datetime.now(timezone.utc)

        active_dealers = call_inventory_internal_api('dealer/v1/?integration_partner_name=autostock')
        logger.info(f"Active dealers: {active_dealers}")

        secret = get_secrets("inventory-integrations-sftp", SECRET_KEY)
        
        hostname, port, username, password = secret["hostname"], secret["port"], secret["username"], secret["password"]
        sftp_conn = connect_sftp_server(hostname, port, username, password)

        folder_name = "."
        files = get_sftp_files(sftp_conn, folder_name, active_dealers)

        sftp_conn.close()

        message = {
            "folder": folder_name,
            "files": files,
            "search_time": current_time.isoformat()
        }

        logger.info(f"Message to be sent to the download queue: {message}")
        send_to_download_queue(message)

    except Exception:
        logger.exception("Error occurred while checking SFTP for files")
        raise
