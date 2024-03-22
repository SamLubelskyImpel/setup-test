import logging
from os import environ
from datetime import datetime, timedelta, timezone
import json
import boto3
from typing import Any
from utils import get_sftp_secrets, connect_sftp_server, get_file_modification_time
from rds_instance import RDSInstance

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


def list_and_filer(sftp, folder_name, active_dealers, last_modified_time):
    """List and filter files in the folder."""
    files = sftp.listdir(folder_name)
    logger.info(f"Files in the folder: {files}")

    to_ignore = []
    to_download = []
    for file in files:
        try:
            file_name = file.split(".csv")[0]
        except IndexError:
            logger.exception(f"Invalid file name: {file}")
            to_ignore.append(file)
            continue

        if file_name not in active_dealers:
            to_ignore.append(file)
            continue

        modification_time = get_file_modification_time(sftp, folder_name + file)
        logger.info(f"File {file} was last modified on {modification_time}")
        if modification_time < last_modified_time:
            to_ignore.append(file)
            continue

        to_download.append({
            "file_name": file,
            "modification_time": modification_time.strftime('%Y-%m-%dT%H:%M:%SZ')
        })

    logger.info(f"Ignoring files: {to_ignore}")
    logger.info(f"Files to download: {to_download}")
    return to_download


def lambda_handler(event: Any, context: Any) -> Any:
    """Check for modified feed files on the SFTP server."""
    logger.info(f"Event: {event}")

    try:
        current_time = datetime.now(timezone.utc)
        last_modified_time = current_time - timedelta(minutes=65)  # 1hr + 5min buffer
        logger.info(f"Checking for files modified since {last_modified_time.isoformat()}")

        rds_instance = RDSInstance()
        active_dealers = rds_instance.select_db_active_dealer_partners("coxau")

        hostname, port, username, password = get_sftp_secrets("inventory-integrations-sftp", SECRET_KEY)
        sftp_conn = connect_sftp_server(hostname, port, username, password)

        # List files in the folder
        folder_name = "coxau/"
        files = list_and_filer(sftp_conn, folder_name, active_dealers, last_modified_time)

        sftp_conn.close()

        message = {
            "folder": folder_name,
            "files": files,
            "search_time": current_time.isoformat()
        }
        logger.info(f"Message to be sent to the download queue: {message}")
        send_to_download_queue(message)

    except Exception:
        logger.exception("Error occurred while checking SFTP for modified files")
        raise
