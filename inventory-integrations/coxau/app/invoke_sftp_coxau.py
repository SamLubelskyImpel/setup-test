import logging
from os import environ
from datetime import datetime, timedelta, timezone
import json
import boto3
from typing import Any
from utils import get_sftp_secrets, connect_sftp_server
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


def get_file_modification_time(sftp, file_name):
    """Get the modification time of a file on the SFTP server."""
    attrs = sftp.stat(file_name)
    modification_time = datetime.fromtimestamp(attrs.st_mtime, tz=timezone.utc)
    return modification_time


def list_and_filter(sftp, folder_name, active_dealers, last_modified_time):
    """List and filter files in the folder."""
    files = sftp.listdir(folder_name)
    logger.info(f"Files in the folder: {files}")

    # Filename format: {dealer_id}_Inventory_{timestamp}.csv
    selected_files = {}
    for file in files:
        # Filter files by active dealers
        try:
            dealer_id = file.split('_Inventory_')[0]
            file_name, extension = file.rsplit('.', 1)
            if extension != "csv":
                logger.warning(f"Invalid file extension: {file}")
                continue
        except Exception:
            logger.warning(f"Invalid file name: {file}")
            continue

        if dealer_id not in active_dealers:
            continue

        # Filter files modified before the last_modified_time
        modification_time = get_file_modification_time(sftp, folder_name + file)
        logger.info(f"File {file} was last modified on {modification_time}")

        if modification_time < last_modified_time:
            continue

        # Select most recent file for each dealer
        if dealer_id in selected_files:
            if selected_files[dealer_id]["modification_time"] < int(modification_time.timestamp()):
                selected_files[dealer_id] = {
                    "provider_dealer_id": dealer_id,
                    "modification_time": int(modification_time.timestamp()),
                    "file_name": file
                }
        else:
            selected_files[dealer_id] = {
                "provider_dealer_id": dealer_id,
                "modification_time": int(modification_time.timestamp()),
                "file_name": file
            }

    # Extract the selected files from the selected_files dictionary
    selected_files_list = list(selected_files.values())
    logger.info(f"Selected files: {selected_files_list}")
    return selected_files_list


def lambda_handler(event: Any, context: Any) -> Any:
    """Check for modified feed files on the SFTP server."""
    logger.info(f"Event: {event}")

    try:
        current_time = datetime.now(timezone.utc)
        last_modified_time = current_time - timedelta(minutes=65)  # 1hr + 5min buffer
        logger.info(f"Checking for files modified since {last_modified_time.isoformat()}")

        rds_instance = RDSInstance()
        active_dealers = rds_instance.select_db_active_dealer_partners("coxau")
        active_dealers = [dealer[0] for dealer in active_dealers]
        logger.info(f"Active dealers: {active_dealers}")

        hostname, port, username, password = get_sftp_secrets("inventory-integrations-sftp", SECRET_KEY)
        sftp_conn = connect_sftp_server(hostname, port, username, password)

        # List files in the folder
        folder_name = "coxau/"
        files = list_and_filter(sftp_conn, folder_name, active_dealers, last_modified_time)

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
