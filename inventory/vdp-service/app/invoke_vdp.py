import json
import logging
from datetime import datetime, timedelta, timezone
from os import environ
from typing import Any

import boto3

from rds_instance import RDSInstance
from utils import connect_sftp_server, get_sftp_secrets, send_alert_notification

ENVIRONMENT = environ["ENVIRONMENT"]
DOWNLOAD_QUEUE_URL = environ["DOWNLOAD_QUEUE_URL"]

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


def get_new_vdp_files(sftp, folder_name, active_dealers, last_modified_time) -> list:
    """Get and filter VDP files modified after the last_modified_time."""
    vdp_files: dict[str, dict[str, Any]] = {}

    # Retrieve and process files from the SFTP server
    folder_name = f"{folder_name}/"
    for file_attr in sftp.listdir_attr(folder_name):
        file_name = file_attr.filename.lower()
        modification_time = datetime.fromtimestamp(
            file_attr.st_mtime, tz=timezone.utc
        )

        # Skip files modified before the last_modified_time
        if modification_time < last_modified_time:
            continue

        # Validate file name and check for "_vdp"
        if "_vdp" not in file_name:
            continue

        # Extract dealer ID
        dealer_id = file_name.split('_vdp')[0]
        if not dealer_id:
            logger.warning(f"Error parsing dealer ID from file name: {file_name}")
            continue

        # Validate file extension
        if not file_name.endswith(".csv"):
            logger.warning(f"Invalid file extension for file: {file_name}. Only '.csv' files are allowed.")
            continue

        # Check if dealer is active
        if dealer_id not in active_dealers:
            logger.debug(f"Skipping file: {file_name}. Dealer ID '{dealer_id}' is not active.")
            continue

        # Update VDP files if it is the latest modification
        if dealer_id not in vdp_files or vdp_files[dealer_id]["modification_time"] < modification_time:
            vdp_files[dealer_id] = {
                "modification_time": int(modification_time.timestamp()),
                "file_name": file_name
            }
            logger.info(f"Added/Updated VDP file for dealer '{dealer_id}': {file_name}")

    logger.info(f"Filtered VDP files: {len(vdp_files)} files found.")

    # Prepare the final list of selected files directly from vdp_files
    selected_files = [
        {
            "provider_dealer_id": dealer_id,
            "vdp_file": details["file_name"],
            "modification_time": details["modification_time"]
        }
        for dealer_id, details in vdp_files.items()
    ]

    return selected_files


def lambda_handler(event, context):
    """Check for modified feed files on the SFTP server."""
    logger.info(f"Event: {event}")
    logger.info(f"request_id: {event['request_id']}")
    try:
        current_time = datetime.now(timezone.utc)

        # This is the time before the last pull time
        last_modified_time = current_time - timedelta(minutes=65)  # 1hr + 5min buffer
        logger.info(f"Checking for files modified since {last_modified_time.isoformat()}")

        rds_instance = RDSInstance()

        impel_integration_partner_id = event.get("impel_integration_partner_id")
        sftp_secret_key = event.get("sftp_secret_key")

        active_dealers = rds_instance.select_db_active_dealer_partners(impel_integration_partner_id)
        active_dealers = [dealer[0] for dealer in active_dealers]
        logger.info(f"Active dealers for {impel_integration_partner_id}: {active_dealers}")

        hostname, port, username, password = get_sftp_secrets("inventory-integrations-sftp", sftp_secret_key)
        sftp_conn = connect_sftp_server(hostname, port, username, password)

        files = get_new_vdp_files(
            sftp_conn, impel_integration_partner_id, active_dealers, last_modified_time
        )

        sftp_conn.close()

        message = {
            "folder": impel_integration_partner_id,
            "files": files,
            "search_time": current_time.isoformat()
        }
        logger.info(f"Message to be sent to the download queue: {message}")
        send_to_download_queue(message)

    except Exception:
        logger.exception("Error occurred while checking SFTP for modified files")
        raise
