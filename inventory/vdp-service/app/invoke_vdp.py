"""Checks for modified VDP files on the SFTP server and sends them to the download queue."""

import json
import logging
from datetime import datetime, timedelta, timezone
from os import environ
from typing import Any

import boto3

from rds_instance import RDSInstance
from utils import connect_sftp_server, get_sftp_secrets

ENVIRONMENT = environ.get("ENVIRONMENT", "test")
DOWNLOAD_VDP_QUEUE_URL = environ["DOWNLOAD_VDP_QUEUE_URL"]
SFTP_SECRET_KEY = environ["SFTP_SECRET_KEY"]

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
sqs_client = boto3.client("sqs")


def send_to_download_queue(message):
    """Send message to the download queue."""
    try:
        response = sqs_client.send_message(
            QueueUrl=DOWNLOAD_VDP_QUEUE_URL,
            MessageBody=json.dumps(message),
        )
        logger.info(f"Message {message} added to the download queue: {response}")
    except Exception:
        logger.exception("Error occurred while sending message to the download queue")
        raise


def get_new_vdp_files(sftp, active_dealers, last_modified_time) -> list:
    """Get and filter VDP files modified after the last_modified_time."""
    vdp_files: dict[str, dict[str, Any]] = {}

    # Retrieve and process files from the SFTP server
    for file_attr in sftp.listdir_attr("."):
        file_name = file_attr.filename.lower()
        modification_time = datetime.fromtimestamp(
            file_attr.st_mtime, tz=timezone.utc
        )

        # Skip files modified before the last_modified_time
        if modification_time < last_modified_time:
            continue

        # Validate file extension
        if not file_name.endswith(".csv"):
            logger.warning(f"Invalid file extension for file: {file_name}. Only '.csv' files are allowed.")
            continue

        # Validate file name and check for "_vdp"
        if "_vdp" not in file_name:
            continue

        # Extract dealer ID
        dealer_id = file_name.split('_vdp')[0]
        if not dealer_id:
            logger.warning(f"Error parsing dealer ID from file name: {file_name}")
            continue

        # Check if dealer is active
        if dealer_id not in active_dealers:
            logger.warning(f"Skipping file: {file_name}. Dealer ID '{dealer_id}' is not active.")
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
    try:
        current_time = datetime.now(timezone.utc)

        # This is the time before the last pull time
        last_modified_time = current_time - timedelta(minutes=65)  # 1hr + 5min buffer
        logger.info(f"Checking for files modified since {last_modified_time.isoformat()}")

        rds_instance = RDSInstance()

        active_dealers = rds_instance.get_active_dealer_partners()
        active_dealers = [dealer[0] for dealer in active_dealers]
        logger.info(f"Active dealers: {active_dealers}")

        if not active_dealers:
            logger.warning("No active dealers found. Exiting.")
            return

        hostname, port, username, password = get_sftp_secrets("inventory-integrations-sftp", SFTP_SECRET_KEY)
        sftp_conn = connect_sftp_server(hostname, port, username, password)

        files = get_new_vdp_files(
            sftp_conn, active_dealers, last_modified_time
        )

        sftp_conn.close()

        if not files:
            logger.info("No new files found. Exiting.")
            return

        message = {
            "files": files,
            "search_time": current_time.isoformat(),
        }
        logger.info(f"Message to be sent to the download queue: {message}")
        send_to_download_queue(message)

    except Exception:
        logger.exception("Error occurred while checking SFTP for modified files")
        raise
