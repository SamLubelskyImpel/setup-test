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


def get_modified_sftp_files(sftp, folder_name, last_modified_time) -> list:
    """Get files modified after the last_modified_time."""
    return [
        {
            'file_name': file_attr.filename,
            'modification_time': int(datetime.fromtimestamp(file_attr.st_mtime, tz=timezone.utc).timestamp())
        }
        for file_attr in sftp.listdir_attr(folder_name)
        if datetime.fromtimestamp(file_attr.st_mtime, tz=timezone.utc) >= last_modified_time
    ]


def sort_modified_files(modified_files, active_dealers):
    """Sort modified files into inventory and VDP files."""
    inventory_files = {}
    vdp_files = {}

    for file_info in modified_files:
        file_name = file_info["file_name"].lower()
        modification_time = file_info["modification_time"]

        if "_inventory_" in file_name:
            dealer_id = file_name.split('_inventory_')[0]
            is_inventory = True
        elif "_vdp" in file_name:
            dealer_id = file_name.split('_vdp')[0]
            is_inventory = False
        else:
            logger.warning(f"Error parsing file name: {file_name}. Invalid file name")
            continue

        if not file_name.endswith(".csv"):
            logger.warning(f"Error parsing file name: {file_name}. Invalid file extension")
            continue

        if dealer_id not in active_dealers:
            continue

        if is_inventory:
            if dealer_id not in inventory_files or inventory_files[dealer_id]["modification_time"] < modification_time:
                inventory_files[dealer_id] = {
                    "modification_time": modification_time,
                    "file_name": file_name
                }
        else:
            vdp_files[dealer_id] = {
                "modification_time": modification_time,
                "file_name": file_name
            }

    return inventory_files, vdp_files


def get_new_files(sftp, folder_name, active_dealers, last_modified_time) -> list:
    """Get new files from the SFTP server."""
    modified_files = get_modified_sftp_files(sftp, folder_name, last_modified_time)
    inventory_files, vdp_files = sort_modified_files(modified_files, active_dealers)

    selected_files = [
        {
            "provider_dealer_id": dealer_id,
            "inventory_file": inventory_files[dealer_id]["file_name"] if dealer_id in inventory_files else None,
            "inventory_modification_time": inventory_files[dealer_id]["modification_time"] if dealer_id in inventory_files else None,
            "vdp_file": vdp_files[dealer_id]["file_name"] if dealer_id in vdp_files else None,
            "vdp_modification_time": vdp_files[dealer_id]["modification_time"] if dealer_id in vdp_files else None
        }
        for dealer_id in active_dealers
        if dealer_id in inventory_files or dealer_id in vdp_files
    ]

    return selected_files


def lambda_handler(event, context):
    """Check for modified feed files on the SFTP server."""
    logger.info(f"Event: {event}")
    logger.info(f"request_id: {event['request_id']}")
    try:
        current_time = datetime.now(timezone.utc)
        last_modified_time = current_time - timedelta(minutes=65)  # 1hr + 5min buffer
        logger.info(f"Checking for files modified since {last_modified_time.isoformat()}")

        rds_instance = RDSInstance()

        impel_integration_partner_id = event.get("impel_integration_partner_id")

        active_dealers = rds_instance.select_db_active_dealer_partners(impel_integration_partner_id)
        active_dealers = [dealer[0] for dealer in active_dealers]
        logger.info(f"Active dealers for {impel_integration_partner_id}: {active_dealers}")

        hostname, port, username, password = get_sftp_secrets("inventory-integrations-sftp", SECRET_KEY)
        sftp_conn = connect_sftp_server(hostname, port, username, password)

        folder_name = f"{impel_integration_partner_id}/"
        files = get_new_files(sftp_conn, folder_name, active_dealers, last_modified_time)

        sftp_conn.close()

        message = {
            "folder": folder_name,
            "files": files,
            "search_time": current_time.isoformat()
        }
        logger.info(f"Message to be sent to the download queue: {message}")
        send_to_download_queue(message)

    except Exception as e:
        send_alert_notification(event["request_id"], "invoke_sftp_coxau", "Error occurred while checking SFTP for modified files", e)
        logger.exception("Error occurred while checking SFTP for modified files")
        raise
