import boto3
import json
import logging
import os
import tempfile
from typing import Any
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)
from utils import get_sftp_secrets, connect_sftp_server, send_alert_notification
from uuid import uuid4

INVENTORY_BUCKET = os.environ["INVENTORY_BUCKET"]
ENVIRONMENT = os.environ["ENVIRONMENT"]
SECRET_KEY = os.environ["SFTP_SECRET_KEY"]
MERGE_QUEUE_URL = os.environ["MERGE_QUEUE_URL"]

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOGLEVEL", "INFO").upper())
s3_client = boto3.client("s3")
sqs_client = boto3.client("sqs")


def download_file(sftp, remote_file_path, local_folder, provider_dealer_id, export_file_name):
    """Download a file from the SFTP server."""
    local_file_name = os.path.basename(remote_file_path)
    sftp.get(remote_file_path, local_file_name)

    logger.info(f"File {local_file_name} downloaded successfully.")

    upload_to_s3(local_file_name, provider_dealer_id, export_file_name)
    os.remove(local_file_name)


def download_and_process_file(sftp, file, local_folder):
    """Download and process a file from the SFTP server."""
    provider_dealer_id = file["provider_dealer_id"]
    remote_inv_file_path = file["inventory_file"]
    remote_vdp_file_path = file["vdp_file"]
    inv_modification_time = file["inventory_modification_time"]
    vdp_modification_time = file["vdp_modification_time"]

    if not inv_modification_time and not vdp_modification_time:
        logger.error("Invalid modification time found for downloaded files.")
        raise ValueError("Both modification times are missing.")
    else:
        modification_time = max(inv_modification_time or vdp_modification_time, vdp_modification_time or inv_modification_time)

    # Change to the local folder where you want to save the file
    os.chdir(local_folder)

    # Use get method to download the file
    if remote_inv_file_path:
        download_file(sftp, remote_inv_file_path, local_folder, provider_dealer_id, "inventory")

    if remote_vdp_file_path:
        download_file(sftp, remote_vdp_file_path, local_folder, provider_dealer_id, "vdp")

    # Notify merge process
    message = {
        "provider_dealer_id": provider_dealer_id,
        "modification_time": modification_time,
    }

    sqs_client.send_message(
        QueueUrl=MERGE_QUEUE_URL,
        MessageBody=json.dumps(message),
    )


def upload_to_s3(local_filename, provider_dealer_id, export_file_name):
    """Upload files to S3."""
    s3_file_name = f"{export_file_name}.csv"

    s3_key = f"landing-zone/coxau/{provider_dealer_id}/{s3_file_name}"
    s3_client.upload_file(
        Filename=local_filename,
        Bucket=INVENTORY_BUCKET,
        Key=s3_key
    )
    logger.info(f"File {s3_file_name} uploaded to S3.")


def record_handler(record: SQSRecord) -> Any:
    """Download files from the SFTP server and upload to S3."""
    logger.info(f"Record: {record}")

    try:
        body = json.loads(record["body"])
        folder_name = body["folder"]
        files = body["files"]

        request_id = str(uuid4())

        if not files:
            logger.info("No files to download from the SFTP server.")
            send_alert_notification(
                request_id=request_id,
                endpoint="download_sftp",
                e=Exception("No files to download from the SFTP server.")
            )
            return

        # Get SFTP secrets
        hostname, port, username, password = get_sftp_secrets("inventory-integrations-sftp", SECRET_KEY)

        # Connect to SFTP server
        sftp_conn = connect_sftp_server(hostname, port, username, password)
        sftp_conn.chdir(folder_name)

        # Process files one by one
        for file in files:
            # Create a temporary directory to download the file
            with tempfile.TemporaryDirectory() as temp_dir:
                # Download and process the file
                download_and_process_file(sftp_conn, file, temp_dir)

        sftp_conn.close()

    except Exception as e:
        logger.exception("Error occurred while downloading files from SFTP server")
        raise e


def lambda_handler(event: Any, context: Any) -> Any:
    """Download files from the SFTP server and upload to S3."""
    logger.info(f"Event: {event}")

    try:
        processor = BatchProcessor(event_type=EventType.SQS)
        result = process_partial_response(
            event=event,
            record_handler=record_handler,
            processor=processor,
            context=context
        )
        return result

    except Exception:
        logger.exception("Error occurred while processing the event.")
        raise
