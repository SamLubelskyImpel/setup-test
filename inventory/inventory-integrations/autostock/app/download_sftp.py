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
from utils import get_secrets, connect_sftp_server
from datetime import datetime, timezone

INVENTORY_BUCKET = os.environ["INVENTORY_BUCKET"]
ENVIRONMENT = os.environ["ENVIRONMENT"]
SECRET_KEY = os.environ["SFTP_SECRET_KEY"]

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOGLEVEL", "INFO").upper())
s3_client = boto3.client("s3")
sqs_client = boto3.client("sqs")


def download_file(sftp, file, local_folder):
    """Download from the SFTP server and upload to S3."""
    provider_dealer_id = file["provider_dealer_id"]
    remote_file_path = file["file"]

    # Change to the local folder where you want to save the file
    os.chdir(local_folder)

    # Use get method to download the file
    local_file_name = os.path.basename(remote_file_path)
    sftp.get(remote_file_path, local_file_name)

    logger.info(f"File {local_file_name} downloaded successfully.")

    upload_to_s3(local_file_name, provider_dealer_id)

    os.remove(local_file_name)


def upload_to_s3(local_filename, provider_dealer_id):
    """Upload files to S3."""
    format_string = '%Y/%m/%d/%H'
    now = datetime.now(timezone.utc)
    date_key = now.strftime(format_string)
    s3_file_name = f"{provider_dealer_id}_{int(now.timestamp())}.csv"

    s3_key = f"raw/autostock/{date_key}/{s3_file_name}"
    s3_client.upload_file(
        Bucket=INVENTORY_BUCKET,
        Key=s3_key,
        Filename=local_filename
    )

    logger.info(f"File {s3_key} uploaded to S3.")


def record_handler(record: SQSRecord) -> Any:
    """Download files from the SFTP server and upload to S3."""
    logger.info(f"Record: {record}")

    try:
        body = json.loads(record["body"])
        folder_name = body["folder"]
        files = body["files"]

        if not files:
            logger.info("No files to download from the SFTP server.")
            return

        # Get SFTP secrets
        secret = get_secrets("inventory-integrations-sftp", SECRET_KEY)
        hostname, port, username, password = secret["hostname"], secret["port"], secret["username"], secret["password"]

        # Connect to SFTP server
        sftp_conn = connect_sftp_server(hostname, port, username, password)
        sftp_conn.chdir(folder_name)

        # Process files one by one
        for file in files:
            # Create a temporary directory to download the file
            with tempfile.TemporaryDirectory() as temp_dir:
                # Download the file
                download_file(sftp_conn, file, temp_dir)

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
