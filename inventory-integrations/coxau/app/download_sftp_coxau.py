import boto3
import json
import logging
import os
import tempfile
from typing import Any
from datetime import datetime
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)
from utils import get_sftp_secrets, connect_sftp_server

INVENTORY_BUCKET = os.environ["INVENTORY_BUCKET"]
ENVIRONMENT = os.environ["ENVIRONMENT"]
SECRET_KEY = os.environ["SFTP_SECRET_KEY"]

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOGLEVEL", "INFO").upper())
s3_client = boto3.client("s3")


def download_and_process_file(sftp, remote_file_path, local_folder, modification_time):
    """Download and process a file from the SFTP server."""
    # Change to the local folder where you want to save the file
    os.chdir(local_folder)

    # Use get method to download the file
    local_file_name = os.path.basename(remote_file_path)
    sftp.get(remote_file_path, local_file_name)

    logger.info(f"File {local_file_name} downloaded successfully.")

    upload_to_s3(local_file_name, remote_file_path, modification_time)

    os.remove(local_file_name)


def upload_to_s3(local_filename, filename, modification_time):
    """Upload files to S3."""
    format_string = '%Y/%m/%d/%H'
    date_key = datetime.utcnow().strftime(format_string)
    base_name, extension = filename.rsplit('.', 1)

    s3_key = f"raw/coxau/{date_key}/{base_name}_{modification_time}.{extension}"
    s3_client.upload_file(
        Filename=local_filename,
        Bucket=INVENTORY_BUCKET,
        Key=s3_key
    )
    logger.info(f"File {filename} uploaded to S3.")


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
        hostname, port, username, password = get_sftp_secrets("inventory-integrations-sftp", SECRET_KEY)

        # Connect to SFTP server
        sftp_conn = connect_sftp_server(hostname, port, username, password)
        sftp_conn.chdir(folder_name)

        # Process files one by one
        for file in files:
            remote_file_path = file["file_name"]
            modification_time = datetime.strptime(file["modification_time"], '%Y-%m-%dT%H:%M:%SZ')

            # Create a temporary directory to download the file
            with tempfile.TemporaryDirectory() as temp_dir:
                # Download and process the file
                download_and_process_file(sftp_conn, remote_file_path, temp_dir, modification_time)

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
