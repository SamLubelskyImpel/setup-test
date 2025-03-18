"""Download files from the SFTP server and upload to S3."""

import json
import logging
import os
import tempfile
from typing import Any

import boto3
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord

from utils import connect_sftp_server, get_sftp_secrets

INVENTORY_BUCKET = os.environ["INVENTORY_BUCKET"]
ENVIRONMENT = os.environ["ENVIRONMENT"]

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOGLEVEL", "INFO").upper())

s3_client = boto3.client("s3")
sqs_client = boto3.client("sqs")


def upload_to_s3(folder_name, local_filename, provider_dealer_id):
    """Upload files to S3."""
    s3_key = f"vdp/{folder_name}/{provider_dealer_id}.csv"
    s3_client.upload_file(Filename=local_filename, Bucket=INVENTORY_BUCKET, Key=s3_key)
    logger.info(f"File {s3_key} uploaded to S3.")


def process_file(sftp_conn, folder_name, file):
    """Download a file from the SFTP server and upload it to S3."""
    try:
        # Create a temporary directory to download the file
        with tempfile.TemporaryDirectory() as temp_dir:
            provider_dealer_id = file["provider_dealer_id"]
            remote_file_path = file["vdp_file"]
            modification_time = file["modification_time"]

            # Check if required parameters are present
            if not all([remote_file_path, provider_dealer_id, modification_time]):
                logger.error("Missing required parameters for file download.")
                raise ValueError("remote_file_path, provider_dealer_id, and export_file_name are required.")

            os.chdir(temp_dir)
            local_filename = os.path.basename(remote_file_path)

            # Download file from SFTP server
            sftp_conn.get(remote_file_path, local_filename)
            logger.info(f"File {local_filename} downloaded successfully.")

            upload_to_s3(folder_name, local_filename, provider_dealer_id)
            os.remove(local_filename)

    except Exception as e:
        logger.exception(f"Error occurred while processing file: {file}")
        raise e


def record_handler(record: SQSRecord) -> Any:
    """Download files from the SFTP server and upload to S3."""
    logger.info(f"Record: {record}")
    try:
        body = json.loads(record["body"])
        folder_name = body["folder"]
        files = body["files"]
        sftp_secret_key = body["sftp_secret_key"]

        if not files:
            logger.info("No files to download from the SFTP server.")
            return

        # Get SFTP secrets
        hostname, port, username, password = get_sftp_secrets(
            "inventory-integrations-sftp", sftp_secret_key
        )

        # Connect to SFTP server
        sftp_conn = connect_sftp_server(hostname, port, username, password)
        sftp_conn.chdir(f"{folder_name}/")

        # Process files one by one
        for file in files:
            process_file(sftp_conn, folder_name, file)

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
            context=context,
        )
        return result

    except Exception:
        logger.exception("Error occurred while processing the event.")
        raise
