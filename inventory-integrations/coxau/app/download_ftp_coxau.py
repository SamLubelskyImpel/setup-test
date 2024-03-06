import boto3
import json
import logging
import os
import tempfile
from typing import Any
from ftplib import FTP
from datetime import datetime
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)

INVENTORY_BUCKET = os.environ["INVENTORY_BUCKET"]
ENVIRONMENT = os.environ["ENVIRONMENT"]
SECRET_KEY = os.environ["FTP_SECRET_KEY"]

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOGLEVEL", "INFO").upper())
sm_client = boto3.client("secretsmanager")
s3_client = boto3.client("s3")


def get_ftp_secrets(secret_name: Any, secret_key: Any) -> Any:
    """Get FTP secret from Secrets Manager."""
    secret = sm_client.get_secret_value(
        SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/{secret_name}"
    )
    secret = json.loads(secret["SecretString"])[str(secret_key)]
    secret_data = json.loads(secret)

    return secret_data["hostname"], secret_data["username"], secret_data["password"]


def connect_ftp_server(hostname, username, password):
    """Connect to FTP server and return the connection."""
    ftp = FTP(hostname)
    ftp.login(username, password)
    return ftp


def download_and_process_file(ftp, remote_file_path, local_folder):
    """Download and process a file from the FTP server."""
    # Change to the local folder where you want to save the file
    os.chdir(local_folder)

    # Use RETR command to download the file
    local_file_name = os.path.basename(remote_file_path)
    with open(local_file_name, 'wb') as local_file:
        ftp.retrbinary('RETR ' + remote_file_path, local_file.write)

    logger.info(f"File {local_file_name} downloaded successfully.")

    upload_to_s3(local_file_name, remote_file_path)
    os.remove(local_file_name)


def upload_to_s3(local_filename, filename):
    """Upload files to S3."""
    format_string = '%Y/%m/%d/%H'
    date_key = datetime.utcnow().strftime(format_string)

    s3_key = f"raw/coxau/{date_key}/{filename}"
    s3_client.upload_file(
        Filename=local_filename,
        Bucket=INVENTORY_BUCKET,
        Key=s3_key
    )
    logger.info(f"File {filename} uploaded to S3.")


def record_handler(record: SQSRecord) -> Any:
    """Download files from the FTP server and upload to S3."""
    logger.info(f"Record: {record}")

    try:
        body = json.loads(record["body"])
        folder_name = body["folder"]
        file_names = body["files"]

        if not file_names:
            logger.info("No files to download from the FTP server.")
            return

        # Get FTP secrets
        hostname, username, password = get_ftp_secrets("inventory-integrations-ftp", SECRET_KEY)

        # Connect to FTP server
        ftp_conn = connect_ftp_server(hostname, username, password)
        ftp_conn.cwd(folder_name)

        # Process files one by one
        for remote_file_path in file_names:
            # Create a temporary directory to download the file
            with tempfile.TemporaryDirectory() as temp_dir:
                # Download and process the file
                download_and_process_file(ftp_conn, remote_file_path, temp_dir)

        ftp_conn.quit()

    except Exception as e:
        logger.error(f"Error occurred while downloading files from FTP server: {e}")
        raise


def lambda_handler(event: Any, context: Any) -> Any:
    """Download files from the FTP server and upload to S3."""
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

    except Exception as e:
        logger.error(f"Error processing ReyRey new lead: {e}")
        raise
