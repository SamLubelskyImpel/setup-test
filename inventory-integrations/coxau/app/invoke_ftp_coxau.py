import logging
from os import environ
from datetime import datetime, timedelta, timezone
from ftplib import FTP
import json
import boto3
from typing import Any

ENVIRONMENT = environ["ENVIRONMENT"]
DOWNLOAD_QUEUE_URL = environ["DOWNLOAD_QUEUE_URL"]
SECRET_KEY = environ["FTP_SECRET_KEY"]

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
sm_client = boto3.client("secretsmanager")
sqs_client = boto3.client("sqs")


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


def list_files(ftp, folder):
    """List files in a folder on the FTP server."""
    ftp.cwd(folder)
    files = ftp.nlst()
    return files


def get_file_modification_time(ftp, file_name):
    """Get the modification time of a file on the FTP server."""
    modification_time_str = ftp.sendcmd('MDTM ' + file_name)
    modification_time = datetime.strptime(modification_time_str[4:], '%Y%m%d%H%M%S.%f').replace(tzinfo=timezone.utc)
    return modification_time


def send_to_download_queue(message):
    """Send message to the download queue."""
    try:
        response = sqs_client.send_message(
            QueueUrl=DOWNLOAD_QUEUE_URL,
            MessageBody=json.dumps(message),
        )
        logger.info(f"Message {message} added to the download queue: {response}")
    except Exception as e:
        logger.error(f"Error occurred while sending message to the download queue: {e}")
        raise e


def lambda_handler(event: Any, context: Any) -> Any:
    """Check for modified feed files on the FTP server."""
    logger.info(f"Event: {event}")

    try:
        current_time = datetime.now(timezone.utc)
        last_modified_time = current_time - timedelta(minutes=75)  # 1.25 hours ago
        logger.info(f"Checking for files modified since {last_modified_time.isoformat()}")

        hostname, username, password = get_ftp_secrets("inventory-integrations-ftp", SECRET_KEY)
        ftp_conn = connect_ftp_server(hostname, username, password)

        # List files in the folder
        folder_name = "coxau/"
        files = list_files(ftp_conn, folder_name)
        logger.info(f"Files in the folder: {files}")

        def file_filter(file):
            modification_time = get_file_modification_time(ftp_conn, file)
            logger.info(f"File '{file}' was last modified on {modification_time}")
            return modification_time > last_modified_time

        updated_files = list(filter(file_filter, files))
        logger.info(f"Updated files: {updated_files}")
        ftp_conn.quit()

        message = {
            "folder": folder_name,
            "files": updated_files,
            "search_time": current_time.isoformat()
        }
        logger.info(f"Message to be sent to the download queue: {message}")
        send_to_download_queue(message)

    except Exception as e:
        logger.error(f"Error occurred while checking FTP for modified files: {e}")
        raise
