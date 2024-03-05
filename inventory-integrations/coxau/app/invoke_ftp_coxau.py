import logging
from os import environ, path
from datetime import datetime
from ftplib import FTP
import json
import boto3
from typing import Any

environ["ENVIRONMENT"] = "test"
SECRET_KEY = "COXAU_FTP"

ENVIRONMENT = environ["ENVIRONMENT"]
# DOWNLOAD_QUEUE_URL = environ.get("DOWNLOAD_QUEUE_URL")
# SECRET_KEY = environ.get("FTP_SECRET_KEY")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
sm_client = boto3.client("secretsmanager")


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


def upload_file(ftp, local_file_path, remote_folder):
    # Change to the remote folder on the FTP server
    ftp.cwd(remote_folder)

    # Open the local file in binary mode for uploading
    with open(local_file_path, 'rb') as local_file:
        # Use STOR command to upload the file
        ftp.storbinary('STOR ' + path.basename(local_file_path), local_file)

    print(f"File '{path.basename(local_file_path)}' uploaded successfully.")


def get_file_modification_time(ftp, file_name):
    # Use the modified timestamp of the file on the server
    modification_time_str = ftp.sendcmd('MDTM ' + file_name)
    modification_time = datetime.strptime(modification_time_str[4:], '%Y%m%d%H%M%S.%f')
    return modification_time


def lambda_handler(event, context):
    """List modified files in the FTP server."""
    logger.info(f"Event: {event}")

    try:
        hostname, username, password = get_ftp_secrets("inventory-integrations-ftp", SECRET_KEY)
        # hostname = "swipetospin.exavault.com"
        # username = "stssftp_coxau_reader"
        # password = "%XP$sPDvXTUd36#*5s6mNW!cxCgT3YBCawfi6zFy@LxhQG7nizx7z9r4ig7acX@6"
        ftp_conn = connect_ftp_server(hostname, username, password)

        # List files in the folder
        folder_name = "coxau/"
        files = list_files(ftp_conn, folder_name)
        print("Files in the folder:", files)

        # def file_filter(file):
            # modification_time = get_file_modification_time(ftp_conn, file)
            # print(f"File '{file}' was last modified on {modification_time}")
            # return file.endswith(".csv")

        for file in files:
            modification_time = get_file_modification_time(ftp_conn, file)
            print(f"File '{file}' was last modified on {modification_time}")

        ftp_conn.quit()

    except Exception as e:
        logger.error(f"Failed to get FTP secrets: {e}")
        raise


if __name__ == "__main__":
    lambda_handler({}, {})
