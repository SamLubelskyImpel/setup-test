"""Update file in code. AWS_PROFILE=unified-test python ./upload_file_ftp.py"""

import logging
from os import environ, path
from ftplib import FTP
import json
import boto3
from typing import Any

environ["ENVIRONMENT"] = "test"
SECRET_KEY = "COXAU_SFTP"

ENVIRONMENT = environ["ENVIRONMENT"]

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
sm_client = boto3.client("secretsmanager")


def connect_ftp_server(hostname, username, password):
    """Connect to FTP server and return the connection."""
    ftp = FTP(hostname)
    ftp.login(username, password)
    return ftp


def get_ftp_secrets(secret_name: Any, secret_key: Any) -> Any:
    """Get FTP secret from Secrets Manager."""
    secret = sm_client.get_secret_value(
        SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/{secret_name}"
    )
    secret = json.loads(secret["SecretString"])[str(secret_key)]
    secret_data = json.loads(secret)

    return secret_data["hostname"], secret_data["username"], secret_data["password"]


def upload_file(ftp, local_file_path, remote_folder):
    # Change to the remote folder on the FTP server
    ftp.cwd(remote_folder)

    # Open the local file in binary mode for uploading
    with open(local_file_path, 'rb') as local_file:
        # Use STOR command to upload the file
        ftp.storbinary('STOR ' + path.basename(local_file_path), local_file)

    print(f"File '{path.basename(local_file_path)}' uploaded successfully.")


if __name__ == "__main__":
    # upload file
    hostname, username, password = get_ftp_secrets("inventory-integrations-sftp", SECRET_KEY)
    ftp_conn = connect_ftp_server(hostname, username, password)

    # List files in the folder
    local_file_path = "test_files/13728_Inventory_1721163724.csv"
    # local_file_path = "test_files/13728_vdp.csv"
    folder_name = "coxau/"
    upload_file(ftp_conn, local_file_path, folder_name)

    ftp_conn.quit()
