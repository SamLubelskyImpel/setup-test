from ftplib import FTP
from json import loads
from os import environ

import boto3

FTP_SECRETS_NAME = environ.get("FTP_SECRETS_NAME", "test/KawasakiFTP")
S3_CLIENT = boto3.client("s3")
SECRETS_CLIENT = boto3.client("secretsmanager")


def get_ftp_credentials():
    """Get FTP credentials from secretsmanager."""
    return loads(
        SECRETS_CLIENT.get_secret_value(SecretId=FTP_SECRETS_NAME)["SecretString"]
    )


def list_files_with_prefix(host, username, password, remote_dir, prefix):
    """List files on an ftp server with a given prefix"""
    ftp = FTP(host)
    ftp.login(username, password)
    ftp.cwd(remote_dir)
    file_list = ftp.nlst()
    matching_files = [file for file in file_list if file.startswith(prefix)]
    ftp.quit()
    return matching_files


ftp_credentials = get_ftp_credentials()
for prefix in ["dealerspike"]:
    files = list_files_with_prefix(
        ftp_credentials["host"],
        ftp_credentials["username"],
        ftp_credentials["password"],
        "/",
        prefix,
    )
    print(files)
