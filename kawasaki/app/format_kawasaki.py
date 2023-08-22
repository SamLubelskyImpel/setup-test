"""Format Kawasaki data to CSV and upload to ICC."""
import io
import logging
from datetime import datetime, timezone
from ftplib import FTP
from json import loads
from os import environ
from xml.etree import ElementTree

import boto3
from ari import convert_ari_csv
from dealerspike import convert_dealerspike_csv

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
FTP_SECRETS_NAME = environ.get("FTP_SECRETS_NAME", "test/KawasakiFTP")
S3_CLIENT = boto3.client("s3")
SECRETS_CLIENT = boto3.client("secretsmanager")


def get_ftp_credentials():
    """Get FTP credentials from secretsmanager."""
    return loads(
        SECRETS_CLIENT.get_secret_value(SecretId=FTP_SECRETS_NAME)["SecretString"]
    )


def upload_to_ftp(host, username, password, remote_file_path, file_content):
    """Upload file content to provided FTP server"""
    ftp = FTP(host)
    ftp.login(username, password)
    ftp.cwd("/")
    file_buffer = io.BytesIO(file_content.encode())
    ftp.storbinary("STOR " + remote_file_path, file_buffer)
    ftp.quit()


def parse_s3_file(bucket_name, s3_key):
    """Parse an XML file from S3."""
    response = S3_CLIENT.get_object(Bucket=bucket_name, Key=s3_key)
    xml_content = response["Body"].read()
    return xml_content


def format_upload_kawasaki(bucket, key):
    """Convert kawasaki XML data to CSV and upload to the FTP server."""
    file_name = key.split("/")[-1]
    web_provider = key.split("/")[1]
    xml_data = parse_s3_file(bucket, key)
    if web_provider == "dealerspike":
        csv_data = convert_dealerspike_csv(xml_data)
    elif web_provider == "ari":
        csv_data = convert_ari_csv(xml_data)
    else:
        raise RuntimeError(f"Unsupported web provider: {web_provider}")
    ftp_credentials = get_ftp_credentials()
    upload_to_ftp(
        ftp_credentials["host"],
        ftp_credentials["username"],
        ftp_credentials["password"],
        file_name,
        csv_data,
    )
    logger.info(f"Uploaded {file_name} to {ftp_credentials['host']}")
    now = datetime.utcnow().replace(microsecond=0).replace(tzinfo=timezone.utc)
    s3_key = f"formatted/{web_provider}/{now.year}/{now.month}/{now.day}/{file_name}"
    S3_CLIENT.put_object(Body=csv_data, Bucket=bucket, Key=s3_key)
    logger.info(f"Uploaded {s3_key}")


def lambda_handler(event: dict, context: dict):
    """Format Kawasaki data to CSV and upload to ICC."""
    try:
        for event in [e for e in event["Records"]]:
            message = loads(event["body"])
            logger.info(f"Message of {message}")
            for record in message["Records"]:
                bucket = record["s3"]["bucket"]["name"]
                key = record["s3"]["object"]["key"]
                format_upload_kawasaki(bucket, key)
    except Exception as e:
        logger.exception("Error formatting Kawasaki data.")
        raise e
