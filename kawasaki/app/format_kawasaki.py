"""Format Kawasaki data to CSV and upload to ICC."""
import io
import logging
from datetime import datetime, timezone
from ftplib import FTP
from json import loads
from os import environ

import boto3

from utils.xml_to_csv import convert_xml_to_csv

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
    csv_file_name = file_name.replace(".xml", ".csv")
    web_provider = key.split("/")[1]
    xml_data = parse_s3_file(bucket, key)
    csv_data = convert_xml_to_csv(xml_data, web_provider)
    ftp_credentials = get_ftp_credentials()
    upload_to_ftp(
        ftp_credentials["host"],
        ftp_credentials["username"],
        ftp_credentials["password"],
        csv_file_name,
        csv_data,
    )
    logger.info(f"Uploaded {csv_file_name} to {ftp_credentials['host']}")
    now = datetime.utcnow().replace(microsecond=0).replace(tzinfo=timezone.utc)
    s3_key = (
        f"formatted/{web_provider}/{now.year}/{now.month}/{now.day}/{csv_file_name}"
    )
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
