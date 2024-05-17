"""Get historical data from FTP."""
import boto3
import logging
from os import environ
from json import loads
from datetime import date, datetime, timezone, timedelta
from ftp_wrapper import FtpToS3

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

ENVIRONMENT = environ.get("ENVIRONMENT", "stage")
INTEGRATIONS_BUCKET = f"integrations-us-east-1-{'prod' if ENVIRONMENT == 'prod' else 'test'}"
SNS_TOPIC_ARN = environ.get("CE_TOPIC")
SNS_CLIENT = boto3.client('sns')


def get_ftp_credentials():
    """Get FTP credentials from Secrets Manager."""
    secret_id = f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/TekionFTP"
    try:
        secret = boto3.client("secretsmanager").get_secret_value(SecretId=secret_id)
        return loads(secret["SecretString"])
    except boto3.exceptions.Boto3Error as e:
        logger.error(f"Failed to retrieve secret {secret_id}: {e}")
        raise


def parse_data(data):
    """Parse and handle SQS Message."""
    logger.info(data)
    dealer_id = data["dealer_id"]
    ftp_session = FtpToS3(**get_ftp_credentials())
    ftp = ftp_session.connect_to_ftp()
    if ftp:
        if ftp_session.check_folder_exists(ftp, dealer_id):
            # ftp_session.transfer_file_from_ftp_to_s3(dealer_id)
            logger.info(f"Dealer {dealer_id} folder found in FTP server.")
        else:
            logger.info(f"Dealer {dealer_id} folder not found in FTP server.")


def lambda_handler(event, context):
    try:
        for event in [e for e in event["Records"]]:
            parse_data(loads(event["body"]))
    except Exception:
        logger.exception("Error running historical data lambda")
        raise


# Connect to the FTP server.
# Check if the dealer folder exists.
# Check for the presence of files.
# Convert CSV to JSON.
# Save the JSON files to an S3 bucket.