import os
import boto3
import logging
from json import loads
from datetime import datetime, timezone
from ftp_to_s3_extraction import FtpToS3

def setup_logging():
    log_level = os.environ.get("LOGLEVEL", "INFO").upper()
    logging.basicConfig(level=log_level)
    logger = logging.getLogger(__name__)
    return logger

logger = setup_logging()

AWS_PROFILE = os.environ["AWS_PROFILE"]
ENVIRONMENT = os.environ.get("ENVIRONMENT", "stage")
INTEGRATIONS_BUCKET = f"integrations-us-east-1-{'prod' if ENVIRONMENT == 'prod' else 'test'}"

def get_ftp_credentials():
    """Get FTP credentials from Secrets Manager with error handling."""
    secret_id = f"{'prod' if AWS_PROFILE == 'unified-prod' else 'test'}/TekionFTP"
    try:
        secret = boto3.client("secretsmanager").get_secret_value(SecretId=secret_id)
        return loads(secret["SecretString"])
    except boto3.exceptions.Boto3Error as e:
        logger.error(f"Failed to retrieve secret {secret_id}: {e}")
        raise

def parse_date(date_str):
    """Parse date string or get current UTC datetime if None."""
    return datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S") if date_str else datetime.now(timezone.utc)

def parse_data(sqs_message_data):
    """Parse and handle SQS message."""
    logger.info("Processing SQS message: %s", sqs_message_data)
    secrets = get_ftp_credentials()
    ftp_session = FtpToS3(aws_profile=AWS_PROFILE, **secrets)

    dealer_id = sqs_message_data["dealer_id"].split("-")[0]
    end_dt = parse_date(sqs_message_data.get("end_dt_str"))
    date_path = end_dt.strftime("%Y/%m/%d")
    
    filename = f"Sidekick-{dealer_id}-{end_dt.strftime('%Y-%m-%d')}.csv"
    local_file_path = f"file-id-{dealer_id}.csv"
    remote_directory = f"{dealer_id}/{date_path}"

    ftp_session.transfer_csv_from_ftp_to_s3(
        filename=filename,
        local_file_path=local_file_path,
        bucket_name=INTEGRATIONS_BUCKET,
        remote_directory=remote_directory,
        dealer_id=dealer_id,
    )

def lambda_handler(event, context):
    """Query Sidekick repair order."""
    try:
        for record in event["Records"]:
            parse_data(loads(record["body"]))
    except Exception as exc:
        logger.exception("Error running repair order lambda: %s", exc)
        raise

