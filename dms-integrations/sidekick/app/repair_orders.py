import boto3
import logging
from os import environ
from json import loads
from datetime import date, datetime, timezone, timedelta
from ftp_to_s3_extraction import FtpToS3

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

ENVIRONMENT = environ.get("ENVIRONMENT", "stage")
INTEGRATIONS_BUCKET = f"integrations-us-east-1-{'prod' if ENVIRONMENT == 'prod' else 'test'}"
SNS_TOPIC_ARN = environ.get("CE_TOPIC")
SNS_CLIENT = boto3.client('sns')


def get_ftp_credentials():
    """Get FTP credentials from Secrets Manager with error handling."""
    secret_id = f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/SidekickFTP"
    try:
        secret = boto3.client("secretsmanager").get_secret_value(SecretId=secret_id)
        return loads(secret["SecretString"])
    except boto3.exceptions.Boto3Error as e:
        logger.error(f"Failed to retrieve secret {secret_id}: {e}")
        raise

def parse_date(date_str):
    """Parse date string or get current UTC datetime if None."""
    return datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S") if date_str else datetime.now(timezone.utc)


def alert_topic(dealer_id, daily_date_path, historical_date_path):
    """Notify Topic of missing S3 files."""
    message = f'SIDEKICK: No {dealer_id} files uploaded for {date_path} or there is an issue with the historical file {historical_date_path}.'

    response = SNS_CLIENT.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=message,
            Subject=f"SIDEKICK: Missing files on FTP for {dealer_id}."
        )

    return response


def parse_data(sqs_message_data):
    """Parse and handle SQS message."""
    try:
        logger.info("Processing SQS message: %s", sqs_message_data)
        secrets = get_ftp_credentials()
        ftp_session = FtpToS3(**secrets)

        dealer_id = sqs_message_data["dealer_id"]
        parent_store, child_store = dealer_id.split("-")

        end_dt = parse_date(sqs_message_data.get("end_dt_str"))
        daily_date_path = 'daily/' + end_dt.strftime("%Y/%m/%d")
        historical_date_path = 'historical/' + end_dt.strftime("%Y/%m/%d")

        filename = f"Sidekick-{parent_store}-{end_dt.strftime('%Y%m%d')}.csv"
        historical_filename = f"Sidekick-{parent_store}-{end_dt.strftime('%Y%m%d')}-History.csv"

        # check daily file
        ftp_session.transfer_file_from_ftp_to_s3(
            filename=filename,
            bucket_name=INTEGRATIONS_BUCKET,
            date_path=daily_date_path,
            parent_store=parent_store,
            child_store=child_store,
        )

        logger.info(f"Successfully processed {filename}")

        # check historical file if exists
        ftp_session.transfer_file_from_ftp_to_s3(
            filename=historical_filename,
            bucket_name=INTEGRATIONS_BUCKET,
            date_path=historical_date_path,
            parent_store=parent_store,
            child_store=child_store,
        )

        logger.info(f"Successfully processed {historical_filename}")

    except Exception as e:
        logger.error(f"Error processing SQS message: {e}")
        alert_topic(dealer_id, daily_date_path, historical_date_path)


def lambda_handler(event, context):
    """Query Sidekick repair order."""
    try:
        for record in event["Records"]:
            parse_data(loads(record["body"]))
    except Exception as e:
        logger.error("Error running repair order lambda: %s", e)
        raise

