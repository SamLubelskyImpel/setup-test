import boto3
import logging
from os import environ
from json import loads
from datetime import datetime, timezone, timedelta
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


def alert_topic(dealer_id):
    """Notify Topic of missing S3 files."""
    message = f'SIDEKICK: No {dealer_id} daily files uploaded or there is an issue with the historical file.'

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
        previous_dt = end_dt - timedelta(days=1)

        daily_date_path = 'daily/' + end_dt.strftime("%Y/%m/%d")
        previous_daily_date_path = 'daily/' + previous_dt.strftime("%Y/%m/%d")

        historical_date_path = 'historical/' + end_dt.strftime("%Y/%m/%d")
        previous_historical_date_path = 'historical/' + previous_dt.strftime("%Y/%m/%d")

        logger.info(f"Checking for files in {daily_date_path}, {previous_daily_date_path}, {historical_date_path}, {previous_historical_date_path}.")

        # check daily file
        ftp_session.transfer_file_from_ftp_to_s3(
            bucket_name=INTEGRATIONS_BUCKET,
            date_paths=[daily_date_path, previous_daily_date_path],
            parent_store=parent_store,
            child_store=child_store,
        )

        logger.info(f"Successfully processed a daily file in {daily_date_path} or {previous_daily_date_path}.")

        # check historical file if exists
        ftp_session.transfer_file_from_ftp_to_s3(
            bucket_name=INTEGRATIONS_BUCKET,
            date_paths=[historical_date_path, previous_historical_date_path],
            parent_store=parent_store,
            child_store=child_store,
        )

        logger.info(f"Successfully processed a historical file in {historical_date_path} or {previous_historical_date_path} if it exists.")

    except Exception as e:
        logger.error(f"Error processing SQS message: {e}")
        alert_topic(dealer_id)


def lambda_handler(event, context):
    """Query Sidekick repair order."""
    try:
        for record in event["Records"]:
            parse_data(loads(record["body"]))
    except Exception as e:
        logger.error("Error running repair order lambda: %s", e)
        raise
