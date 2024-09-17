"""Logic for formatting files for “Repair Orders,” “Customers,” and “Vehicles” from Quiter."""

import os
from os import environ
import logging
import boto3
import json
import pandas as pd
from io import StringIO
from botocore.exceptions import ClientError
from unified_data import upload_unified_json
from repair_order_merge import detect_file_encoding

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3")
sns_client = boto3.client("sns")
lambda_client = boto3.client("lambda")

INTEGRATIONS_BUCKET = environ.get("INTEGRATIONS_BUCKET")
TOPIC_ARN = os.getenv("CLIENT_ENGINEERING_SNS_TOPIC_ARN")
UNIFIED_RO_INSERT_LAMBDA = os.getenv("UNIFIED_RO_INSERT_LAMBDA")


def load_dataframe_from_s3(bucket_name, s3_key):
    """Load the file from S3 and convert it into a pandas DataFrame with the correct encoding."""
    try:
        logger.info(
            f"Attempting to load file from S3: Bucket={bucket_name}, Key={s3_key}"
        )

        response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
        file_content = response["Body"].read()

        encoding = detect_file_encoding(file_content)
        file_content_decoded = file_content.decode(encoding)
        df = pd.read_csv(StringIO(file_content_decoded))

        logger.info(f"File successfully loaded from S3: {s3_key}")
        return df

    except ClientError as e:
        logger.error(
            f"Error loading the file from S3: Bucket={bucket_name}, Key={s3_key}, Error={e}"
        )
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise


def send_sns_notification(message, topic_arn):
    """Sends a notification to the SNS topic."""
    try:
        logger.info(f"Sending SNS notification to topic: {topic_arn}")
        sns_client.publish(
            TopicArn=topic_arn,
            Message=message,
            Subject="Unified Repair Order Processing",
        )
        logger.info("SNS notification sent successfully")
    except ClientError as e:
        logger.error(f"Error sending SNS notification: Topic={topic_arn}, Error={e}")
        raise


def lambda_handler(event, context):
    """Main function triggered by SQS events to process and transform the raw data."""
    logger.info("Lambda handler triggered")

    try:
        for record in event["Records"]:
            logger.debug(f"Processing SQS record: {record}")

            # Extract S3 event info from the message
            s3_event = json.loads(record["body"])
            s3_bucket = s3_event["Records"][0]["s3"]["bucket"]["name"]
            s3_key = s3_event["Records"][0]["s3"]["object"]["key"]

            logger.info(f"Processing file from S3: {s3_bucket}/{s3_key}")

            # Load the raw data from S3
            df_raw = load_dataframe_from_s3(s3_bucket, s3_key)

            if df_raw.empty:
                logger.warning(f"DataFrame is empty for file: {s3_bucket}/{s3_key}")

            # Set integration type and source S3 URI
            integration_type = "repair_order"
            source_s3_uri = s3_key

            # Upload the unified JSON
            unified_key = upload_unified_json(df_raw, integration_type, source_s3_uri)
            logger.debug(f"Generated unified S3 key: {unified_key}")

            # Send SNS notification after successful upload
            if unified_key:
                sns_message = f"File successfully processed and uploaded to {INTEGRATIONS_BUCKET}/{unified_key}"
                send_sns_notification(sns_message, TOPIC_ARN)
            else:
                sns_message = f"No data uploaded for file: {s3_bucket}/{s3_key}"
                logger.warning(sns_message)
                send_sns_notification(sns_message, TOPIC_ARN)

    except Exception as e:
        logger.error(f"Error during file processing: {e}")
        raise
