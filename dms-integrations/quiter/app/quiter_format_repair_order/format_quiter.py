"""Logic for formatting files for “Repair Orders,” “Customers,” and “Vehicles” from Quiter."""

import os
import logging
import boto3
import json
import pandas as pd
from io import StringIO
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3")
sns_client = boto3.client("sns")
lambda_client = boto3.client("lambda")

RAW_BUCKET_NAME = os.getenv("RAW_BUCKET_NAME")
UNIFIED_BUCKET_NAME = os.getenv("UNIFIED_BUCKET_NAME")
TOPIC_ARN = os.getenv("CLIENT_ENGINEERING_SNS_TOPIC_ARN")
UNIFIED_RO_INSERT_LAMBDA = os.getenv("UNIFIED_RO_INSERT_LAMBDA")


def load_dataframe_from_s3(bucket_name, s3_key):
    """Load the file from S3 and convert it into a pandas DataFrame."""
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
        file_content = response["Body"].read().decode("utf-8")
        df = pd.read_csv(StringIO(file_content))
        return df
    except ClientError as e:
        logger.error(f"Error loading the file from S3: {e}")
        raise


def transform_to_unified_format(df):
    """Transforms the raw DataFrame into a unified format according to the provided specifications."""

    df_unified = pd.DataFrame()

    df_unified["id"] = range(1, len(df) + 1)
    df_unified["vehicle_id"] = df.get("vehicle_id", pd.NA)
    df_unified["dealer_integration_partner_id"] = df.get(
        "dealer_integration_partner_id", pd.NA
    )

    df_unified["db_creation_date"] = pd.Timestamp.now()

    return df_unified


def upload_to_unified_s3(df, unified_bucket, s3_key):
    """Upload the converted file to the unified bucket."""
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    try:
        s3_client.put_object(
            Bucket=unified_bucket, Key=s3_key, Body=csv_buffer.getvalue()
        )
        logger.info(f"File successfully uploaded to {unified_bucket}/{s3_key}")
    except ClientError as e:
        logger.error(f"Error uploading the file to the unified bucket: {e}")
        raise


def send_sns_notification(message, topic_arn):
    """Sends a notification to the SNS topic."""
    try:
        sns_client.publish(
            TopicArn=topic_arn,
            Message=message,
            Subject="Unified Repair Order Processing",
        )
        logger.info("SNS notification sent successfully")
    except ClientError as e:
        logger.error(f"Error sending SNS notification: {e}")
        raise


def lambda_handler(event, context):
    """Main function triggered by SQS events to process and transform the raw data."""
    try:
        for record in event["Records"]:
            s3_event = json.loads(record["body"])
            s3_bucket = s3_event["Records"][0]["s3"]["bucket"]["name"]
            s3_key = s3_event["Records"][0]["s3"]["object"]["key"]

            logger.info(f"Processing file from S3: {s3_bucket}/{s3_key}")
            df_raw = load_dataframe_from_s3(s3_bucket, s3_key)
            df_unified = transform_to_unified_format(df_raw)

            unified_key = f"unified/repair_order/quiter/{s3_key.split('/')[-1]}"
            upload_to_unified_s3(UNIFIED_BUCKET_NAME, unified_key, df_unified)

            # Send SNS notification after successful upload
            sns_message = f"File successfully processed and uploaded to {UNIFIED_BUCKET_NAME}/{unified_key}"
            send_sns_notification(sns_message, TOPIC_ARN)

    except Exception as e:
        logger.error(f"Error during file processing: {e}")
        raise
