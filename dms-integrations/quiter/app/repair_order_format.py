"""Logic for formatting files for “Repair Orders,” “Customers,” and “Vehicles” from Quiter."""
import os
import chardet
from os import environ
import logging
import boto3
import json
import pandas as pd
from io import StringIO, BytesIO
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3")
sns_client = boto3.client("sns")
lambda_client = boto3.client("lambda")

INTEGRATIONS_BUCKET = environ.get("INTEGRATIONS_BUCKET")
TOPIC_ARN = os.getenv("CLIENT_ENGINEERING_SNS_TOPIC_ARN")
UNIFIED_RO_INSERT_LAMBDA = os.getenv("UNIFIED_RO_INSERT_LAMBDA")


def detect_file_encoding(file_content):
    """Detect the encoding of a file."""
    result = chardet.detect(file_content)
    encoding = result['encoding']
    logger.info(f"Detected file encoding: {encoding}")
    return encoding

def load_dataframe_from_s3(bucket_name, s3_key):
    """Load the file from S3 and convert it into a pandas DataFrame with the correct encoding."""
    try:
        logger.info(f"Attempting to load file from S3: Bucket={bucket_name}, Key={s3_key}")
        
        response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
        file_content = response["Body"].read()
    
        encoding = detect_file_encoding(file_content)
        file_content_decoded = file_content.decode(encoding)
        df = pd.read_csv(StringIO(file_content_decoded))
        
        logger.info(f"File successfully loaded from S3: {s3_key}")
        return df
    
    except ClientError as e:
        logger.error(f"Error loading the file from S3: Bucket={bucket_name}, Key={s3_key}, Error={e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise


def transform_to_unified_format(df):
    """Transforms the raw DataFrame into a unified format according to the provided specifications."""
    logger.info("Starting transformation of the raw data to unified format.")
    try:
        df_unified = pd.DataFrame()

        df_unified["id"] = range(1, len(df) + 1)
        df_unified["vehicle_id"] = df.get("vehicle_id", pd.NA)
        df_unified["dealer_integration_partner_id"] = df.get(
            "dealer_integration_partner_id", pd.NA
        )
        df_unified["db_creation_date"] = pd.Timestamp.now()

        logger.debug(f"Transformed DataFrame: {df_unified.head()}")  # Log only the first few rows for debugging
        return df_unified
    except Exception as e:
        logger.error(f"Error during data transformation: {e}")
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

            df_unified = transform_to_unified_format(df_raw)
            unified_key = f"unified/repair_order/quiter/{s3_key.split('/')[-1]}"
            logger.debug(f"Generated unified S3 key: {unified_key}")

            upload_to_unified_s3(df_unified, INTEGRATIONS_BUCKET, unified_key)

            # Send SNS notification after successful upload
            sns_message = f"File successfully processed and uploaded to {INTEGRATIONS_BUCKET}/{unified_key}"
            send_sns_notification(sns_message, TOPIC_ARN)

    except Exception as e:
        logger.error(f"Error during file processing: {e}")
        raise
