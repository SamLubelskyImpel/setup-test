"""Merging logic for “Repair Orders”, “Customers” and “Vehicles” from Quiter."""

import os
from os import environ
import boto3
import logging
import pandas as pd
import uuid
import gzip
import io
from json import loads
from datetime import datetime
from botocore.exceptions import ClientError

# Set up logging
logger = logging.getLogger()
logger.setLevel(os.getenv("LOGLEVEL", "INFO").upper())

# AWS clients
s3_client = boto3.client("s3")
sns_client = boto3.client("sns")

# Environment variables
INTEGRATIONS_BUCKET = environ.get("INTEGRATIONS_BUCKET")
TOPIC_ARN = os.getenv("CLIENT_ENGINEERING_SNS_TOPIC_ARN")

SQS_QUEUE_URLS = [
    environ.get("MERGE_REPAIR_ORDER_QUEUE"),
    environ.get("MERGE_APPOINTMENT_QUEUE"),
    environ.get("MERGE_VEHICLE_SALE_QUEUE")
]

FILE_PATTERNS = {
    "RepairOrder": ["RO"],
    "Consumer": ["CONS"],
    "Vehicle": ["VEH"],
    "Appointment": ["APPT"],
    "VehicleSales": ["VS", "SalesTxn", "SaleTxn"]
}

def merge_files(repair_orders_df, customers_df, vehicles_df):
    """Merges the dataframes for repair orders, customers, and vehicles using customer_id and vin."""
    logger.info("Merging Repair Orders, Customers, and Vehicles data.")
    merged_df = pd.merge(repair_orders_df, customers_df, on="customer_id", how="inner")
    merged_df = pd.merge(merged_df, vehicles_df, on="vin", how="inner")
    logger.debug(f"Merged DataFrame head: {merged_df.head()}")  # Log the DataFrame head for debugging
    return merged_df

def load_dataframe_from_s3(file_key):
    """Loads and returns a dataframe from a gzipped CSV file in S3."""
    try:
        logger.info(f"Loading file from S3: {file_key}")
        obj = s3_client.get_object(Bucket=INTEGRATIONS_BUCKET, Key=file_key)
        with gzip.GzipFile(fileobj=io.BytesIO(obj["Body"].read())) as gz:
            df = pd.read_csv(gz)
        logger.info(f"File successfully loaded from S3: {file_key}")
        return df
    except ClientError as e:
        logger.error(f"Error loading the file from S3: {file_key}, Error: {e}")
        raise

def notify_client_engineering(error_message):
    """Sends a notification to the client engineering SNS topic."""
    try:
        logger.info("Sending error notification to SNS topic.")
        sns_client.publish(
            TopicArn=TOPIC_ARN,
            Subject="QuiterMergeRepairOrder Lambda Error",
            Message=str(error_message),
        )
        logger.info("SNS notification sent successfully.")
    except ClientError as e:
        logger.error(f"Error sending SNS notification: {e}")
        raise

def lambda_handler(event, context):
    """Main function handling the merging of files and exception management."""
    logger.info("Lambda function triggered")
    
    try:
        for record in event["Records"]:
            logger.debug(f"Processing SQS record: {record}")

            data = loads(record["body"])
            dealer_id = data["dealer_id"]
            logger.info(f"Processing dealer: {dealer_id}")
            current_date = datetime.now()

            # Build the S3 path
            base_path = f"quiter/landing_zone/{dealer_id}/{current_date.year}/{current_date.month}/{current_date.day}"
            logger.info(f"Fetching files from S3 with prefix: {base_path}")
            response = s3_client.list_objects_v2(Bucket=INTEGRATIONS_BUCKET, Prefix=base_path)
            files = response.get("Contents", [])

            if not files:
                logger.warning(f"No files found in the S3 bucket with prefix: {base_path}")
                raise ValueError(f"No files found in the S3 bucket with prefix {base_path}")

            # Initialize DataFrames
            repair_orders_df = None
            customers_df = None
            vehicles_df = None

            # Search for files matching patterns
            for file in files:
                file_name = file["Key"].split("/")[-1]
                logger.debug(f"Checking file: {file_name}")

                if any(pattern in file_name for pattern in FILE_PATTERNS["RepairOrder"]) and file_name.endswith(".csv"):
                    logger.info(f"Found Repair Order file: {file_name}")
                    repair_orders_df = load_dataframe_from_s3(file["Key"])
                elif any(pattern in file_name for pattern in FILE_PATTERNS["Consumer"]) and file_name.endswith(".csv"):
                    logger.info(f"Found Consumer file: {file_name}")
                    customers_df = load_dataframe_from_s3(file["Key"])
                elif any(pattern in file_name for pattern in FILE_PATTERNS["Vehicle"]) and file_name.endswith(".csv"):
                    logger.info(f"Found Vehicle file: {file_name}")
                    vehicles_df = load_dataframe_from_s3(file["Key"])

            # Validate that all required files are loaded
            if not all([repair_orders_df, customers_df, vehicles_df]):
                logger.error("Missing one or more required data files.")
                raise ValueError("Missing one or more required data files.")

            # Validate the consistency of data across the files
            if len(repair_orders_df) != len(customers_df) or len(customers_df) != len(vehicles_df):
                logger.error("Mismatch in the number of customers, vehicles, or repair orders.")
                raise ValueError("Mismatch in the number of customers, vehicles, or repair orders.")

            # Merge the files
            merged_df = merge_files(repair_orders_df, customers_df, vehicles_df)

            # Upload the merged file to S3
            csv_buffer = merged_df.to_csv(index=False)
            unique_id = str(uuid.uuid4())
            s3_key = f"quiter/repair_order/{current_date.year}/{current_date.month}/{current_date.day}/{unique_id}.csv"
            logger.info(f"Uploading merged file to S3: {s3_key}")
            s3_client.put_object(Bucket=INTEGRATIONS_BUCKET, Key=s3_key, Body=csv_buffer)
            logger.info(f"Merged file saved to {s3_key}.")

    except ValueError as e:
        logger.error(f"Data consistency error: {e}")
        notify_client_engineering(e)
        raise

    except ClientError as e:
        logger.error(f"AWS S3 error: {e}")
        notify_client_engineering(e)
        raise

    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        notify_client_engineering(e)
        raise
