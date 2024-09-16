"""Merging and transformation logic for “Vehicle Sales,” “Customers” and “Vehicles” from Quiter."""

import boto3
import pandas as pd
import os
import logging

from json import loads
from datetime import datetime
from botocore.exceptions import ClientError
import io
import uuid

from utils import list_files_in_s3, find_matching_files, merge_files, extract_date_from_key, detect_encoding, clean_data, identify_and_separate_records,save_to_s3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3")

BUCKET_NAME = os.environ["INTEGRATIONS_BUCKET"]
TOPIC_ARN = os.environ["CLIENT_ENGINEERING_SNS_TOPIC_ARN"]

FILE_PATTERNS = {
    "Consumer": ["CONS"],
    "Vehicle": ["VEH"],
    "VehicleSales": ["VS", "SalesTxn", "SaleTxn"]
}


def lambda_handler(event, context):
    try:
        for record in event["Records"]:
            message = loads(record["body"])
            logger.info(f"event Body: {message}")

            dealer_id = message["dealer_id"]
            s3_key = message["s3_key"]

            year, month, day = extract_date_from_key(s3_key)
            s3_prefix = f"quiter/landing_zone/{dealer_id}/{year}/{month}/{day}/"
            s3_files = list_files_in_s3(s3_prefix)

            # Find matching files for Consumer, Vehicle, and VehicleSales
            found_files = find_matching_files(s3_files)

            # Ensure we have all required files: Consumer, Vehicle, and VehicleSales
            if "Consumer" not in found_files or "Vehicle" not in found_files or "VehicleSales" not in found_files:
                raise ValueError(f"Missing required files in S3 for dealer {dealer_id}. Found: {found_files}")

            # Get the S3 objects for the three required files
            consumers_obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=found_files["Consumer"])
            vehicles_obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=found_files["Vehicle"])
            vehicle_sales_obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=found_files["VehicleSales"])


            consumers_body = consumers_obj['Body'].read()
            vehicles_body = vehicles_obj['Body'].read()
            vehicle_sales_body = vehicle_sales_obj['Body'].read()

            # Detect encoding for each file
            consumer_encoding = detect_encoding(consumers_body)
            vehicle_encoding = detect_encoding(vehicles_body)
            vehicle_sales_encoding = detect_encoding(vehicle_sales_body)

            # Try reading the CSV with error handling for bad lines
            customers_df = pd.read_csv(io.BytesIO(consumers_body), delimiter=';', encoding=consumer_encoding, on_bad_lines='warn')
            vehicles_df = pd.read_csv(io.BytesIO(vehicles_body), delimiter=';', encoding=vehicle_encoding, on_bad_lines='warn')
            vehicle_sales_df = pd.read_csv(io.BytesIO(vehicle_sales_body), delimiter=';', encoding=vehicle_sales_encoding, on_bad_lines='warn', dtype={'Dealer ID': 'string'})

            # Clean the customer and vehicle data using the unified function
            cleaned_customers_df = clean_data(customers_df, 'Dealer Customer No', [])
            cleaned_vehicles_df = clean_data(vehicles_df, 'Vin No', ['OEM Name', 'Model'])

            # Identify missing records in vehicle_sales_df compared to customers_df and vehicles_df
            valid_records_df, orphans_df = identify_and_separate_records(vehicle_sales_df, cleaned_customers_df, cleaned_vehicles_df)

            # Save the orphan records to an error file
            if not orphans_df.empty:
                unique_id = str(uuid.uuid4())
                error_file_key = f"quiter/error_files/vehicle_sale/{dealer_id}/{year}/{month}/{day}/{unique_id}_orphan_records.csv"
                save_to_s3(orphans_df, BUCKET_NAME, error_file_key)

                # Send notification about the error file
                notify_client_engineering(f"Orphan records found. Error file saved at {error_file_key}")

            # Merge the data
            merged_df = merge_files(valid_records_df, cleaned_customers_df, cleaned_vehicles_df)
            
            # Save merged data back to S3
            csv_buffer = merged_df.to_csv(index=False)
            unique_id = str(uuid.uuid4())
            merged_s3_key = f"quiter/fi_closed_deal/{year}/{month}/{day}/{unique_id}.csv"
            s3_client.put_object(Bucket=BUCKET_NAME, Key=merged_s3_key, Body=csv_buffer)
            logger.info(f"Merged Vehicle Sales file saved to {merged_s3_key} in S3.")

    except ValueError as e:
        logger.error(f"Data mismatch error: {e}")
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


def notify_client_engineering(error_message):
    """Send a notification to the client engineering SNS topic."""
    sns_client = boto3.client("sns")

    sns_client.publish(
        TopicArn=TOPIC_ARN,
        Subject="QuiterMergeVehicleSales Lambda Error",
        Message=str(error_message),
    )
