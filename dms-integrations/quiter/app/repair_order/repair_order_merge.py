"""Merging logic for “Repair Orders”, “Customers” and “Vehicles” from Quiter."""

import os
from os import environ
import boto3
import logging
import uuid
from json import loads
from datetime import datetime
from botocore.exceptions import ClientError


from utils import (
    identify_and_separate_records,
    read_csv_from_s3,
    find_matching_files,
    save_error_file,
    extract_date_from_key,
    notify_client_engineering,
    merge_files,
    remove_duplicates,
)

# Set up logging
logger = logging.getLogger()
logger.setLevel(os.getenv("LOGLEVEL", "INFO").upper())

# AWS clients
s3_client = boto3.client("s3")
sns_client = boto3.client("sns")

# Environment variables
INTEGRATIONS_BUCKET = environ.get("INTEGRATIONS_BUCKET")
TOPIC_ARN = os.getenv("CLIENT_ENGINEERING_SNS_TOPIC_ARN")
SNS_CLIENT = boto3.client("sns")

SQS_QUEUE_URLS = [
    environ.get("MERGE_REPAIR_ORDER_QUEUE"),
    environ.get("MERGE_APPOINTMENT_QUEUE"),
    environ.get("MERGE_VEHICLE_SALE_QUEUE"),
]

FILE_PATTERNS = {
    "RepairOrder": ["RO"],
    "Consumer": ["CONS"],
    "Vehicle": ["VEH"],
    "Appointment": ["APPT"],
    "VehicleSales": ["VS", "SalesTxn", "SaleTxn"],
}


def lambda_handler(event, context):
    """
    Main function triggered by SQS events to process and transform the raw data.

    This function pulls data from S3, cleans it, merges it, and saves it back to S3.
    It also handles orphan records and sends error notifications.

    Args:
        event (dict): The event payload triggering the Lambda function.
        context (object): The context in which the Lambda is executed.
    """
    try:
        for record in event["Records"]:
            data = loads(record["body"])
            logger.info(f"Processing new event with dealer_id: {data['dealer_id']}")
            logger.debug(f"Full event body: {data}")

            dealer_id = data["dealer_id"]
            s3_key = data["s3_key"]
            current_date = datetime.now()

            logger.info(f"Extracting date from s3_key: {s3_key}")
            year, month, day = extract_date_from_key(s3_key)
            base_path = f"quiter/landing_zone/{dealer_id}/{year}/{month}/{day}/"

            logger.info(f"Listing objects in S3 bucket with prefix: {base_path}")
            response = s3_client.list_objects_v2(
                Bucket=INTEGRATIONS_BUCKET, Prefix=base_path
            )
            files = response.get("Contents", [])

            if not files:
                logger.error(
                    f"No files found in S3 bucket for dealer {dealer_id} with prefix {base_path}"
                )
                raise ValueError(
                    f"No files found in the S3 bucket with prefix {base_path}"
                )

            logger.debug(f"Files found in S3: {files}")
            found_files = find_matching_files(files)
            logger.info(f"Matching files found: {found_files}")

            if (
                "Consumer" not in found_files
                or "Vehicle" not in found_files
                or "RepairOrder" not in found_files
            ):
                logger.error(
                    f"Missing required files for dealer {dealer_id}. Found: {found_files}"
                )
                raise ValueError(
                    f"Missing required files in S3 for dealer {dealer_id}. Found: {found_files}"
                )

            try:
                logger.info(
                    f"Fetching RepairOrder file from S3: {found_files['RepairOrder']}"
                )
                repairorder_obj = s3_client.get_object(
                    Bucket=INTEGRATIONS_BUCKET, Key=found_files["RepairOrder"]
                )
                logger.info(
                    f"Fetching Consumer file from S3: {found_files['Consumer']}"
                )
                customers_obj = s3_client.get_object(
                    Bucket=INTEGRATIONS_BUCKET, Key=found_files["Consumer"]
                )
                logger.info(f"Fetching Vehicle file from S3: {found_files['Vehicle']}")
                vehicles_obj = s3_client.get_object(
                    Bucket=INTEGRATIONS_BUCKET, Key=found_files["Vehicle"]
                )

                logger.info(
                    f"Reading Consumer data from S3 object: {found_files['Consumer']}"
                )
                customers_df = read_csv_from_s3(
                    customers_obj["Body"].read(),
                    found_files["Consumer"],
                    "Consumer",
                    SNS_CLIENT,
                    TOPIC_ARN,
                )
                logger.debug(f"Consumer DataFrame shape: {customers_df.shape}")

                logger.info(
                    f"Reading Vehicle data from S3 object: {found_files['Vehicle']}"
                )
                vehicles_df = read_csv_from_s3(
                    vehicles_obj["Body"].read(),
                    found_files["Vehicle"],
                    "Vehicle",
                    SNS_CLIENT,
                    TOPIC_ARN,
                )
                logger.debug(f"Vehicle DataFrame shape: {vehicles_df.shape}")

                logger.info(
                    f"Reading RepairOrder data from S3 object: {found_files['RepairOrder']}"
                )
                repairorder_df = read_csv_from_s3(
                    repairorder_obj["Body"].read(),
                    found_files["RepairOrder"],
                    "RepairOrder",
                    SNS_CLIENT,
                    TOPIC_ARN,
                )
                logger.debug(f"RepairOrder DataFrame shape: {repairorder_df.shape}")

            except KeyError as e:
                logger.error(f"KeyError while fetching or reading files from S3: {e}")
                raise

            logger.info("Removing duplicates from Consumer and Vehicle dataframes")
            customers_df_clean = remove_duplicates(
                customers_df, "Dealer Customer No", []
            )
            vehicles_df_clean = remove_duplicates(
                vehicles_df, "Vin No", ["OEM Name", "Model"]
            )
            repairorder_df = repairorder_df.dropna(subset=["Consumer ID", "Vin No"])

            valid_records_df, orphans_df = identify_and_separate_records(
                repairorder_df, customers_df_clean, vehicles_df_clean
            )
            logger.info("Merging RepairOrder, Consumer, and Vehicle dataframes")
            merged_df = merge_files(
                main_df=valid_records_df,
                customers_df=customers_df_clean,
                vehicles_df=vehicles_df_clean,
                main_to_customers_keys=("Consumer ID", "Dealer Customer No"),
                main_to_vehicles_keys="Vin No",
                columns_to_drop=[
                    "Dealer ID_y",
                    "Consumer ID_y",
                    "Warranty Expiration Date_y",
                ],
                rename_columns={
                    "Dealer ID_x": "Dealer ID",
                    "Consumer ID_x": "Consumer ID",
                    "Warranty Expiration Date_x": "Warranty Expiration Date",
                },
            )

            logger.debug(f"Orphan records found: {orphans_df.shape[0]}")
            save_error_file(orphans_df, dealer_id, current_date)

            logger.info(f"Uploading merged file to S3 for dealer {dealer_id}")
            csv_buffer = merged_df.to_csv(index=False)
            unique_id = str(uuid.uuid4())
            s3_key = f"quiter/repair_order/{dealer_id}/{current_date.year}/{current_date.month}/{current_date.day}/{unique_id}.csv"
            s3_client.put_object(
                Bucket=INTEGRATIONS_BUCKET, Key=s3_key, Body=csv_buffer
            )
    except ValueError as e:
        logger.error(f"ValueError encountered: {e}")
        notify_client_engineering("Data consistency error in Quiter", str(e))
        raise
    except ClientError as e:
        logger.error(f"ClientError encountered during S3 operation: {e}")
        notify_client_engineering("AWS S3 error in Quiter", str(e))
        raise
    except Exception as e:
        logger.error(f"Unexpected error encountered: {e}")
        notify_client_engineering("Unexpected error in Quiter", str(e))
        raise
