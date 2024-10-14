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
    list_files_in_s3,
    save_error_file,
    extract_date_from_key,
    notify_client_engineering,
    merge_files,
    clean_data,
)

# Set up logging
logger = logging.getLogger()
logger.setLevel(os.getenv("LOGLEVEL", "INFO").upper())

# AWS clients
s3_client = boto3.client("s3")

# Environment variables
INTEGRATIONS_BUCKET = environ.get("INTEGRATIONS_BUCKET")
TOPIC_ARN = os.getenv("CLIENT_ENGINEERING_SNS_TOPIC_ARN")
SNS_CLIENT = boto3.client("sns")


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
            dealer_id = data["dealer_id"]
            s3_key = data["s3_key"]
            current_date = datetime.now()

            year, month, day = extract_date_from_key(s3_key)
            base_path = f"quiter/landing_zone/{dealer_id}/{year}/{month}/{day}/"

            files = list_files_in_s3(base_path)

            logger.debug(f"Files found in S3: {files}")
            found_files = find_matching_files(files)

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
                repairorder_obj = s3_client.get_object(
                    Bucket=INTEGRATIONS_BUCKET, Key=found_files["RepairOrder"]
                )
                customers_obj = s3_client.get_object(
                    Bucket=INTEGRATIONS_BUCKET, Key=found_files["Consumer"]
                )
                vehicles_obj = s3_client.get_object(
                    Bucket=INTEGRATIONS_BUCKET, Key=found_files["Vehicle"]
                )


                customers_df = read_csv_from_s3(
                    customers_obj["Body"].read(),
                    found_files["Consumer"],
                    "Consumer",
                    SNS_CLIENT,
                    TOPIC_ARN,
                )

                vehicles_df = read_csv_from_s3(
                    vehicles_obj["Body"].read(),
                    found_files["Vehicle"],
                    "Vehicle",
                    SNS_CLIENT,
                    TOPIC_ARN,
                )

                repairorder_df = read_csv_from_s3(
                    repairorder_obj["Body"].read(),
                    found_files["RepairOrder"],
                    "RepairOrder",
                    SNS_CLIENT,
                    TOPIC_ARN,
                    dtype={"Dealer ID": "string", "Dealer Customer No": "string"},
                )

                logger.info(f"retrieved and converted files, repairorder_df:{repairorder_df}, vehicles_df: {vehicles_df}, customers_df: {customers_df}")

            except KeyError as e:
                logger.error(f"KeyError while fetching or reading files from S3: {e}")
                raise

            customers_df_clean = clean_data(
                customers_df, "Dealer Customer No", []
            )
            vehicles_df_clean = clean_data(
                vehicles_df, "Vin No", ["OEM Name", "Model"]
            )

            valid_records_df, orphans_df = identify_and_separate_records(
                repairorder_df, customers_df_clean, vehicles_df_clean
            )

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
            merged_df = merged_df.drop_duplicates()
            merged_df = merged_df.loc[:, ~merged_df.columns.duplicated()]
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
