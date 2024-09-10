"""Merging logic for “Repair Orders”, “Customers” and “Vehicles” from Quiter."""

import os
import boto3
import logging
import pandas as pd
import uuid
import gzip
import io

from json import loads
from datetime import datetime
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(os.getenv("LOGLEVEL", "INFO").upper())

s3_client = boto3.client("s3")
sns_client = boto3.client("sns")

BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
TOPIC_ARN = os.getenv("CLIENT_ENGINEERING_SNS_TOPIC_ARN")



def merge_files(repair_orders_df, customers_df, vehicles_df):
    """Merges the dataframes for repair orders, customers, and vehicles using customer_id and vin."""
    merged_df = pd.merge(repair_orders_df, customers_df, on="customer_id", how="inner")
    merged_df = pd.merge(merged_df, vehicles_df, on="vin", how="inner")
    return merged_df


def load_dataframe_from_s3(file_key):
    """Loads and returns a dataframe from a gzipped CSV file in S3."""
    obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=file_key)
    with gzip.GzipFile(fileobj=io.BytesIO(obj["Body"].read())) as gz:
        df = pd.read_csv(gz)
    return df


def notify_client_engineering(error_message):
    """Sends a notification to the client engineering SNS topic."""
    sns_client.publish(
        TopicArn=TOPIC_ARN,
        Subject="QuiterMergeRepairOrder Lambda Error",
        Message=str(error_message),
    )


def lambda_handler(event, context):
    """Main function handling the merging of files and exception management."""
    try:
        for record in event["Records"]:
            data = loads(record["body"])
            dealer_id = data["dealer_id"]
            s3_key=data["s3_key"]
            current_date = datetime.now()

            base_path = f"quiter/landing_zone/{dealer_id}/{current_date.year}/{current_date.month}/{current_date.day}"
            response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=base_path)
            files = response.get("Contents", [])

            repair_orders_df = None
            customers_df = None
            vehicles_df = None

            for file in files:
                file_name = s3_key.split("/")[-1]

                if "RO" in file_name and file_name.endswith(".csv"):
                    repair_orders_df = load_dataframe_from_s3(s3_key)
                elif "CONS" in file_name and file_name.endswith(".csv"):
                    customers_df = load_dataframe_from_s3(s3_key)
                elif "VEH" in file_name and file_name.endswith(".csv"):
                    vehicles_df = load_dataframe_from_s3(s3_key)

            if not all([repair_orders_df, customers_df, vehicles_df]):
                raise ValueError("Missing one or more required data files.")

            if len(repair_orders_df) != len(customers_df) or len(customers_df) != len(
                vehicles_df
            ):
                raise ValueError(
                    "Mismatch in the number of customers, vehicles, or repair orders."
                )

            merged_df = merge_files(repair_orders_df, customers_df, vehicles_df)

            csv_buffer = merged_df.to_csv(index=False)
            unique_id = str(uuid.uuid4())
            s3_key = f"quiter/repair_order/{current_date.year}/{current_date.month}/{current_date.day}/{unique_id}"
            s3_client.put_object(Bucket=BUCKET_NAME, Key=s3_key, Body=csv_buffer)

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