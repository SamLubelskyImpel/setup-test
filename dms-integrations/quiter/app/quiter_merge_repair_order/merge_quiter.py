"""Merging and transformation logic for “Repair Orders”, “Customers” and “Vehicles” from Quiter."""

import boto3
import pandas as pd
import os
import logging

from json import loads
from datetime import datetime
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3")

BUCKET_NAME = os.environ["S3_BUCKET_NAME"]
TOPIC_ARN = os.environ["CLIENT_ENGINEERING_SNS_TOPIC_ARN"]


def merge_files(repair_orders_df, customers_df, vehicles_df):
    """Merge repair orders, customers, and vehicles dataframes on customer_id and vin."""
    merged_df = pd.merge(repair_orders_df, customers_df, on="customer_id", how="inner")
    merged_df = pd.merge(merged_df, vehicles_df, on="vin", how="inner")
    return merged_df


def lambda_handler(event, context):
    try:
        for record in event["Records"]:
            data = loads(record["body"])
            dealer_id = data["dealer_id"]
            unique_id = data["uuid"]
            current_date = datetime.now()

            repair_orders_key = f"quiter/landing_zone/repair_order/{dealer_id}/{current_date.year}/{current_date.month}/{current_date.day}/{unique_id}"
            customers_key = f"/quiter/landing_zone/consumer/{dealer_id}/{current_date.year}/{current_date.month}/{current_date.day}/{unique_id}"
            vehicles_key = f"quiter/landing_zone/vehicle/{dealer_id}/{current_date.year}/{current_date.month}/{current_date.day}/{unique_id}"

            repair_orders_obj = s3_client.get_object(
                Bucket=BUCKET_NAME, Key=repair_orders_key
            )
            customers_obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=customers_key)
            vehicles_obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=vehicles_key)

            repair_orders_df = pd.read_csv(repair_orders_obj["Body"])
            customers_df = pd.read_csv(customers_obj["Body"])
            vehicles_df = pd.read_csv(vehicles_obj["Body"])

            if len(repair_orders_df) != len(customers_df) or len(customers_df) != len(
                vehicles_df
            ):
                raise ValueError(
                    "Mismatch in the number of customers, vehicles, or repair orders."
                )

            merged_df = merge_files(repair_orders_df, customers_df, vehicles_df)

            current_date = datetime.now()
            csv_buffer = merged_df.to_csv(index=False)
            s3_key = f"quiter/repair_order_merge/{current_date.year}/{current_date.month}/{current_date.day}/{unique_id}"
            s3_client.put_object(Bucket=BUCKET_NAME, Key=s3_key, Body=csv_buffer)

            logger.info(f"Merged file saved to {s3_key} in the raw zone.")

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
        Subject="QuiterMergeRepairOrder Lambda Error",
        Message=str(error_message),
    )
