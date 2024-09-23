"""Merging logic for “Repair Orders”, “Customers” and “Vehicles” from Quiter."""

import os
from os import environ
import boto3
import logging
import pandas as pd
import uuid
import gzip
import io
import chardet
import re
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
    environ.get("MERGE_VEHICLE_SALE_QUEUE"),
]

FILE_PATTERNS = {
    "RepairOrder": ["RO"],
    "Consumer": ["CONS"],
    "Vehicle": ["VEH"],
    "Appointment": ["APPT"],
    "VehicleSales": ["VS", "SalesTxn", "SaleTxn"],
}


def detect_file_encoding(file_content):
    """Detect the encoding of a file."""
    result = chardet.detect(file_content)
    encoding = result["encoding"]
    logger.info(f"Detected file encoding: {encoding}")
    return encoding


def remove_duplicates(df, id_column, important_columns):
    """
    Remove duplicates based on a specific ID column and prioritize rows with the most data.

    Args:
        df (pd.DataFrame): The DataFrame to clean.
        id_column (str): The column used to identify duplicates (e.g., 'customer_id').
        important_columns (list): List of columns that are prioritized when deciding which row to keep.

    Returns:
        pd.DataFrame: Cleaned DataFrame without duplicates.
    """
    try:
        if id_column not in df.columns:
            raise KeyError(f"ID column '{id_column}' not found in DataFrame.")

        df["non_empty_count"] = df.notnull().sum(axis=1)
        df = df.sort_values(
            by=[id_column] + important_columns + ["non_empty_count"],
            ascending=[True] + [False] * len(important_columns) + [False],
        )
        df_cleaned = df.drop_duplicates(subset=id_column, keep="first")
        df_cleaned = df_cleaned.drop(columns=["non_empty_count"])

        return df_cleaned
    except KeyError as e:
        logger.error(f"KeyError during data cleaning: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in clean_data: {e}")
        raise


def merge_files(repair_orders_df, customers_df, vehicles_df):
    """
    Merge repair orders, customers, and vehicles dataframes on customer_id and vin.

    This function performs left merges to identify orphan records (those without a corresponding customer or vehicle).

    Returns:
        tuple: A tuple containing:
            - merged_df (pd.DataFrame): Merged DataFrame with valid records.
            - orphan_records_df (pd.DataFrame): DataFrame containing orphan records.
    """
    try:
        # Merge Repair Orders with Customers on Consumer ID using left join
        merged_customer = pd.merge(
            repair_orders_df, customers_df, on="Consumer ID", how="left", indicator=True
        )
        # Identify orphan records where customer data is missing
        orphan_customers = merged_customer[merged_customer["_merge"] == "left_only"]
        merged_customer = merged_customer[merged_customer["_merge"] == "both"].drop(
            columns=["_merge"]
        )

        # Merge the result with Vehicles on Vin No using left join
        merged_vehicle = pd.merge(
            merged_customer, vehicles_df, on="Vin No", how="left", indicator=True
        )
        # Identify orphan records where vehicle data is missing
        orphan_vehicles = merged_vehicle[merged_vehicle["_merge"] == "left_only"]
        merged_df = merged_vehicle[merged_vehicle["_merge"] == "both"].drop(
            columns=["_merge"]
        )

        # Combine orphan records
        orphan_records_df = pd.concat(
            [orphan_customers, orphan_vehicles], ignore_index=True
        )
        redundant_columns = [col for col in merged_df.columns if "_y" in col]
        merged_df = merged_df.drop(columns=redundant_columns, errors="ignore")
        merged_df.columns = [col.replace("_x", "") for col in merged_df.columns]

        return merged_df, orphan_records_df
    except KeyError as e:
        logger.error(f"KeyError during merging: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in merge_files: {e}")
        raise


def load_dataframe_from_s3(file_key):
    """
    Load a CSV file from S3, detect encoding, and handle bad lines.

    Args:
        file_key (str): S3 key of the file to load.

    Returns:
        pd.DataFrame: DataFrame loaded from the CSV file.
    """
    try:
        obj = s3_client.get_object(Bucket=INTEGRATIONS_BUCKET, Key=file_key)
        with gzip.GzipFile(fileobj=io.BytesIO(obj["Body"].read())) as gz:
            file_body = gz.read()
        # Detect encoding
        encoding = detect_file_encoding(file_body)
        df = pd.read_csv(
            io.BytesIO(file_body), delimiter=";", encoding=encoding, on_bad_lines="warn"
        )
        return df
    except ClientError as e:
        logger.error(f"Error loading file from S3: {e}")
        raise


def notify_client_engineering(subject, message):
    """
    Send a notification to the client engineering SNS topic.

    Args:
        subject (str): The subject of the notification.
        message (str): The message body of the notification.
    """
    try:
        sns_client.publish(
            TopicArn=TOPIC_ARN,
            Subject=subject,
            Message=message,
        )
    except ClientError as e:
        logger.error(f"Error sending SNS notification: {e}")
        raise


def extract_date_from_key(s3_key):
    """
    Extract date components (year, month, day) from an S3 key.

    The S3 key is expected to contain the date in the format: YYYY/MM/DD.
    Example: 'somepath/2024/09/12/somefile.csv'

    Args:
        s3_key (str): The S3 key to extract the date from.

    Returns:
        tuple: A tuple containing (year, month, day).
    """
    try:
        match = re.search(r"(\d{4})/(\d{1,2})/(\d{1,2})", s3_key)
        if match:
            year, month, day = match.groups()
            return year, month, day
        else:
            raise ValueError(
                f"Invalid S3 key format. Unable to extract date from {s3_key}"
            )
    except Exception as e:
        logger.error(f"Error extracting date from S3 key: {e}")
        raise


def save_error_file(df, dealer_id, current_date):
    """
    Save orphan records to an error file in S3 and send a notification.

    Args:
        df (pd.DataFrame): DataFrame containing orphan records.
        dealer_id (str): Dealer ID associated with the records.
        current_date (datetime): Current date to use in the S3 key.
    """
    if df.empty:
        return
    unique_id = str(uuid.uuid4())
    s3_key = f"quiter/error_records/{dealer_id}/{current_date.year}/{current_date.month}/{current_date.day}/{unique_id}.csv"
    csv_buffer = df.to_csv(index=False)
    s3_client.put_object(Bucket=INTEGRATIONS_BUCKET, Key=s3_key, Body=csv_buffer)
    subject = "Orphan records detected in Quiter"
    message = f"Orphan records detected for dealer {dealer_id}. File saved at {s3_key}."
    notify_client_engineering(subject, message)


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
            response = s3_client.list_objects_v2(
                Bucket=INTEGRATIONS_BUCKET, Prefix=base_path
            )
            files = response.get("Contents", [])
            if not files:
                raise ValueError(
                    f"No files found in the S3 bucket with prefix {base_path}"
                )
            repair_orders_df = pd.DataFrame()
            customers_df = pd.DataFrame()
            vehicles_df = pd.DataFrame()
            for file in files:
                file_name = file["Key"].split("/")[-1]
                if any(
                    pattern in file_name for pattern in FILE_PATTERNS["RepairOrder"]
                ) and file_name.endswith(".csv"):
                    temp_df = load_dataframe_from_s3(file["Key"])
                    repair_orders_df = pd.concat(
                        [repair_orders_df, temp_df], ignore_index=True
                    )
                elif any(
                    pattern in file_name for pattern in FILE_PATTERNS["Consumer"]
                ) and file_name.endswith(".csv"):
                    temp_df = load_dataframe_from_s3(file["Key"])
                    customers_df = pd.concat([customers_df, temp_df], ignore_index=True)
                elif any(
                    pattern in file_name for pattern in FILE_PATTERNS["Vehicle"]
                ) and file_name.endswith(".csv"):
                    temp_df = load_dataframe_from_s3(file["Key"])
                    vehicles_df = pd.concat([vehicles_df, temp_df], ignore_index=True)
            if repair_orders_df.empty or customers_df.empty or vehicles_df.empty:
                raise ValueError("Missing one or more required data files.")
            customers_df = remove_duplicates(customers_df, "Dealer Customer No", [])
            vehicles_df = remove_duplicates(
                vehicles_df, "Vin No", ["OEM Name", "Model"]
            )

            merged_df, orphan_records_df = merge_files(
                repair_orders_df, customers_df, vehicles_df
            )
            save_error_file(orphan_records_df, dealer_id, current_date)
            csv_buffer = merged_df.to_csv(index=False)
            unique_id = str(uuid.uuid4())
            s3_key = f"quiter/repair_order/{dealer_id}/{current_date.year}/{current_date.month}/{current_date.day}/{unique_id}.csv"
            s3_client.put_object(
                Bucket=INTEGRATIONS_BUCKET, Key=s3_key, Body=csv_buffer
            )
    except ValueError as e:
        notify_client_engineering("Data consistency error in Quiter", str(e))
        raise
    except ClientError as e:
        notify_client_engineering("AWS S3 error in Quiter", str(e))
        raise
    except Exception as e:
        notify_client_engineering("Unexpected error in Quiter", str(e))
        raise
