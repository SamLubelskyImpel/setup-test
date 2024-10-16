import os
from os import environ
import boto3
import logging
import pandas as pd
import uuid
import io
import chardet
import re
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


def clean_data(df, id_column, important_columns):
    """
    Clean a DataFrame by keeping the record with the most complete data for each unique identifier.

    - Sorts rows by the unique ID and the important columns.
    - Drops duplicates by the unique ID, keeping the one with more complete data (fewer NaN values).

    Args:
    - df (pd.DataFrame): The DataFrame to clean.
    - id_column (str): The name of the column that contains the unique identifier (e.g., 'Vin No', 'Dealer Customer No').
    - important_columns (list): Columns that are prioritized when deciding which row to keep.
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


def merge_files(
    main_df,
    customers_df,
    vehicles_df,
    main_to_customers_keys,
    main_to_vehicles_keys,
    columns_to_drop=None,
    rename_columns=None,
):
    """
    Generic function to merge a main dataframe (e.g., sales, service, etc.) with customers and vehicles dataframes.

    The function performs an inner join between:
      - Main dataframe and Customers on specified keys
      - Main dataframe and Vehicles on specified keys

    After merging, optional column drops and renaming are performed.

    :param main_df: The main dataframe (e.g., vehicle sales, service data, etc.)
    :param customers_df: Dataframe containing customer information
    :param vehicles_df: Dataframe containing vehicle information
    :param main_to_customers_keys: Tuple of columns to join main_df and customers_df (left_on, right_on)
    :param main_to_vehicles_keys: Columns to join main_df and vehicles_df on
    :param columns_to_drop: List of columns to drop after merging (default: None)
    :param rename_columns: Dictionary for renaming columns after merging (default: None)
    :return: Merged dataframe
    """
    try:
        merged_df = pd.merge(
            main_df,
            customers_df,
            left_on=main_to_customers_keys[0],
            right_on=main_to_customers_keys[1],
            how="inner",
        )
        
        merged_df = pd.merge(
            merged_df,
            vehicles_df,
            left_on=main_to_vehicles_keys,
            right_on=main_to_vehicles_keys,
            how="inner",
        )

        if columns_to_drop:
            merged_df = merged_df.drop(columns=columns_to_drop, errors="ignore")
        if rename_columns:
            merged_df = merged_df.rename(columns=rename_columns)

        return merged_df
    except KeyError as e:
        logger.error(f"KeyError during merging: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in merge_files: {e}")
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
    error_file_key = f"quiter/error_files/repair_order/{dealer_id}/{current_date.year}/{current_date.month}/{current_date.day}/{unique_id}_orphan_records.csv"
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    s3_client.put_object(
        Bucket=INTEGRATIONS_BUCKET, Key=error_file_key, Body=csv_buffer.getvalue()
    )
    subject = "Orphan records detected in Quiter"
    message = f"Orphan records detected for dealer {dealer_id}."
    notify_client_engineering(subject, message)


def find_matching_files(s3_files):
    """Find matching files for repair orders, consumers, and vehicles based on patterns."""
    try:
        found_files = {}
        for file in s3_files:
            file_name = file["Key"].split("/")[-1]
            for file_type, patterns in FILE_PATTERNS.items():
                for pattern in patterns:
                    if pattern in file_name:
                        found_files[file_type] = file["Key"]
                        break
        if not found_files:
            raise ValueError(
                f"No matching files found for the patterns: {FILE_PATTERNS}"
            )
        return found_files
    except KeyError as e:
        logger.error(f"KeyError while finding matching files: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in find_matching_files: {e}")
        raise


def list_files_in_s3(base_path):
    """List files in the S3 path with the given prefix."""
    try:
        response = s3_client.list_objects_v2(
            Bucket=INTEGRATIONS_BUCKET, Prefix=base_path
        )
        if "Contents" not in response:
            raise ValueError(
                f"No files found at S3 path: {INTEGRATIONS_BUCKET}/{base_path}"
            )
        return response.get("Contents", [])
    except ClientError as e:
        logger.error(f"Failed to list files in S3: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error when listing files: {e}")
        raise


def read_csv_from_s3(s3_body, file_name, file_type, sns_client, topic_arn, dtype=None):
    """
    Helper function to read CSV file from S3 and handle encoding errors.

    Parameters:
    - s3_body: S3 file body content
    - file_name: Name of the S3 file for logging purposes
    - file_type: A string to indicate the type of the file being processed (e.g., 'Consumer', 'Vehicle')
    - sns_client: The boto3 SNS client for sending error notifications
    - topic_arn: The SNS topic ARN for notifications
    - dtype: Optional, a dictionary to specify data types for specific columns

    Returns:
    - DataFrame of the CSV content if successful, else raises an error
    """
    try:
        return pd.read_csv(io.BytesIO(s3_body), delimiter=';', encoding='us-ascii', on_bad_lines='warn', dtype=dtype)
    except Exception as e:
        error_message = f"Error processing '{file_type}' file: {file_name} - {str(e)}"
        logger.error(error_message)
        notify_client_engineering(error_message, sns_client, topic_arn)
        raise


def standardize_data_types(repairorder_df, customers_df, vehicles_df):
    """
    Standardize the data types of key columns in the DataFrames.
    """
    logger.info("Starting to standardize data types for key columns.")

    try:
        logger.info("Standardizing 'Consumer ID' in repairorder_df.")
        repairorder_df['Consumer ID'] = repairorder_df['Consumer ID'].astype(str)

        logger.info("Standardizing 'Dealer Customer No' in customers_df.")
        customers_df['Dealer Customer No'] = customers_df['Dealer Customer No'].astype(str)
        logger.info("Standardizing 'Vin No' in repairorder_df.")
        repairorder_df['Vin No'] = repairorder_df['Vin No'].astype(str)

        logger.info("Standardizing 'Vin No' in vehicles_df.")
        vehicles_df['Vin No'] = vehicles_df['Vin No'].astype(str)

        logger.info("Data type standardization completed successfully.")

    except Exception as e:
        logger.error(f"Error during data type standardization: {e}")
        raise

    return repairorder_df, customers_df, vehicles_df


def identify_and_separate_records(repairorder_df, customers_df, vehicles_df):
    """
    Identify orphan records and separate valid records.

    Orphan records are those in the repair order file that do not have a corresponding
    entry in the consumer or vehicle files.

    This function separates valid records (with corresponding entries) from orphan records
    (missing either a customer or vehicle).
    """
    try:
        logger.info("Starting the identification of orphan records.")

        repairorder_df, customers_df, vehicles_df = standardize_data_types(
            repairorder_df, customers_df, vehicles_df
        )

        missing_customers = repairorder_df[~repairorder_df['Consumer ID'].isin(customers_df['Dealer Customer No'])]
        missing_customer_ids = missing_customers['Consumer ID'].unique()
        logger.info(f"Identified {len(missing_customers)} missing customer records.")

        missing_vehicles = repairorder_df[~repairorder_df['Vin No'].isin(vehicles_df['Vin No'])]
        missing_vin_numbers = missing_vehicles['Vin No'].unique()
        logger.info(f"Identified {len(missing_vehicles)} missing vehicle records.")

        orphans_df = repairorder_df[
            repairorder_df['Consumer ID'].isin(missing_customer_ids) | 
            repairorder_df['Vin No'].isin(missing_vin_numbers)
        ]
        logger.info(f"Found {len(orphans_df)} orphan records.")

        valid_records_df = repairorder_df[
            ~repairorder_df['Consumer ID'].isin(missing_customer_ids) & 
            ~repairorder_df['Vin No'].isin(missing_vin_numbers)
        ]
        logger.info(f"Separated {len(valid_records_df)} valid records.")

        return valid_records_df, orphans_df

    except KeyError as e:
        logger.error(f"KeyError during record identification: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during record identification: {e}")
        raise
