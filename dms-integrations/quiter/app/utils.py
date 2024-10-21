import boto3
import pandas as pd
import os
import logging
import uuid

from botocore.exceptions import ClientError
import re
import io

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3")

BUCKET_NAME = os.environ["INTEGRATIONS_BUCKET"]
TOPIC_ARN = os.environ["CLIENT_ENGINEERING_SNS_TOPIC_ARN"]
INTEGRATIONS_BUCKET = os.environ.get("INTEGRATIONS_BUCKET")
SNS_CLIENT = boto3.client("sns")

FILE_PATTERNS = {
    "RepairOrder": ["RO"],
    "Consumer": ["CONS"],
    "Vehicle": ["VEH"],
    "Appointments": ["APPT"],
    "VehicleSales": ["VS", "SalesTxn", "SaleTxn"]
}

def list_files_in_s3(prefix):
    """List files in the S3 path with the given prefix."""
    try:
        response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)
        if 'Contents' not in response:
            raise ValueError(f"No files found at S3 path: {BUCKET_NAME}/{prefix}")
        return response.get("Contents", [])
    except ClientError as e:
        logger.error(f"Failed to list files in S3: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error when listing files: {e}")
        raise

def find_matching_files(s3_files):
    """Find matching files for repair orders, consumers, and vehicles based on patterns."""
    try:
        found_files = {}
        for file in s3_files:
            # Extract the file name from the S3 key
            file_name = file["Key"].split('/')[-1]
            # Iterate over predefined file patterns to match each file
            for file_type, patterns in FILE_PATTERNS.items():
                # Match the file name to a known pattern and store it in the result
                for pattern in patterns:
                    if pattern in file_name:
                        found_files[file_type] = file["Key"]
                        break
        if not found_files:
            raise ValueError(f"No matching files found for the patterns: {FILE_PATTERNS}")
        return found_files
    except KeyError as e:
        logger.error(f"KeyError while finding matching files: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in find_matching_files: {e}")
        raise

def merge_files(main_df, customers_df, vehicles_df, main_to_customers_keys, main_to_vehicles_keys, columns_to_drop=None, rename_columns=None):
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
        # First, merge main dataframe with Customers based on specified keys
        merged_df = pd.merge(main_df, customers_df, left_on=main_to_customers_keys[0], right_on=main_to_customers_keys[1], how="inner")

        # Then, merge the result with Vehicles based on VIN or other vehicle-related keys
        merged_df = pd.merge(merged_df, vehicles_df, left_on=main_to_vehicles_keys, right_on=main_to_vehicles_keys, how="inner")

        # Drop redundant or unnecessary columns if specified
        if columns_to_drop:
            merged_df = merged_df.drop(columns=columns_to_drop, errors='ignore')

        # Rename columns if a rename dictionary is provided
        if rename_columns:
            merged_df = merged_df.rename(columns=rename_columns)

        return merged_df
    except KeyError as e:
        logger.error(f"KeyError during merging: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in merge_files: {e}")
        raise

def extract_date_from_key(s3_key):
    """
    Extract date components (year, month, day) from an S3 key.
    
    The S3 key is expected to contain the date in the format: YYYY/MM/DD.
    Example: 'somepath/2024/09/12/somefile.csv'
    """
    try:
        match = re.search(r"(\d{4})/(\d{1,2})/(\d{1,2})", s3_key)
        if match:
            year, month, day = match.groups()
            return year, month, day
        else:
            raise ValueError(f"Invalid S3 key format. Unable to extract date from {s3_key}")
    except Exception as e:
        logger.error(f"Error extracting date from S3 key: {e}")
        raise

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
        
        # Create a temporary column to count non-empty fields in each row
        df['non_empty_count'] = df.notnull().sum(axis=1)
        
        # Sort the DataFrame by ID, important columns, and non-empty count
        df = df.sort_values(by=[id_column] + important_columns + ['non_empty_count'], ascending=[True] + [False] * len(important_columns) + [False])

        # Drop duplicates based on the ID, keeping the row with the most data (fewer NaNs)
        df_cleaned = df.drop_duplicates(subset=id_column, keep='first')

        # Drop the temporary column used for sorting
        df_cleaned = df_cleaned.drop(columns=['non_empty_count'])

        return df_cleaned
    except KeyError as e:
        logger.error(f"KeyError during data cleaning: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in clean_data: {e}")
        raise

def clean_id_column(df, column_name):
    """
    Cleans up an ID column by ensuring it's a string and removing any floating point artifacts like '.0'.
    Replaces NaN values with an empty string and logs the processing.
    """
    def process_value(x):
        if pd.isna(x):
            return ''
        else:
            return str(int(x)) if isinstance(x, (int, float)) else str(x)
    
    return df[column_name].apply(process_value)



def identify_and_separate_records(main_df, customers_df, vehicles_df):
    """
    Identify orphan records and separate valid records.
    
    Orphan records are those in the main file that do not have a corresponding 
    entry in the consumer or vehicle files.
    
    This function separates valid records (with corresponding entries) from orphan records 
    (missing either a customer or vehicle).
    """
    try:
        # Ensure both Consumer ID and Dealer Customer No are cleaned for comparison
        main_df['Consumer ID'] = clean_id_column(main_df, 'Consumer ID')
        customers_df['Dealer Customer No'] = clean_id_column(customers_df, 'Dealer Customer No')

        # Identify orphan consumer records by checking if 'Consumer ID' in main_df is not present in customers_df
        missing_customers = main_df[~main_df['Consumer ID'].isin(customers_df['Dealer Customer No'])]
        missing_customer_ids = missing_customers['Consumer ID'].unique()

        # Identify orphan vehicle records by checking if 'Vin No' in main_df is not present in vehicles_df
        missing_vehicles = main_df[~main_df['Vin No'].isin(vehicles_df['Vin No'])]
        missing_vin_numbers = missing_vehicles['Vin No'].unique()

        # Combine missing records (those with missing customer or vehicle)
        orphans_df = main_df[
            main_df['Consumer ID'].isin(missing_customer_ids) | 
            main_df['Vin No'].isin(missing_vin_numbers)
        ]

        # Valid records are those not in the orphan list
        valid_records_df = main_df[
            ~main_df['Consumer ID'].isin(missing_customer_ids) & 
            ~main_df['Vin No'].isin(missing_vin_numbers)
        ]

        return valid_records_df, orphans_df
    except KeyError as e:
        logger.error(f"KeyError during record identification: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in identify_and_separate_records: {e}")
        raise


def save_to_s3(df, bucket_name, key):
    """
    Save a DataFrame to S3 as a CSV file.
    
    Converts the DataFrame to a CSV string and uploads it to the specified S3 bucket and key.
    """
    try:
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        s3_client.put_object(Bucket=bucket_name, Key=key, Body=csv_buffer.getvalue())
        logger.info(f"Saved file to S3: {key}")
    except ClientError as e:
        logger.error(f"Failed to save file to S3: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error when saving file to S3: {e}")
        raise




def notify_client_engineering(error_message, sns_client, topic_arn, subject):
    """Send a notification to the client engineering SNS topic."""
    sns_client.publish(
        TopicArn=topic_arn,
        Subject=subject,
        Message=str(error_message),
    )


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
        notify_client_engineering(error_message, sns_client, topic_arn, 'Error Reading CSV')
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
    notify_client_engineering(message, SNS_CLIENT, TOPIC_ARN, subject)