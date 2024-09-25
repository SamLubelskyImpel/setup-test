import boto3
import pandas as pd
import os
import logging

from botocore.exceptions import ClientError
import re 
import io
import chardet

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3")

BUCKET_NAME = os.environ["INTEGRATIONS_BUCKET"]
# TOPIC_ARN = os.environ["CLIENT_ENGINEERING_SNS_TOPIC_ARN"]

FILE_PATTERNS = {
    "Consumer": ["CONS"],
    "Vehicle": ["VEH"],
    "VehicleSales": ["VS", "SalesTxn", "SaleTxn"],
    "Appointments": ["APPT"]
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

def merge_files(main_df, customers_df, vehicles_df):
    """
    Merge vehicle sales/repair orders/appointments, customers, and vehicles dataframes on customer_id and VIN.
    
    The function performs an inner join between:
      - Vehicle Sales/Repair Orders/Appointments and Consumers on "Consumer ID" and "Dealer Customer No"
      - Vehicle Sales/Repair Orders/Appointments and Vehicles on "Vin No"
    
    After merging, redundant columns are removed.
    """
    try:
        # First, merge Vehicle Sales with Consumers based on Consumer ID (customer_id)
        merged_df = pd.merge(main_df, customers_df, left_on="Consumer ID", right_on="Dealer Customer No", how="inner")

        # Then, merge the result with Vehicles based on VIN No
        merged_df = pd.merge(merged_df, vehicles_df, on="Vin No", how="inner")

        # Drop redundant or duplicate columns (from multiple joins)
        merged_df = merged_df.drop(columns=['Dealer ID_y', 'Consumer ID_y', 'Warranty Expiration Date_y'], errors='ignore')
        
        # Rename columns for consistency
        merged_df = merged_df.rename(columns={
            'Dealer ID_x': 'Dealer ID',
            'Consumer ID_x': 'Consumer ID',
            'Warranty Expiration Date_x': 'Warranty Expiration Date'
        })

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

def detect_encoding(file_body_bytes, sample_size=100000):
    """
    Detect the encoding of a file by analyzing the first few bytes.
    
    This function uses the chardet library to detect the encoding based on a 
    sample of the file's content.
    """
    try:
        sample = file_body_bytes[:sample_size]
        result = chardet.detect(sample)
        encoding = result['encoding']
        if not encoding:
            raise ValueError("Failed to detect file encoding.")
        return encoding
    except Exception as e:
        logger.error(f"Error detecting encoding: {e}")
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

def identify_and_separate_records(main_df, customers_df, vehicles_df):
    """
    Identify orphan records and separate valid records.
    
    Orphan records are those in the vehicle sales file that do not have a corresponding 
    entry in the consumer or vehicle files.
    
    This function separates valid records (with corresponding entries) from orphan records 
    (missing either a customer or vehicle).
    """
    try:
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