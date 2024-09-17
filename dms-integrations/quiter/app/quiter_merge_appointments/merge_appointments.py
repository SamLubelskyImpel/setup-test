"""Merging logic for “Appointments”, “Customers” and “Vehicles” from Quiter."""
"""Much of this code is similar to other files and should be extracted to a separate file in the future"""
import boto3
import pandas as pd
import os
import logging
import re 
import io
import chardet

from json import loads, dumps
from datetime import datetime
from botocore.exceptions import ClientError
from uuid import uuid4

logger = logging.getLogger()
logger.setLevel(logging.INFO)

S3_CLIENT = boto3.client("s3")
SQS_CLIENT = boto3.client("sqs")
BUCKET_NAME = os.environ["INTEGRATIONS_BUCKET"]
TOPIC_ARN = os.environ["CE_TOPIC"]

def detect_encoding(file_body_bytes, sample_size=100000):
    """Detect the encoding using a sample of the file."""
    # Use only the first `sample_size` bytes for encoding detection
    sample = file_body_bytes[:sample_size]
    result = chardet.detect(sample)
    encoding = result['encoding']
    logger.info(f"Detected encoding: {encoding}")
    return encoding

def find_object(pattern: str, s3_prefix: str):
    objects = boto3.resource('s3').Bucket(BUCKET_NAME).objects.filter(Prefix=s3_prefix)

    for object in objects:
        if pattern in object.key:
            return S3_CLIENT.get_object(Bucket=BUCKET_NAME, Key=object.key)

    raise Exception(f"Could not find {pattern} file in s3 path {s3_prefix}")

def get_dms_id(s3_prefix: str):
    objects = boto3.resource('s3').Bucket(BUCKET_NAME).objects.filter(Prefix=s3_prefix)

    for object in objects:
        dms_id = object.key.split('/')[6].split('_')[0]
        logger.info(f"DMS_ID = {dms_id}")
        return dms_id

    raise Exception(f"Could not find {pattern} file in s3 path {s3_prefix}")

def merge_files(main_df, customers_df, vehicles_df):
    """Merge appointments, customers, and vehicles dataframes on Consumer ID and Vin No."""
    merged_df = pd.merge(main_df, customers_df, left_on=["Consumer ID"], right_on=["Dealer Customer No"], how="inner")
    merged_df = pd.merge(merged_df, vehicles_df, on=["Vin No"], how="inner")

    # Drop redundant or duplicate columns (from multiple joins)
    merged_df = merged_df.drop(columns=['Dealer ID_y', 'Consumer ID_y', 'Warranty Expiration Date_y'], errors='ignore')  # Adjust these as needed
        
    # Rename columns for consistency
    merged_df = merged_df.rename(columns={
        'Dealer ID_x': 'Dealer ID',
        'Consumer ID_x': 'Consumer ID',
        'Warranty Expiration Date_x': 'Warranty Expiration Date'
    })
    return merged_df

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

def identify_and_separate_records(appointments_df, customers_df, vehicles_df):
    """
    Identify orphan records and separate valid records.
    
    Orphan records are those in the appontments file that do not have a corresponding 
    entry in the consumer or vehicle files.
    
    This function separates valid records (with corresponding entries) from orphan records 
    (missing either a customer or vehicle).
    """
    try:
        # Identify orphan consumer records by checking if 'Consumer ID' in appointments_df is not present in customers_df
        missing_customers = appointments_df[~appointments_df['Consumer ID'].isin(customers_df['Dealer Customer No'])]
        missing_customer_ids = missing_customers['Consumer ID'].unique()

        # Identify orphan vehicle records by checking if 'Vin No' in appointments_df is not present in vehicles_df
        missing_vehicles = appointments_df[~appointments_df['Vin No'].isin(vehicles_df['Vin No'])]
        missing_vin_numbers = missing_vehicles['Vin No'].unique()

        # Combine missing records (those with missing customer or vehicle)
        orphans_df = appointments_df[
            appointments_df['Consumer ID'].isin(missing_customer_ids) | 
            appointments_df['Vin No'].isin(missing_vin_numbers)
        ]

        # Valid records are those not in the orphan list
        valid_records_df = appointments_df[
            ~appointments_df['Consumer ID'].isin(missing_customer_ids) & 
            ~appointments_df['Vin No'].isin(missing_vin_numbers)
        ]

        return valid_records_df, orphans_df
    except KeyError as e:
        logger.error(f"KeyError during record identification: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in identify_and_separate_records: {e}")
        raise


def lambda_handler(event, context):
    try:
        for record in event["Records"]:
            data = loads(record["body"])
            dealer_id = data["dealer_id"]
            s3_key = data["s3_key"]
            dms_id = get_dms_id(s3_key)

            appointments_obj = find_object('APPT', s3_key)['Body'].read()
            customers_obj = find_object('CONS', s3_key)['Body'].read()
            vehicles_obj = find_object('VEH', s3_key)['Body'].read()

            consumer_encoding = detect_encoding(customers_obj) 
            vehicle_encoding = detect_encoding(vehicles_obj)
            appointments_encoding = detect_encoding(appointments_obj)

            appointments_df = pd.read_csv(io.BytesIO(appointments_obj), delimiter=';', encoding=appointments_encoding, on_bad_lines='warn', dtype={'Dealer ID': 'string'})
            customers_df = pd.read_csv(io.BytesIO(customers_obj), delimiter=';', encoding=consumer_encoding, on_bad_lines='warn', dtype={'Dealer ID': 'string'})
            vehicles_df = pd.read_csv(io.BytesIO(vehicles_obj), delimiter=';', encoding=vehicle_encoding, on_bad_lines='warn', dtype={'Dealer ID': 'string'})

            # Clean the customer and vehicle data using the unified function
            cleaned_customers_df = clean_data(customers_df, 'Dealer Customer No', [])
            cleaned_vehicles_df = clean_data(vehicles_df, 'Vin No', ['OEM Name', 'Model'])

            # Identify missing records in vehicle_sales_df compared to customers_df and vehicles_df
            valid_records_df, orphans_df = identify_and_separate_records(appointments_df, cleaned_customers_df, cleaned_vehicles_df)

            current_date = datetime.now()

            # Save the orphan records to an error file
            if not orphans_df.empty:
                unique_id = str(uuid4())
                error_file_key = f"quiter/error_files/{dealer_id}/{current_date.year}/{current_date.month}/{current_date.day}/{unique_id}_orphan_records.csv"
                csv_buffer = io.StringIO()
                orphans_df.to_csv(csv_buffer, index=False)
                S3_CLIENT.put_object(Bucket=BUCKET_NAME, Key=error_file_key, Body=csv_buffer.getvalue())

                # Send notification about the error file
                notify_client_engineering(f"Orphan records found. Error file saved at {error_file_key}")

            merged_df = merge_files(valid_records_df, cleaned_customers_df, cleaned_vehicles_df)

            
            csv_buffer = merged_df.to_csv(index=False)
            s3_key = f"quiter/service_appointment/{current_date.year}/{current_date.month}/{current_date.day}/merged_appointments_{str(uuid4())}.csv"
            S3_CLIENT.put_object(Bucket=BUCKET_NAME, Key=s3_key, Body=csv_buffer)

            logger.info(f"Merged file saved to {s3_key} in the raw zone.")

            queue_url = os.environ["FORMAT_APPOINTMENT_QUEUE"]
            data = {
                "dms_id": dms_id,
                "s3_key": s3_key,
            }
            response = SQS_CLIENT.send_message(
                QueueUrl=queue_url,
                MessageBody=dumps(data)
            )
            # Verify that the message was sent successfully by checking the response

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
        Subject="QuiterMergeAppointment Lambda Error",
        Message=str(error_message),
    )
    return