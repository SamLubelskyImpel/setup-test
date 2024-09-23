"""Merging logic for “Appointments”, “Customers” and “Vehicles” from Quiter."""
"""Much of this code is similar to other files and should be extracted to a separate file in the future"""
import boto3
import pandas as pd
import os
import logging
import io

from json import loads, dumps
from datetime import datetime
from botocore.exceptions import ClientError
from uuid import uuid4
from utils import detect_encoding, merge_files, clean_data, identify_and_separate_records, save_to_s3, list_files_in_s3, find_matching_files

logger = logging.getLogger()
logger.setLevel(logging.INFO)

S3_CLIENT = boto3.client("s3")
SQS_CLIENT = boto3.client("sqs")
BUCKET_NAME = os.environ["INTEGRATIONS_BUCKET"]
TOPIC_ARN = os.environ["CE_TOPIC"]

def lambda_handler(event, context):
    try:
        for record in event["Records"]:
            data = loads(record["body"])
            dealer_id = data["dealer_id"]
            s3_key = data["s3_key"]
            
            appointments_obj = find_object('APPT', s3_key)['Body'].read()
            customers_obj = find_object('CONS', s3_key)['Body'].read()
            vehicles_obj = find_object('VEH', s3_key)['Body'].read()

            consumer_encoding = detect_encoding(customers_obj) 
            vehicle_encoding = detect_encoding(vehicles_obj)
            appointments_encoding = detect_encoding(appointments_obj)

            appointments_df = pd.read_csv(io.BytesIO(appointments_obj), delimiter=';', encoding=appointments_encoding, on_bad_lines='warn', dtype={'Dealer ID': 'string'})
            customers_df = pd.read_csv(io.BytesIO(customers_obj), delimiter=';', encoding=consumer_encoding, on_bad_lines='warn', dtype={'Dealer ID': 'string'})
            vehicles_df = pd.read_csv(io.BytesIO(vehicles_obj), delimiter=';', encoding=vehicle_encoding, on_bad_lines='warn', dtype={'Dealer ID': 'string'})

            dms_id = pd.Series(appointments_df.get('Dealer ID')).array[0]

            # Clean the customer and vehicle data using the unified function
            cleaned_customers_df = clean_data(customers_df, 'Dealer Customer No', [])
            cleaned_vehicles_df = clean_data(vehicles_df, 'Vin No', ['OEM Name', 'Model'])

            # Identify missing records in vehicle_sales_df compared to customers_df and vehicles_df
            valid_records_df, orphans_df = identify_and_separate_records(appointments_df, cleaned_customers_df, cleaned_vehicles_df)

            current_date = datetime.now()

            # Save the orphan records to an error file
            if not orphans_df.empty:
                unique_id = str(uuid4())
                error_file_key = f"quiter/error_files/service_appointment/{dealer_id}/{current_date.year}/{current_date.month}/{current_date.day}/{unique_id}_orphan_records.csv"
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