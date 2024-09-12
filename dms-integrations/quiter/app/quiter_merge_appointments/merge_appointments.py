"""Merging logic for “Appointments”, “Customers” and “Vehicles” from Quiter."""

import boto3
import pandas as pd
import os
import logging
import re 
import io
import chardet

from json import loads
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


def merge_files(main_df, customers_df, vehicles_df):
    """Merge appointments, customers, and vehicles dataframes on Consumer ID and Vin No."""
    merged_df = pd.merge(main_df, customers_df, left_on=["Consumer ID"], right_on=["Dealer Customer No"], how="inner")
    merged_df = pd.merge(merged_df, vehicles_df, on=["Vin No"], how="inner")
    return merged_df


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

            appointments_df = pd.read_csv(io.BytesIO(appointments_obj), delimiter=';', encoding=appointments_encoding, on_bad_lines='warn')
            customers_df = pd.read_csv(io.BytesIO(customers_obj), delimiter=';', encoding=consumer_encoding, on_bad_lines='warn')
            vehicles_df = pd.read_csv(io.BytesIO(vehicles_obj), delimiter=';', encoding=vehicle_encoding, on_bad_lines='warn')

            # if len(appointments_df) != len(customers_df) or len(customers_df) != len(
            #     vehicles_df
            # ):
            #     raise ValueError(
            #         "Mismatch in the number of customers, vehicles, or appointments."
            #     )

            # merged_df = merge_files(appointments_df, customers_df, vehicles_df)

            # current_date = datetime.now()
            # csv_buffer = merged_df.to_csv(index=False)
            # s3_key = f"quiter/service_appointment/{current_date.year}/{current_date.month}/{current_date.day}/merged_appointments_{str(uuid4())}"
            # S3_CLIENT.put_object(Bucket=BUCKET_NAME, Key=s3_key, Body=csv_buffer)

            # logger.info(f"Merged file saved to {s3_key} in the raw zone.")

            # queue_url = os.environ("FORMAT_APPOINTMENT_QUEUE")
            # data = {
            #     "dms_id": dealer_id,
            #     "s3_key": s3_key,
            # }
            # response = SQS_CLIENT.send_message(
            #     QueueUrl=queue_url,
            #     MessageBody=dumps(data)
            # )
            # # Verify that the message was sent successfully by checking the response
            # # TODO: Delete after testing
            # if 'MessageId' in response:
            #     logging.info(f"Message sent successfully to {queue_url}, MessageId: {response['MessageId']}")
            # else:
            #     logging.error(f"Failed to send message to {queue_url}")

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