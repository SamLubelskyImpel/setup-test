import boto3
from os import environ
import logging
import urllib.parse
import csv
from json import loads
from io import StringIO
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)
import requests

LOG_LEVEL = environ.get('LOG_LEVEL', 'INFO')
DMS_API_DOMAIN = environ.get('DMS_API_DOMAIN')
ENVIRONMENT = environ.get('ENVIRONMENT')
SECRET_KEY = environ.get('SECRET_KEY')

logger = logging.getLogger()
logger.setLevel(LOG_LEVEL)

s3_client = boto3.client('s3')
sm_client = boto3.client('secretsmanager')

class EmptyFileError(Exception):
    pass

class LeadNotFoundError(Exception):
    pass

def get_secret(secret_name, secret_key):
    """Get secret from Secrets Manager."""
    secret = sm_client.get_secret_value(
        SecretId=f"{'prod' if ENVIRONMENT == 'prod' else 'test'}/{secret_name}"
    )
    secret = loads(secret["SecretString"])[str(secret_key)]
    secret_data = loads(secret)

    return secret_data

def make_dms_api_request(url: str, method: str, dms_api_key: str, data=None):
    """Generic helper function to make DMS API requests."""

    headers = {
        'client_id': SECRET_KEY,
        'x_api_key': dms_api_key
    }
    response = requests.request(method, url, headers=headers, json=data)
    return response

def parse(csv_object):
    """Parse CSV object and extract entries"""
    csv_reader = csv.DictReader(StringIO(csv_object))
    fieldnames = csv_reader.fieldnames
    output_stream = StringIO()
    writer = csv.DictWriter(output_stream, fieldnames=fieldnames)
    writer.writeheader()

    # Check if the CSV has a row with values
    rows = list(csv_reader)
    if not rows:
        logger.warning('No rows found in the CSV')
        raise EmptyFileError

    for row in rows:
        if row.get("dms_vendor_name", "").lower() == 'shared layer':
            # Waiting for CE-2121 Api endpoint to be available

            # consumer_id = row["dms_consumer_id"]

            # logger.info(f"Updating vendor name for lead with DMS Consumer ID {consumer_id}")

            # url = f'https://{DMS_API_DOMAIN}/consumer/v1'

            # dms_api_key = get_secret("DmsDataService", SECRET_KEY)["api_key"] # TODO: Confirm credentials
            # response = make_dms_api_request(url, "GET", dms_api_key)

            # if response.status_code != 200:
                # raise LeadNotFoundError(f"Consumer with DMS Consumer ID {consumer_id} not found. {response.text}")

            # logger.info(f"CRM API responded with: {response.status_code} for lead with CRM Lead ID {consumer_id}")

            # vendor_name = response.json().get("dms_vendor_name")
            # dms_consumer_id = response.json().get("dms_consumer_id")
            vendor_name = "mocked vendor name"
            dms_consumer_id = "123456789"


            row["dms_vendor_name"] = vendor_name
            row["dms_consumer_id"] = dms_consumer_id

        writer.writerow(row)

    updated_csv = output_stream.getvalue()

    return updated_csv

def record_handler(record: SQSRecord):
    """Process CSV file from S3 and update the vendor name"""
    logger.info(f"Record: {record}")
    try:
        event = loads(record["body"])
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        file_key = event['Records'][0]['s3']['object']['key']
        decoded_key = urllib.parse.unquote(file_key)

        parts = decoded_key.split('/')
        product_name = parts[1]
        filename = parts[2]
        batch_file = parts[-1]

        csv_file = s3_client.get_object(
            Bucket=bucket_name,
            Key=decoded_key
        )
        csv_object = csv_file['Body'].read().decode('utf-8')

        updated_csv = parse(csv_object)

        batch_s3_key = f"customer-inbound-processed/{product_name}/{filename}/{batch_file}"
        s3_client.put_object(Bucket=bucket_name, Key=batch_s3_key, Body=updated_csv)

        logger.info(f"Updated file {decoded_key}\nUploaded to S3 bucket: {bucket_name}/{batch_s3_key}")

    except EmptyFileError:
        return
    except Exception as e:
        logger.error(f'Error: {e}')
        raise


def lambda_handler(event, context):
    logger.info(f"Event: {event}")

    try:
        processor = BatchProcessor(event_type=EventType.SQS)
        result = process_partial_response(
            event=event,
            record_handler=record_handler,
            processor=processor,
            context=context
        )
        return result
    except Exception as e:
        logger.error(f"Error processing batch: {e}")
        raise
