import boto3
import csv
import io
import os
import logging
import urllib
from json import loads
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)

BATCH_SIZE = int(os.getenv('BATCH_SIZE', '1000'))
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

logger = logging.getLogger()
logger.setLevel(LOG_LEVEL)

s3_client = boto3.client('s3')


def process_batch(batch, bucket, filename, sfdc_account_id, batch_count):
    """Process a batch of CSV rows"""
    csv_buffer = io.StringIO()
    csv_writer = csv.writer(csv_buffer)

    csv_writer.writerows(batch)

    # Example output key
    # customer-inbound-processed/0010a00001e7M7dAAE/0010a00001e7M7dAAE_2024-09-24T14_00_25Z/batch_0.csv
    batch_s3_key = f'customer-inbound-processed/{sfdc_account_id}/{filename}/batch_{batch_count}.csv'
    s3_client.put_object(Bucket=bucket, Key=batch_s3_key, Body=csv_buffer.getvalue())

    logger.info(f'Uploading batch to S3: {batch_s3_key}')


def record_handler(record: SQSRecord):
    """Process CSV file from S3 and split into batches"""
    logger.info(f"Record: {record}")
    try:
        message = loads(record["body"])
        bucket_name = message["detail"]["bucket"]["name"]
        file_key = message["detail"]["object"]["key"]
        decoded_key = urllib.parse.unquote(file_key)

        # Expected name: consumer-inbound/product/0010a00001e7M7dAAE_2024-09-24T14_00_25Z.csv
        filename = decoded_key.split("/")[-1].split(".")[0]
        sfdc_account_id = filename.split("_")[1]

        csv_file = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        csv_object = csv_file['Body'].read().decode('utf-8')

        csv_reader = csv.reader(io.StringIO(csv_object))
        headers = next(csv_reader)  # Assuming first row is headers

        batch = [headers]
        batch_count = 0

        for row in csv_reader:
            batch.append(row)

            if len(batch) == BATCH_SIZE:
                process_batch(batch, bucket_name, filename, sfdc_account_id, batch_count)
                batch_count += 1
                batch = [headers]

        # Process any remaining rows
        if batch:
            process_batch(batch, bucket_name, filename, sfdc_account_id, batch_count)

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
