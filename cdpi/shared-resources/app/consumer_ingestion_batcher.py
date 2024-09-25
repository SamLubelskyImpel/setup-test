import boto3
import csv
import io
import os
import logging

BATCH_SIZE = int(os.getenv('BATCH_SIZE', '1000'))
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

logger = logging.getLogger()
logger.setLevel(LOG_LEVEL)

s3_client = boto3.client('s3')


def lambda_handler(event, context):
    """Process CSV file from S3 and split into batches"""
    logger.info(f'Event: {event}')
    try:
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        file_key = event['Records'][0]['s3']['object']['key']

        csv_file = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        csv_object = csv_file['Body'].read().decode('utf-8')

        # Read CSV data
        csv_reader = csv.reader(io.StringIO(csv_object))
        headers = next(csv_reader)  # Assuming first row is headers

        batch = []
        batch_count = 0

        # Process rows in the CSV
        for row in csv_reader:
            batch.append(row)

            if len(batch) == BATCH_SIZE:
                process_batch(batch, bucket_name, file_key, batch_count, headers)
                batch_count += 1
                batch = []  # Clear batch after processing

        # Process any remaining rows
        if batch:
            process_batch(batch, bucket_name, file_key, batch_count, headers)

    except Exception as e:
        logger.error(f'Error: {e}')
        raise


def process_batch(batch, original_bucket, original_key, batch_count, headers):
    """Process a batch of CSV rows"""
    batch_content = [headers] + batch  # Add headers to each batch
    csv_buffer = io.StringIO()
    csv_writer = csv.writer(csv_buffer)

    for row in batch_content:
        csv_writer.writerow(row)

    batch_data = csv_buffer.getvalue()

    s3_key = original_key.split("/", 1)[1].split(".")[0]
    filename = original_key.split("/")[-1].split(".")[0]

    batch_s3_key = f'customer-inbound-batches/{s3_key}-batch/{filename}_{batch_count}.csv'
    s3_client.put_object(Bucket=original_bucket, Key=batch_s3_key, Body=batch_data)

    logger.info(f'Uploading batch to S3: {batch_s3_key}')
    return
