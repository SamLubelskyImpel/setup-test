import boto3
import csv
import io
import os
import logging
import urllib

OUTPUT_BUCKET = os.getenv('OUTPUT_BUCKET')
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '1000'))
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

logger = logging.getLogger()
logger.setLevel(LOG_LEVEL)

s3_client = boto3.client('s3')


TARGET_COLUMNS = [
    "c_fddguid",
    "d_pacode",
    "epm_sales_date",
    "epm_sales_decile",
    "esm_service_date",
    "esm_service_decile",
    "EXT_CONSUMER_ID"
]


def extract_columns(headers, row):
    """Extract only the target columns from the row."""
    row_dict = dict(zip(headers, row))
    if "EXT_CONSUMER_ID" not in row_dict:
        return None

    # Extract only the target columns
    extracted_row = [row_dict.get(col) for col in TARGET_COLUMNS]
    return extracted_row


def filter_rows(bucket, key):
    """Filter only the rows with the target columns."""
    response = s3_client.get_object(Bucket=bucket, Key=key)

    csv_file = io.TextIOWrapper(response['Body'], encoding='utf-8')
    csv_reader = csv.reader(csv_file, delimiter="|^|")

    original_headers = next(csv_reader)

    for row in csv_reader:
        extracted_row = extract_columns(original_headers, row)
        if not extracted_row:
            continue

        yield extracted_row


def upload_batch(batch, bucket, filename, dealer_id, batch_count):
    """Upload a batch to the output S3 bucket."""
    csv_buffer = io.StringIO()
    csv_writer = csv.writer(csv_buffer)
    csv_writer.writerows(batch)

    batch_s3_key = f'fd-processed/consumer_profile_summary/{dealer_id}/{filename}/batch_{batch_count}.csv'
    s3_client.put_object(Bucket=bucket, Key=batch_s3_key, Body=csv_buffer.getvalue())

    logger.info(f"Uploaded batch to {batch_s3_key}")


def lambda_handler(event, context):
    try:
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        file_key = event['Records'][0]['s3']['object']['key']
        # Expected name: fd-raw/consumer_profile_summary/dealer_id/consumerprofilesummary_impel_00703_20240910130020.txt
        decoded_key = urllib.parse.unquote(file_key)

        dealer_id = decoded_key.split("/")[2]
        filename = decoded_key.split("/")[-1].split(".")[0]

        # Check if cdp dealer is active??

        batch = [TARGET_COLUMNS]
        batch_count = 0

        for row in filter_rows(bucket_name, decoded_key):
            batch.append(row)

            if len(batch) >= BATCH_SIZE:
                upload_batch(batch, bucket_name, filename, dealer_id, batch_count)
                batch_count += 1
                batch = [TARGET_COLUMNS]

        # Process any remaining rows
        if len(batch) > 1:
            upload_batch(batch, bucket_name, filename, dealer_id, batch_count)

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise e
