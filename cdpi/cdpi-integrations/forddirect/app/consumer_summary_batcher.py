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
from cdpi_orm.session_config import DBSession
from cdpi_orm.models.dealer_integration_partner import DealerIntegrationPartner
from cdpi_orm.models.integration_partner import IntegrationPartner


OUTPUT_BUCKET = os.getenv('OUTPUT_BUCKET')
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '1000'))
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

logger = logging.getLogger()
logger.setLevel(LOG_LEVEL)

s3_client = boto3.client('s3')


TARGET_COLUMNS = [
    "c_fddguid",
    "epm_sales_decile",
    "esm_service_decile",
    "ext_consumer_id"
]


def is_active_dealer(cdp_dealer_id):
    """Check if the dealer is active."""
    with DBSession() as session:
        db_dip = session.query(
            DealerIntegrationPartner
        ).join(
            IntegrationPartner, DealerIntegrationPartner.integration_partner_id == IntegrationPartner.id
        ).filter(
            DealerIntegrationPartner.cdp_dealer_id == cdp_dealer_id,
            IntegrationPartner.impel_integration_partner_name == "FORD_DIRECT",
            DealerIntegrationPartner.is_active.is_(True)
        ).first()

        return bool(db_dip)


def extract_columns(headers, row):
    """Extract only the target columns from the row."""
    row_dict = dict(zip(headers, row))
    if not row_dict.get('ext_consumer_id', '').replace('"', '').strip():
        return None

    # Extract only the target columns
    extracted_row = [row_dict.get(col) for col in TARGET_COLUMNS]
    return extracted_row


def filter_rows(bucket, key):
    """Filter only the rows with the target columns."""
    response = s3_client.get_object(Bucket=bucket, Key=key)

    txt_file = io.TextIOWrapper(response['Body'], encoding='utf-8')
    original_headers = []

    first_line = True
    for line in txt_file:
        line = line.strip()

        row = line.split('|^|')

        if first_line:
            original_headers = [i.lower() for i in row]
            first_line = False
            continue

        # Extract columns from the row
        extracted_row = extract_columns(original_headers, row)
        if not extracted_row:
            logger.info(f"Skipping row without Impel ID: {row}")
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


def record_handler(record: SQSRecord):
    """Process TXT file from S3 and split into batches."""
    logger.info(f"Record: {record}")
    try:
        event = loads(record["body"])
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        file_key = event['Records'][0]['s3']['object']['key']
        # Expected name: fd-raw/consumer_profile_summary/dealer_id/year/month/day/consumerprofilesummary_impel_00703_20240910130020.txt
        decoded_key = urllib.parse.unquote(file_key)

        dealer_id = decoded_key.split("/")[2]
        filename = decoded_key.split("/")[-1].split(".")[0]

        # Check if cdp dealer is active
        if not is_active_dealer(dealer_id):
            logger.warning(f"Skipping file processing for inactive dealer: {dealer_id}")
            return

        batch = [TARGET_COLUMNS]
        batch_count = 0

        for row in filter_rows(bucket_name, decoded_key):
            batch.append(row)

            if len(batch)-1 == BATCH_SIZE:
                upload_batch(batch, bucket_name, filename, dealer_id, batch_count)
                batch_count += 1
                batch = [TARGET_COLUMNS]

        # Process any remaining rows
        if len(batch) > 1:
            upload_batch(batch, bucket_name, filename, dealer_id, batch_count)

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise e


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
