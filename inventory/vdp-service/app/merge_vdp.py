"""Merge Inventory and VDP CSV data."""

import csv
from datetime import datetime, timezone
from io import StringIO
import logging
from os import environ
from json import loads, dumps
from typing import Any

import boto3
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord

from rds_instance import get_integration_partner_metadata

INVENTORY_BUCKET = environ["INVENTORY_BUCKET"]

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

s3_client = boto3.client("s3")


def upload_to_s3(csv_content, integration_partner_id, provider_dealer_id, modification_time):
    """Upload files to S3."""
    format_string = '%Y/%m/%d/%H'
    date_key = datetime.utcnow().strftime(format_string)
    s3_file_name = f"{provider_dealer_id}_{modification_time}.csv"

    s3_key = f"raw/{integration_partner_id}/{date_key}/{s3_file_name}"
    s3_client.put_object(
        Bucket=INVENTORY_BUCKET,
        Key=s3_key,
        Body=csv_content
    )

    logger.info(f"File {s3_file_name} uploaded to S3.")


def download_from_s3(s3_key):
    """Download files from S3."""
    try:
        csv_object = s3_client.get_object(
            Bucket=INVENTORY_BUCKET,
            Key=s3_key
        )
        logger.info(f"File {s3_key} downloaded from S3.")
        csv_content = csv_object['Body'].read().decode('utf-8')
        return csv_content

    except Exception as e:
        logger.error(f"Error downloading file from S3: {e}")
        return None


def merge_csv_files(inv_csv_content, vdp_csv_content):
    """Merge the inventory and VDP CSV files."""
    inv_csv_file = StringIO(inv_csv_content)
    inv_reader = csv.DictReader(inv_csv_file)

    vdp_csv_file = StringIO(vdp_csv_content)
    vdp_reader = csv.DictReader(vdp_csv_file)

    # Read the inventory data into a list of dictionaries
    inv_rows = [row for row in inv_reader]

    # Create dictionaries for quick lookup by VIN and StockNo in a single pass
    vdp_rows_vin = {}
    vdp_rows_stock_no = {}
    for row in vdp_reader:
        if 'VIN' in row and row['VIN']:
            vdp_rows_vin[row['VIN']] = row
        if 'StockNo' in row and row['StockNo']:
            vdp_rows_stock_no[row['StockNo']] = row

    # Ensure all relevant columns are present in the inventory rows
    fieldnames = inv_reader.fieldnames
    if 'PhotoURL' not in fieldnames:
        fieldnames.append('PhotoURL')
    if 'VDP' not in fieldnames:
        fieldnames.append('VDP')

    # Update the inventory rows with data from the VDP rows
    for inv_row in inv_rows:
        vdp_row = None
        if inv_row.get('VIN') in vdp_rows_vin:
            vdp_row = vdp_rows_vin[inv_row['VIN']]
        elif inv_row.get('StockNo') in vdp_rows_stock_no:
            vdp_row = vdp_rows_stock_no[inv_row['StockNo']]

        if vdp_row:
            inv_row['PhotoURL'] = vdp_row.get('SRP IMAGE URL', inv_row.get('PhotoURL', ''))
            inv_row['VDP'] = vdp_row.get('VDP URL', inv_row.get('VDP', ''))

    # Write the updated inventory data to a new CSV
    combined_csv = StringIO()
    writer = csv.DictWriter(combined_csv, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(inv_rows)

    return combined_csv.getvalue()


def record_handler(record):
    """Process each record in the batch."""
    logger.info(record)
    try:
        body = loads(record.body)
        inv_s3_key = body['Records'][0]['s3']['object']['key']

        integration_partner_id = inv_s3_key.split('/')[1]

        metadata = get_integration_partner_metadata(integration_partner_id)
        logger.info(f"Integration partner metadata: {metadata}")

        if not metadata.get('vdp_merge_service', False):
            logger.info(f"VDP merge service not enabled for integration partner: {integration_partner_id}")
            return

        provider_dealer_id = inv_s3_key.split('/')[2]
        logger.info(f"Integration partner id: {integration_partner_id}, Provider dealer id: {provider_dealer_id}")

        # Get inventory file
        inv_csv_content = download_from_s3(inv_s3_key)

        if not inv_csv_content:
            logger.error(f"Error downloading inventory file from S3: {inv_s3_key}")
            raise Exception(f"Error downloading inventory file from S3: {inv_s3_key}")

        # Get VDP file
        vdp_s3_key = f"vdp/{provider_dealer_id}.csv"
        vdp_csv_content = download_from_s3(vdp_s3_key)

        # Merge the files
        if vdp_csv_content:
            combined_csv_content = merge_csv_files(inv_csv_content, vdp_csv_content)
        else:
            logger.warning(f"VDP file not found: {vdp_s3_key}, skipping merge.")
            combined_csv_content = inv_csv_content

        # Upload the merged file to S3
        modification_time = int(datetime.now(timezone.utc).timestamp())
        upload_to_s3(combined_csv_content, integration_partner_id, provider_dealer_id, modification_time)
    except Exception as e:
        logger.error(f"Error merging files - {record}: {e}")
        raise


def lambda_handler(event: Any, context: Any) -> Any:
    """Download files from the SFTP server and upload to S3."""
    logger.info(f"Event: {event}")

    try:
        processor = BatchProcessor(event_type=EventType.SQS)
        result = process_partial_response(
            event=event,
            record_handler=record_handler,
            processor=processor,
            context=context,
        )
        return result

    except Exception:
        logger.exception("Error occurred while processing the event.")
        raise
