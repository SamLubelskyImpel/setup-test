import json
import logging
import os
import boto3
import csv
from datetime import datetime
from io import StringIO
from aws_lambda_powertools.utilities.batch import BatchProcessor, EventType, process_partial_response

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOGLEVEL", "INFO").upper())
s3_client = boto3.client('s3')

INVENTORY_BUCKET = os.environ["INVENTORY_BUCKET"]


class MissingVDPException(Exception):
    pass


def upload_to_s3(csv_content, provider_dealer_id, modification_time):
    """Upload files to S3."""
    format_string = '%Y/%m/%d/%H'
    date_key = datetime.utcnow().strftime(format_string)
    s3_file_name = f"{provider_dealer_id}_{modification_time}.csv"

    s3_key = f"raw/coxau/{date_key}/{s3_file_name}"
    s3_client.put_object(
        Bucket=INVENTORY_BUCKET,
        Key=s3_key,
        Body=csv_content
    )

    logger.info(f"File {s3_file_name} uploaded to S3.")


def merge_csv_files(inv_csv_content, vdp_csv_content):
    inv_csv_file = StringIO(inv_csv_content)
    inv_reader = csv.DictReader(inv_csv_file)

    vdp_csv_file = StringIO(vdp_csv_content)
    vdp_reader = csv.DictReader(vdp_csv_file)

    # Read the inventory and VDP data into lists of dictionaries
    inv_rows = [row for row in inv_reader]
    vdp_rows = {row['VIN']: row for row in vdp_reader}

    # Ensure all relevant columns are present in the inventory rows
    fieldnames = inv_reader.fieldnames
    if 'PhotoURL' not in fieldnames:
        fieldnames.append('PhotoURL')
    if 'VDP' not in fieldnames:
        fieldnames.append('VDP')

    # Update the inventory rows with data from the VDP rows
    for inv_row in inv_rows:
        vin = inv_row['VIN']
        if vin in vdp_rows:
            vdp_row = vdp_rows[vin]
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
    try:
        body = json.loads(record.body)

        provider_dealer_id = body['provider_dealer_id']
        modificiation_time = body['modification_time']
        logger.info(f"Provider dealer id: {provider_dealer_id}, Modification time: {modificiation_time}")

        s3_base_path = f"landing-zone/coxau/{provider_dealer_id}/"

        # Get inventory file
        try:
            inv_csv_object = s3_client.get_object(
                Bucket=INVENTORY_BUCKET,
                Key=s3_base_path + "inventory.csv"
            )
            inv_csv_content = inv_csv_object['Body'].read().decode('utf-8')
        except Exception as e:
            logger.info(f"Inventory file not found: {e}. Not processing VDP file.")
            return

        # Get VDP file
        try:
            try:
                vdp_csv_object = s3_client.get_object(
                    Bucket=INVENTORY_BUCKET,
                    Key=s3_base_path + "vdp.csv"
                )
                vdp_csv_content = vdp_csv_object['Body'].read().decode('utf-8')
            except Exception as e:
                logger.info(f"VDP file not found: {e}. Only processing inventory file.")
                raise MissingVDPException("VDP file not found")

            combined_csv_content = merge_csv_files(inv_csv_content, vdp_csv_content)

        except MissingVDPException:
            logger.info("Only inventory file found. Skipping merge.")
            combined_csv_content = inv_csv_content

        upload_to_s3(combined_csv_content, provider_dealer_id, modificiation_time)

    except Exception as e:
        logger.error(f"Error transforming csv to json - {record}: {e}")
        raise


def lambda_handler(event, context):
    """Lambda function entry point for processing SQS messages."""
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
        logger.error(f"Error processing records: {e}")
        raise
