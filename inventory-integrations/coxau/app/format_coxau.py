import json
import logging
import os
import boto3
import csv
from datetime import datetime
from io import StringIO
from aws_lambda_powertools.utilities.batch import BatchProcessor, EventType, process_partial_response
import urllib.parse
from unified_df import upload_unified_json
from json import dumps

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOGLEVEL", "INFO").upper())
s3_client = boto3.client('s3')

FIELD_MAPPINGS = {
    "inv_vehicle": {
        "vin": "VIN",
        "oem_name": "Make",
        "type": "Body",
        "mileage": "Odometer",
        "make": "Make",
        "model": "Model",
        "year": "ManuYear",
        "stock_num": "StockNo",
        "new_or_used": "Condition",
    },
    "inv_dealer_integration_partner": {
        "provider_dealer_id": "DealerID",
    },
    "inv_inventory": {
        "list_price": "AdvertisedPrice",
        "special_price": "EGCPrice",
        "fuel_type": "FuelType",
        "exterior_color": "BodyColour",
        "interior_color": "TrimColour",
        "doors": "Doors",
        "seats": "Seats",
        "transmission": "Gearbox",
        "photo_url": "PhotoURL",
        "comments": "Comments",
        "drive_train": "DriveType",
        "cylinders": "Cylinders",
        "body_style": "Body",
        "series": "Series",
        "vin": "VIN",
        "interior_material": "TrimColour",
        "source_data_drive_train": "DriveType",
        "trim": "Badge",
        "source_data_interior_material_description": "TrimColour",
        "vdp": "VDP",
    },
    "inv_options.inv_options": {
        "option_description": "RedbookCode",
    },
}


def transform_csv_to_entries(csv_content, received_datetime, s3_uri):
    """
    Transform CSV content to a list of JSON-formatted entries.

    Args:
        csv_content (str): The contents of a CSV file.

    Returns:
        list: A list of dictionaries, where each dictionary is an entry ready to be converted to JSON.
    """
    csv_file = StringIO(csv_content)
    reader = csv.DictReader(csv_file)
    entries = []

    for row in reader:
        entry = {}
        options = []

        for table, table_mapping in FIELD_MAPPINGS.items():
            if table != "inv_options.inv_options":  # Handle options separately
                entry[table] = {}
                for impel_field, cox_au_field in table_mapping.items():
                    if cox_au_field == "Condition":
                        entry[table][impel_field] = "Used" if row.get(cox_au_field, "") == "Demo" else row.get(cox_au_field, None)
                    elif cox_au_field == "PhotoURL":
                        entry[table][impel_field] = row.get(cox_au_field, ",").split(',')[0]
                    elif cox_au_field == "AdvertisedPrice":
                        entry[table][impel_field] = row.get(cox_au_field) if row.get(cox_au_field) else row.get("DriveAwayPrice", None)
                    else:
                        entry[table][impel_field] = row.get(cox_au_field, None)
            else:
                # Split the RedbookCode string on pipe delimiter and skip the first entry
                option_descriptions = row.get('RedbookCode', '').split('|')[1:]  # Skip the Redbook code itself
                options = [{"inv_option|option_description": desc.strip(), "inv_option|is_priority": False}
                           for desc in option_descriptions if desc.strip()]

        entry = process_entry(entry, received_datetime, s3_uri, options)
        entries.append(entry)
    return entries


def process_entry(entry, received_datetime, source_s3_uri, options):
    """
    Process and possibly modify an entry before it's added to the final list

    Args:
        entry (dict): The entry to process.
        row (dict): The original row data from CSV.

    Returns:
        dict: The processed entry.
    """
    metadata = dumps(source_s3_uri)
    if 'inv_inventory' in entry:
        entry['inv_inventory']['metadata'] = metadata
        entry['inv_inventory']['region'] = 'AU'
        entry['inv_inventory']['on_lot'] = True
        entry['inv_inventory']['received_datetime'] = received_datetime

    entry['inv_options.inv_options'] = options if options else []
    return entry


def record_handler(record):
    """Process each record in the batch."""
    try:
        body = json.loads(record.body)
        bucket_name = body['Records'][0]['s3']['bucket']['name']
        file_key = body['Records'][0]['s3']['object']['key']
        decoded_key = urllib.parse.unquote(file_key)

        csv_object = s3_client.get_object(
            Bucket=bucket_name,
            Key=decoded_key
        )
        csv_content = csv_object['Body'].read().decode('utf-8')
        provider_dealer_id, received_time = decoded_key.split('/')[-1].rsplit('.')[0].rsplit('_', 1)
        received_datetime = datetime.fromtimestamp(int(received_time)).strftime('%Y-%m-%dT%H:%M:%SZ')
        logger.info(f"Provider dealer id: {provider_dealer_id}, Received time: {received_datetime}")

        entries = transform_csv_to_entries(csv_content, received_datetime, decoded_key)
        logger.info(f"Entries: {entries}")
        upload_unified_json(entries, provider_dealer_id)
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
