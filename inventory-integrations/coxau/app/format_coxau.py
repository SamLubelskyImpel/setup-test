import json
import logging
import os
import boto3
import csv
import re
from io import StringIO
from aws_lambda_powertools.utilities.batch import BatchProcessor, EventType, process_partial_response
import urllib.parse
from unified_df import upload_unified_json
from json import dumps

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOGLEVEL", "INFO").upper())
s3_client = boto3.client('s3')



def transform_csv_to_entries(csv_content, mapping, s3_uri):
    """
    Transform CSV content to a list of JSON-formatted entries.

    Args:
        csv_content (str): The contents of a CSV file.
        mapping (dict): A dictionary defining how CSV column names map to database field names.

    Returns:
        list: A list of dictionaries, where each dictionary is an entry ready to be converted to JSON.
    """
    csv_file = StringIO(csv_content)
    reader = csv.DictReader(csv_file)
    entries = []

    for row in reader:
        entry = {}
        options = [] 

        for table, table_mapping in mapping.items():
            if table != "inv_options.inv_options":  # Handle options separately
                entry[table] = {}
                for impel_field, cox_au_field in table_mapping.items():
                    entry[table][impel_field] = row.get(cox_au_field, None)
            else:
                 # Split the RedbookCode string on pipe delimiter and skip the first entry
                option_descriptions = row.get('RedbookCode', '').split('|')[1:]  # Skip the Redbook code itself
                options = [{"inv_option|option_description": desc.strip(), "inv_option|is_priority": False}
                           for desc in option_descriptions if desc.strip()]

        entry = process_entry(entry, s3_uri, options)
        entries.append(entry)
    return entries
    

def process_entry(entry, source_s3_uri, options):
    """
    Process and possibly modify an entry before it's added to the final list

    Args:
        entry (dict): The entry to process.
        row (dict): The original row data from CSV.

    Returns:
        dict: The processed entry.
    """
    # Assumes the filename is the last part of the source_s3_uri after a slash (/)
    filename = source_s3_uri.split('/')[-1]
    datetime_match = re.search(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z', filename)
    if datetime_match:
        received_datetime = datetime_match.group(0)
    else:
        #  We need to ensure that we always have a datetime since the load layer depends on it.
        received_datetime = "Unknown"

    metadata = dumps(source_s3_uri)
    if 'inv_inventory' in entry:
        entry['inv_inventory']['metadata'] = metadata
        entry['inv_inventory']['region'] = 'AU'
        entry['inv_inventory']['on_lot'] = True
        entry['inv_inventory']['received_datetime'] = received_datetime

    if options:
        entry['inv_options.inv_options'] = options
    return entry

def record_handler(record):
    """Process each record in the batch."""
    mappings = {
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
        },
        "inv_options.inv_options": {
            "option_description": "RedbookCode",
        },
    }
    try:
        body = json.loads(record.body)
        bucket_name = body['Records'][0]['s3']['bucket']['name']
        file_key = body['Records'][0]['s3']['object']['key']
        decoded_key = urllib.parse.unquote(file_key)
        csv_object = s3_client.get_object(Bucket=bucket_name, Key=decoded_key)
        csv_content = csv_object['Body'].read().decode('utf-8')
        entries = transform_csv_to_entries(csv_content, mappings, decoded_key)
        upload_unified_json(entries, decoded_key)
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
