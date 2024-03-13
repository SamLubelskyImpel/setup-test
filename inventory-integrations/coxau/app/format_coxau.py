import json
import logging
import os
import boto3
from datetime import datetime
import csv
from io import StringIO
from aws_lambda_powertools.utilities.data_classes import SQSEvent
from aws_lambda_powertools.utilities.batch import BatchProcessor, EventType, process_partial_response
from unified_df import upload_unified_json
from json import dumps, loads
import re

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

        for table, table_mapping in mapping.items():
            entry[table] = {}
            for impel_field, cox_au_field in table_mapping.items():
                entry[table][impel_field] = row.get(cox_au_field, None)
        entry = process_entry(entry, row, s3_uri)
        entries.append(entry)
    return entries

def process_entry(entry, row, source_s3_uri):
    """
    Process and possibly modify an entry before it's added to the final list.

    Args:
        entry (dict): The entry to process.
        row (dict): The original row data from CSV.

    Returns:
        dict: The processed entry.
    """
    # db_metadata = {
    #     "Region": REGION,
    #     "PartitionYear": source_s3_uri.split("/")[2],
    #     "PartitionMonth": source_s3_uri.split("/")[3],
    #     "PartitionDate": source_s3_uri.split("/")[4],
    #     "s3_url": s3_uri,
    # }
    metadata = dumps(source_s3_uri)
    if 'inv_inventory' in entry:
        entry['inv_inventory']['metadata'] = metadata
        entry['inv_inventory']['region'] = 'AU'
        entry['inv_inventory']['on_lot'] = 'True'


    # sample file name: raw/coxau/2024/03/06/22/13728_example3_22:23:00.csv
    # Extract the timestamp from the S3 URI using a regular expression
    # timestamp_match = re.search(r'(\d{4})/(\d{2})/(\d{2})/(\d{2})/.*_(\d{2}:\d{2}:\d{2})\.csv', source_s3_uri)
    # if timestamp_match:
    #     # Format the timestamp in ISO 8601 format
    #     received_datetime = f"{timestamp_match.group(1)}-{timestamp_match.group(2)}-{timestamp_match.group(3)}T{timestamp_match.group(4)}:{timestamp_match.group(5)}"

    #     # Update the 'received_datetime' field in the 'inv_inventory' part of the entry
    #     if 'inv_inventory' in entry:
    #         entry['inv_inventory']['received_datetime'] = received_datetime
        
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
            "trim": "TrimColour",
            "source_data_interior_material_description": "Badge",
            # "received_datetime": "", #timestamp in uploaded filename
        },
    }
    try:
        body = json.loads(record.body)
        logger.info(body)
        bucket_name = body['bucket']
        file_key = body['key']
        csv_object = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        csv_content = csv_object['Body'].read().decode('utf-8')
        entries = transform_csv_to_entries(csv_content, mappings, file_key)
        upload_unified_json(entries, file_key)
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
