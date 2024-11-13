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
        "type": "BodyType",
        "mileage": "Mileage",
        "make": "Make",
        "model": "Model",
        "year": "Year",
        "stock_num": "Stock",
        "new_or_used": "IsNew",
    },
    "inv_dealer_integration_partner": {
        "provider_dealer_id": "DealerID",
    },
    "inv_inventory": {
        "list_price": "ListPrice",
        "special_price": "SpecialPrice",
        "fuel_type": "FuelType",
        "exterior_color": "Exteriorcolor",
        "interior_color": "Interiorcolor",
        "doors": "DoorCount",
        "transmission": "Transmission",
        "photo_url": "PhotoURL",
        "comments": "DealerComments",
        "drive_train": "Drivetrain",
        "cylinders": "Cylinders",
        "body_style": "BodyStyle",
        "vin": "VIN",
        "interior_material": "Interiormaterial",
        "source_data_drive_train": "SourceDataDrivetrain",
        "trim": "Trim",
        "source_data_interior_material_description": "SourceDataInteriorMaterialDescription",
        "vdp": "VDP",
        "cost_price": "CostPrice",
        "source_data_transmission": "SourceDataTransmission",
        "source_data_transmission_speed": "SourceDataTransmissionSpeed",
        "transmission_speed": "TransmissionSpeed",
        "build_data": "BuildData",
        "highway_mpg": "HwyMPG",
        "city_mpg": "CityMPG",
        "engine": "Engine",
        "engine_displacement": "EngineDisplacement",
        "factory_certified": "FactoryCertified",
        "options": "OptionDescription",
        "priority_options": "PriorityOptions"
    }
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

        for table, table_mapping in FIELD_MAPPINGS.items():
            entry[table] = {}
            for impel_field, icc_field in table_mapping.items():
                if impel_field == "new_or_used":
                    entry[table][impel_field] = "Used" if row.get(icc_field, "") == "U" else "New"
                elif impel_field == "factory_certified":
                    entry[table][impel_field] = row.get(icc_field, "") == "C"
                elif impel_field == "cylinders":
                    entry[table][impel_field] = row.get(icc_field, "").replace(".", "")
                elif impel_field == "photo_url":
                    entry[table][impel_field] = row.get(icc_field, "").split("|")[0]
                elif impel_field == "options":
                    entry[table][impel_field] = [desc.strip()[:255] for desc in row.get("OptionDescription", "").split("|")] if row.get("OptionDescription") else []
                elif impel_field == "priority_options":
                    entry[table][impel_field] = [desc.strip()[:255] for desc in row.get("PriorityOptions", "").split("|")] if row.get("PriorityOptions") else []
                else:
                    entry[table][impel_field] = row.get(icc_field, None)

        entry = process_entry(entry, received_datetime, s3_uri)
        entries.append(entry)
    return entries


def process_entry(entry, received_datetime, source_s3_uri):
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
        entry['inv_inventory']['on_lot'] = True
        entry['inv_inventory']['received_datetime'] = received_datetime

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
