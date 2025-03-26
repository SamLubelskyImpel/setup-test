import logging
import os
import boto3
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response
)
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from json import dumps, loads
import urllib.parse
from datetime import datetime
from mappings import FIELD_MAPPINGS
from unified_df import upload_unified_json


INVENTORY_BUCKET = os.environ.get('INVENTORY_BUCKET')

logger = logging.getLogger()
logger.setLevel(os.environ.get('LOGLEVEL', 'INFO').upper())
s3_client = boto3.client('s3')


def transform_to_unified(json_content: dict, received_dt: str, decoded_key: str):
    entries = []

    for vehicle in json_content.get("Inventory", {}).get("listOfVehicles", []):
        transformed: dict = {}

        for table, table_mapping in FIELD_MAPPINGS.items():
            transformed[table] = {}
            for impel_field, icc_field in table_mapping.items():
                transformed[table][impel_field] = icc_field(vehicle)

        transformed['inv_inventory']['on_lot'] = True
        transformed['inv_inventory']['received_datetime'] = received_dt
        transformed['inv_inventory']['metadata'] = dumps({ "source_s3": decoded_key })

        entries.append(transformed)

    return entries


def record_handler(record: SQSRecord):
    logger.info(f"Record: {record}")

    try:
        body = record.json_body
        bucket_name = body['Records'][0]['s3']['bucket']['name']
        file_key = body['Records'][0]['s3']['object']['key']
        decoded_key = urllib.parse.unquote(file_key)

        json_object = s3_client.get_object(
            Bucket=bucket_name,
            Key=decoded_key
        )

        json_content = loads(json_object['Body'].read().decode('utf-8'))
        provider_dealer_id, received_ts = decoded_key.split("/")[-1].split("_")
        received_ts = ".".join(received_ts.split(".")[:-1])
        received_dt = datetime.fromtimestamp(float(received_ts)).strftime("%Y-%m-%dT%H:%M:%SZ")
        logger.info(f"Provider dealer id: {provider_dealer_id}, Received time: {received_dt}")

        entries = transform_to_unified(json_content, received_dt, decoded_key)
        upload_unified_json(entries, provider_dealer_id)
    except:
        logger.exception(f"Error transforming record to unified json")
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
    except:
        logger.exception(f"Error processing records")
        raise
