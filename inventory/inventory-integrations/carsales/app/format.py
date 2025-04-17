import logging
import os
import boto3
from typing import Dict, Any
from types import FunctionType
from json import loads, dumps
import urllib.parse
from mappings import FIELD_MAPPINGS, get_nested_value
from unified_df import upload_unified_json


INVENTORY_BUCKET = os.environ.get('INVENTORY_BUCKET')

logger = logging.getLogger()
logger.setLevel(os.environ.get('LOGLEVEL', 'INFO').upper())
s3_client = boto3.client('s3')


def get_most_recent_vehicle_event(all_events: list[dict], vin: str) -> bool:
    vin_mapping = FIELD_MAPPINGS['inv_vehicle']['vin']
    vehicle_events = [e for e in all_events if vin_mapping(e) == vin]
    vehicle_events.sort(key=lambda x: x['timestamp'])
    return vehicle_events[-1]


def clean_vin_duplicated_events(all_events: dict[str, list]):
    result: dict[str, list] = {}
    for impel_dealer_id, events in all_events.items():
        deduped_events = []
        checked_vins = []

        for event in events:
            vin_mapping = FIELD_MAPPINGS['inv_vehicle']['vin']
            vin = vin_mapping(event)

            if vin not in checked_vins:
                most_recent = get_most_recent_vehicle_event(events, vin)
                checked_vins.append(vin)
                deduped_events.append(most_recent)

        result[impel_dealer_id] = deduped_events

    return result


def merge_events_from(keys: list[str]) -> dict[str, list]:
    merged: dict[str, list] = {}

    for key in keys:
        decoded_key = urllib.parse.unquote(key)
        split_key = decoded_key.split('/')
        impel_dealer_id = split_key[2]
        timestamp = split_key[-1].split('_')[0]

        obj = s3_client.get_object(Bucket=INVENTORY_BUCKET, Key=decoded_key)
        json_content = loads(obj['Body'].read())
        json_content['impel_dealer_id'] = impel_dealer_id
        json_content['timestamp'] = timestamp

        if impel_dealer_id not in merged:
            merged[impel_dealer_id] = []

        merged[impel_dealer_id].append(json_content)

    merged = clean_vin_duplicated_events(merged)

    return merged


def transform_to_unified(events_grouped: dict[str, list]) -> dict[str, list]:
    entries: dict[str, list] = {}

    for dealer_id, events in events_grouped.items():
        events_transformed = []

        for event in events:
            entry = {}

            for table, table_mapping in FIELD_MAPPINGS.items():
                entry[table] = {}
                for impel_field, carsales_field in table_mapping.items():
                    if isinstance(carsales_field, FunctionType):
                        entry[table][impel_field] = carsales_field(event)
                    else:
                        entry[table][impel_field] = get_nested_value(event, carsales_field)
            entry["inv_dealer_integration_partner"] = {}
            entry["inv_dealer_integration_partner"]["provider_dealer_id"] = dealer_id
            events_transformed.append(entry)

        entries[dealer_id] = events_transformed

    return entries


def lambda_handler(event: Dict, context: Any):
    logger.info(f'Event: {event}')

    try:
        keys = [
            loads(r['body'])['Records'][0]['s3']['object']['key']
            for r in event['Records']]
        logger.info(f'Processing {len(keys)} events: {keys}')
        vehicle_events = merge_events_from(keys)
        entries = transform_to_unified(vehicle_events)
        upload_unified_json(entries)
    except:
        logger.exception('Failure processing events')
        raise
