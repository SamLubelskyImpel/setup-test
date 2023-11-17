"""Transform raw dealerpeak data to the unified format"""

import boto3
import logging
from os import environ
from json import loads
from typing import Any


logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


s3_client = boto3.client("s3")


def extract_contact_information(item_name, item, db_entity):
    """Extract contact information from the dealerpeak json data."""

    db_entity[f'crm_{item_name}_id'] = item.get('userID', None)
    db_entity["first_name"] = item.get('givenName', None)
    db_entity["last_name"] = item.get('familyName', None)
    emails = item.get('contactInformation', {}).get('emails', None)
    db_entity["email"] = emails[0].get('address', None) if emails else None
    phone_numbers = item.get('contactInformation', {}).get('phoneNumbers', [])
    db_entity["phone"] = phone_numbers[0].get("number") if phone_numbers else None

    # Iterate to find a mobile or cell phone number
    for phone in phone_numbers:
        if phone.get("type", "").lower() in ["mobile", "cell"]:
            db_entity["phone"] = phone.get('number')
            break

    if item == 'consumer':
        addresses = item.get('contactInformation', {}).get('addresses', [])
        address = addresses[0] if addresses else None

        if address:
            line1 = address.get('line1', '')
            line2 = address.get('line2', '')
            db_entity["address"] = f"{line1} {line2}".strip() if line1 or line2 else None
            db_entity["city"] = address.get('city', None)
            db_entity["postal_code"] = address.get('postcode', None)


def parse_json_to_entries(json_data) -> Any:
    """Format dealerpeak json data to unified format."""
    entries = []
    try:
        for item in json_data:
            db_lead = {}
            db_vehicles = []
            db_consumer = {}
            db_salesperson = {}

            db_lead["lead_id"] = item.get('leadID', None)
            db_lead["lead_ts"] = item.get('dateCreated', None)
            db_lead["status"] = item.get('status', {}).get('status', None)
            db_lead["comment"] = item.get('comment', {}).get('note', None)
            db_lead["source_channel"] = item.get('source', {}).get('source', None)

            vehicles = item.get('vehiclesOfInterest', [])
            for vehicle in vehicles:
                db_vehicle = {}
                db_vehicle["crm_vehicle_id"] = vehicle.get('carID', None)
                db_vehicles.append(db_vehicle)

            db_lead["vehicles_of_interest"] = db_vehicles

            consumer = item.get('customer', None)
            extract_contact_information('consumer', consumer, db_consumer)

            salesperson = item.get('agent', None)
            extract_contact_information('salesperson', salesperson, db_salesperson)

            entry = {
                "lead": db_lead,
                "consumer": db_consumer,
                "salesperson": db_salesperson,
            }
            entries.append(entry)
        return entries
    except Exception as e:
        logger.error(f"Error processing record: {e}")
        raise


def lambda_handler(event: Any, context: Any) -> Any:
    """Transform raw dealerpeak data to the unified format."""
    logger.info(f"Event: {event}")
    try:
        for record in event["Records"]:
            message = loads(record["body"])
            bucket = message["detail"]["bucket"]["name"]
            key = message["detail"]["object"]["key"]

            # Retrieve the object from S3
            response = s3_client.get_object(Bucket=bucket, Key=key)
            content = response['Body'].read()
            json_data = loads(content)

            logger.info(f"Raw data: {json_data}")

            entries = parse_json_to_entries(json_data)

            logger.info(f"Processed entries: {entries}")

            # send it to the /upload endpoint

    except Exception:
        logger.exception(f"Error transforming dealerpeak file {event}")
        raise
