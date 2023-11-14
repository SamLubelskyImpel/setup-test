"""Format raw tekion data to unified format."""
import logging
import urllib.parse
from json import dumps, loads
from os import environ

import boto3
from unified_df import upload_unified_json

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
ENVIRONMENT = environ.get("ENVIRONMENT", "test")
REGION = environ.get("REGION", "us-east-1")
IS_PROD = ENVIRONMENT == "prod"
INTEGRATIONS_BUCKET = f"integrations-{REGION}-{'prod' if IS_PROD else 'test'}"
s3_client = boto3.client("s3")


def parse_json_to_entries(json_data):
    """Format tekion data to unified format."""
    entries = []
    for entry in json_data:
        db_dealer_integration_partner = {}
        db_vehicle_sale = {}
        db_vehicle = {}
        db_consumer = {}
        db_service_contracts = []

        db_dealer_integration_partner["dms_id"] = entry.get("dms_id")

        entry = {
            "dealer_integration_partner": db_dealer_integration_partner,
            "vehicle_sale": db_vehicle_sale,
            "vehicle": db_vehicle,
            "consumer": db_consumer,
            "service_contracts.service_contracts": db_service_contracts,
        }
        entries.append(entry)
    return entries, None


def lambda_handler(event, context):
    """Transform tekion deals files."""
    try:
        for record in event["Records"]:
            message = loads(record["body"])
            logger.info(f"Message of {message}")
            for s3_record in message["Records"]:
                bucket = s3_record["s3"]["bucket"]["name"]
                key = s3_record["s3"]["object"]["key"]
                decoded_key = urllib.parse.unquote(key)
                response = s3_client.get_object(Bucket=bucket, Key=decoded_key)
                json_data= loads(response["Body"].read())
                entries, dms_id = parse_json_to_entries(json_data)
                if not dms_id:
                    raise RuntimeError("No dms_id found")
                upload_unified_json(entries, "fi_closed_deal", decoded_key, dms_id)
    except Exception:
        logger.exception(f"Error transforming tekion deals file {event}")
        raise
