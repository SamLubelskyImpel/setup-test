from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from aws_lambda_powertools.utilities.batch import (
    BatchProcessor,
    EventType,
    process_partial_response,
)
import logging
import boto3
from os import environ
import requests
import json
from datetime import datetime, timezone


INVENTORY_BUCKET = environ.get("INVENTORY_BUCKET")
API_SECRET_KEY = environ.get("API_SECRET_KEY")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
s3_client = boto3.client("s3")
sm_client = boto3.client("secretsmanager")


def get_api_config():
    resp = sm_client.get_secret_value(SecretId=API_SECRET_KEY)
    secret = json.loads(resp.get("SecretString"))
    return secret.get("url"), secret.get("api_key")


def get_inventory(provider_dealer_id):
    url, api_key = get_api_config()
    params = {
        "ApiKey": api_key,
        "format": "json",
        "dealers": provider_dealer_id
    }
    resp = requests.get(url, params=params)

    if resp.status_code != 200:
        logger.error(f"ICC API returned {resp.status_code}")

    resp.raise_for_status()
    return resp.json()


def save_to_s3(data, provider_dealer_id):
    current_ts = datetime.now(tz=timezone.utc)
    date = current_ts.strftime("%Y/%m/%d/%H")
    key = f"raw/icc-api/{date}/{provider_dealer_id}_{current_ts.timestamp()}.json"
    s3_client.put_object(
        Bucket=INVENTORY_BUCKET,
        Key=key,
        Body=json.dumps(data)
    )
    logger.info(f"Saved object to {key}")


def record_handler(record: SQSRecord):
    logger.info(f"Record: {record}")

    try:
        body = record.json_body
        provider_dealer_id = body.get("provider_dealer_id")
        inventory = get_inventory(provider_dealer_id)
        save_to_s3(inventory, provider_dealer_id)
    except:
        logger.exception("Error occurred while processing the record")
        raise


def lambda_handler(event, context):
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
    except Exception:
        logger.exception("Error occurred while processing the event")
        raise
