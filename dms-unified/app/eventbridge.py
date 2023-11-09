""" Code for interacting with eventbridge """
import logging
from json import dumps
from os import environ

import boto3

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())
ENVIRONMENT = environ.get("ENVIRONMENT", "test")
IS_PROD = ENVIRONMENT == "prod"
EVENT_BUS = environ.get("EVENT_BUS", "unified-dms-bus-test")
client = boto3.client("events")


def notify_event_bus(detail):
    """Put event onto event bus."""
    try:
        event = {
            "Source": f"unified-dms-insertions-{'prod' if IS_PROD else 'test'}",
            "DetailType": "object",
            "Detail": dumps(detail),
            "EventBusName": EVENT_BUS,
        }
        response = client.put_events(Entries=[event])
        if response and response.get("FailedEntryCount", 0) > 0:
            logging.error(
                f"Failed to put these events: {dumps(detail)} with response: {response}"
            )
    except Exception as e:
        logging.exception(f"An error occurred while putting event: {e}")
