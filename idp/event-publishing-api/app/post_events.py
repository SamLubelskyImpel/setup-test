"""Publish events to SNS.

Example SNS event:
{
    "event_bus_id": event_id,
    "timestamp": "2024-11-04T14:23:15.123Z",
    "source": "my-service-name",
    "payload": event_json
}
"""

import logging
from os import environ
from json import dumps, loads
from typing import Any
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError

ENVIRONMENT = environ.get("ENVIRONMENT")
EVENT_BUS_NAME = environ.get("EVENT_BUS_NAME")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())

events_client = boto3.client('events')


def lambda_handler(event: Any, context: Any) -> Any:
    """Post events."""
    logger.info(f"Event: {event}")

    try:
        body = loads(event["body"])

        try:
            event_json = body["event_json"]
            event_source = body["event_source"]

            if not isinstance(event_json, list):
                raise TypeError("event_json must be an array.")
            if not all(isinstance(event, dict) for event in event_json):
                raise TypeError("event_json must contain only objects.")
            if not isinstance(event_source, str):
                raise TypeError("event_source must be a string.")
        except KeyError as e:
            logger.exception(f"Missing required fields: {e}")
            return {
                "statusCode": 400,
                "body": dumps({"error": f"Missing required fields: {e}"}),
            }
        except TypeError as e:
            logger.exception(f"Invalid data type: {e}")
            return {
                "statusCode": 400,
                "body": dumps({"error": f"Invalid data type: {e}"}),
            }

        if not event_json:
            return {
                "statusCode": 400,
                "body": dumps({"error": "No events to publish."}),
            }

        timestamp = datetime.now(timezone.utc).isoformat()
        entries = [{
                'Time': timestamp,
                'Source': event_source,
                'DetailType': 'JSON',
                'Detail': dumps(event),
                'EventBusName': EVENT_BUS_NAME
            } for event in event_json]

        logger.info(f"Entries: {entries}")

        # group events into groups of 10, 10 is the max there can be less
        grouped_entries = [entries[i:i + 10] for i in range(0, len(entries), 10)]
        api_response = []

        for i, entry_group in enumerate(grouped_entries):
            logger.info(f"Batch: {i} Size: {len(entry_group)}")
            # Send events to EventBridge
            event_response = events_client.put_events(
                Entries=entry_group
            )
            logger.info(f"Batch: {i} Event response: {event_response}")

            response_entries = event_response.get('Entries', [])

            # AWS EventBridge guarantees order of responses to match order of events, this is a fail safe catch.
            if len(response_entries) != len(entry_group):
                logger.error("Number of responses does not match number of events. Cannot guarantee order.")
                raise Exception("Number of responses does not match number of events. Cannot guarantee order.")

            for original_event, response_entry in zip(entry_group, response_entries):
                raw_event = original_event.get("Detail", "")
                api_response.append({
                    "event_json": loads(raw_event) if raw_event and isinstance(raw_event, str) else {},
                    "event_bus_id": response_entry.get("EventId", None),
                    "error_code": response_entry.get("ErrorCode", None),
                    "error_message": response_entry.get("ErrorMessage", None)
                })

    except ClientError as e:
        if e.response['Error']['Code'] == 'InternalException':
            error_message = f"Error sending events to EventBridge Bus: {e}."
            logger.exception(error_message)
            return {
                "statusCode": 500,
                "body": dumps({"error": {error_message}}),
            }
        logger.exception(f"Unknown exception occured sending events: {e}.")
        return {
                "statusCode": 500,
                "body": dumps({"error": str(e)}),
            }

    except Exception as e:
        logger.exception(f"Error sending events: {e}.")
        return {
            "statusCode": 500,
            "body": dumps({"error": "An error occurred while processing the request."}),
        }

    return {
        "statusCode": 200,
        "body": dumps(api_response)
    }
