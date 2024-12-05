"""Publish events to SNS."""
import logging
from os import environ
from json import dumps, loads
from typing import Any
from uuid import uuid4

ENVIRONMENT = environ.get("ENVIRONMENT")

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def lambda_handler(event: Any, context: Any) -> Any:
    """Post events."""
    logger.info(f"Event: {event}")

    try:
        body = loads(event["body"])

        try:
            event_json = body["event_json"]
            source = body["source"]

            if not isinstance(event_json, list):
                raise TypeError("event_json must be an array.")
            if not all(isinstance(event, dict) for event in event_json):
                raise TypeError("event_json must contain only objects.")
            if not isinstance(source, str):
                raise TypeError("source must be a string.")
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

        event_response = []
        for event_content in event_json:
            event_id = str(uuid4())
            event_response.append(
                {
                    "event_json": event_content,
                    "event_id": event_id,
                }
            )

        """
        Example SNS event:
        {
            "envelope": {
                "event_id": event_id,
                "timestamp": "2024-11-04T14:23:15.123Z",
                "source": "my-service-name",
                "payload": event_json
                }
        }
        """

    except Exception as e:
        logger.exception(f"Error sending events: {e}.")
        return {
            "statusCode": 500,
            "body": dumps({"error": "An error occurred while processing the request."}),
        }

    return {
        "statusCode": 200,
        "body": dumps(event_response)
    }
