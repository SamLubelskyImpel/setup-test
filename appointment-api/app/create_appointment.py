import logging
from os import environ
from json import dumps
from uuid import uuid4

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def lambda_handler(event, context):
    logger.info(f"Event: {event}")

    request_id = str(uuid4())
    logger.info(f"Request ID: {request_id}")

    return {
        "statusCode": "201",
        "body": dumps({
            "appointment_id": 111111,
            "consumer_id": 111111,
            "vehicle_id": 111111,
            "request_id": request_id,
        })
    }
