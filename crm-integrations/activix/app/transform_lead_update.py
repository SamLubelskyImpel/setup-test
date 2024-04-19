import json
import logging
from os import environ

logger = logging.getLogger()
logger.setLevel(environ.get("LOG_LEVEL", "INFO").upper())


def lambda_handler(event, context):
    """Transform raw activix lead update data to the unified format."""
    logger.info(f"Event: {event}")

    return {
        'statusCode': 200
    }
