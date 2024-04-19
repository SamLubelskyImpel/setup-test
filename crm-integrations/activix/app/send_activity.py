import logging
from os import environ

logger = logging.getLogger()
logger.setLevel(environ.get("LOG_LEVEL", "INFO").upper())


def lambda_handler(event, context):
    """Create activity on Activix."""
    logger.info(f"Event: {event}")

    return {
        'statusCode': 200
    }