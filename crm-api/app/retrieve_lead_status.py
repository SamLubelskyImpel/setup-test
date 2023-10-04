import logging
from os import environ

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def lambda_handler(event, context):
    """Retrieve lead status."""
    logger.info(f"Event: {event}")
