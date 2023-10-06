import logging
from os import environ

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def lambda_handler(event, context):
    """Retrieve a list of all leads for a given integration partner id."""
    logger.info(f"Event: {event}")
