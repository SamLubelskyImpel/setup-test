import logging
from os import environ

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def lambda_handler(event, context):
    """Update salesperson by salesperson id."""
    logger.info(f"Event: {event}")
