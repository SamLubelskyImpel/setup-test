import logging
from os import environ

logger = logging.getLogger()
logger.setLevel(environ.get("LOGLEVEL", "INFO").upper())


def lambda_handler(event, context):
    """Retrieve salesperson data by lead id."""
    logger.info(f"Event: {event}")
